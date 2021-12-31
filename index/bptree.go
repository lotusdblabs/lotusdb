package index

import (
	"go.etcd.io/bbolt"
	"sync"
	"time"
)

type BoltOptions struct {
	indexType        IndexerType
	BatchSize        int
	ColumnFamilyName string
	BucketName       []byte
	DirPath          string

	// todo
	MaxDataSize int64
}

type bolt struct {
	db   *bbolt.DB
	conf *BoltOptions

	// todo
	metedatadb *bbolt.DB
}

type boltManager struct {
	boltdbMap map[string]*bolt
	sync.RWMutex
}

const (
	defaultBatchLoopNum = 1
	defaultBatchSize    = 10000
)

var manager *boltManager

func init() {
	manager = &boltManager{boltdbMap: make(map[string]*bolt)}
}

func (bc *BoltOptions) SetType(typ IndexerType) {
	bc.indexType = typ
}

func (bc *BoltOptions) SetColumnFamilyName(cfName string) {
	bc.ColumnFamilyName = cfName
}

func (bc *BoltOptions) SetDirPath(dirPath string) {
	bc.DirPath = dirPath
}

func (bc *BoltOptions) GetType() IndexerType {
	return bc.indexType
}

func (bc *BoltOptions) GetColumnFamilyName() string {
	return bc.ColumnFamilyName
}

func (bc *BoltOptions) GetDirPath() string {
	return bc.DirPath
}

func checkBoltOptions(opt *BoltOptions) error {
	if opt.ColumnFamilyName == "" {
		return ErrColumnFamilyNameNil
	}

	if opt.DirPath == "" {
		return ErrDirPathNil
	}

	if opt.BucketName == nil || len(opt.BucketName) == 0 {
		return ErrBucketNameNil
	}

	if opt.BatchSize < defaultBatchSize {
		opt.BatchSize = defaultBatchSize
	}
	return nil
}

// BptreeBolt create a boltdb instance.
// A file can only be opened once. if not, file lock competition will occur.
func BptreeBolt(opt *BoltOptions) (*bolt, error) {
	if err := checkBoltOptions(opt); err != nil {
		return nil, err
	}

	manager.Lock()
	defer manager.Unlock()
	if db, ok := manager.boltdbMap[opt.GetColumnFamilyName()]; ok {
		manager.RUnlock()
		return db, nil
	}

	// check
	if db, ok := manager.boltdbMap[opt.GetColumnFamilyName()]; ok {
		return db, nil
	}

	// open metadatadb and db
	metaDatadb, err := bbolt.Open(opt.GetColumnFamilyName(), 0600, &bbolt.Options{
		Timeout:         1 * time.Second,
		NoSync:          true,
		InitialMmapSize: 1024,
	})
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(opt.DirPath, 0600, &bbolt.Options{
		Timeout:         1 * time.Second,
		NoSync:          true,
		InitialMmapSize: 1024,
	})
	if err != nil {
		return nil, err
	}

	// open metadatadb and db TX
	metaDatadbTx, err := metaDatadb.Begin(true)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}

	// cas create bucket
	if _, err := metaDatadbTx.CreateBucketIfNotExists([]byte("meta")); err != nil {
		return nil, err
	}

	if _, err := tx.CreateBucketIfNotExists(opt.BucketName); err != nil {
		return nil, err
	}

	// commit operation
	if err := metaDatadbTx.Commit(); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	b := &bolt{
		metedatadb: metaDatadb,
		db:         db,
		conf:       opt,
	}

	manager.boltdbMap[opt.GetColumnFamilyName()] = b
	return b, nil
}

// Put The put method starts a transaction.
// This method writes kv according to the bucket,
// and creates it if the bucket name does not exist.
func (b *bolt) Put(k, v []byte) (err error) {
	var tx *bbolt.Tx
	tx, err = b.db.Begin(true)
	if err != nil {
		return
	}

	bucket := tx.Bucket(b.conf.BucketName)

	err = bucket.Put(k, v)
	if err != nil {
		return
	}

	return tx.Commit()
}

// PutBatch is used for batch writing scenarios.
// The offset marks the transaction write position of the current batch.
// If this function fails during execution, we can write again from the offset position.
// If offset == len(kv) - 1 , all writes are successful.
func (b *bolt) PutBatch(kv []IndexerKvnode) (offset int, err error) {
	var batchLoopNum = defaultBatchLoopNum
	if len(kv) > b.conf.BatchSize {
		batchLoopNum = len(kv) / b.conf.BatchSize
		if len(kv)%b.conf.BatchSize > 0 {
			batchLoopNum++
		}
	}

	batchlimit := b.conf.BatchSize
	for batchIdx := 0; batchIdx < batchLoopNum; batchIdx++ {
		offset = batchIdx * batchlimit
		tx, err := b.db.Begin(true)
		if err != nil {
			return offset, err
		}

		bucket := tx.Bucket(b.conf.BucketName)

	itemLoop:
		for itemIdx := offset; itemIdx < (offset + b.conf.BatchSize - 1); itemIdx++ {
			if itemIdx > len(kv) {
				break itemLoop
			}
			if err := bucket.Put(kv[itemIdx].Key, kv[itemIdx].Value); err != nil {
				tx.Rollback()
				return offset, err
			}
		}

		if err := tx.Commit(); err != nil {
			return offset, err
		}
	}
	return len(kv) - 1, nil
}

func (b *bolt) Delete(key []byte) error {
	tx, err := b.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Commit()

	return tx.Bucket(b.conf.BucketName).Delete(key)
}

// Get The put method starts a transaction.
// This method reads the value from the bucket with key,
func (b *bolt) Get(key []byte) (value []byte, err error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return tx.Bucket(b.conf.BucketName).Get(key), nil
}

func (b *bolt) Close() (err error) {
	if err := b.db.Close(); err != nil {
		return err
	}
	if err := b.metedatadb.Close(); err != nil {
		return err
	}
	return nil
}

type boltIter struct {
	b        *bolt
	dbBucket *bbolt.Bucket
	tx       *bbolt.Tx
}

func (b *bolt) Iter() (IndexerIter, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}

	bucket := tx.Bucket(b.conf.BucketName)
	if bucket == nil {
		return nil, ErrBucketNotInit
	}

	return &boltIter{
		b:        b,
		dbBucket: bucket,
		tx:       tx,
	}, nil
}

func (b *boltIter) First() (key, value []byte) {
	return b.dbBucket.Cursor().First()
}

func (b *boltIter) Last() (key, value []byte) {
	return b.dbBucket.Cursor().Last()
}

func (b *boltIter) Seek(seek []byte) (key, value []byte) {
	return b.dbBucket.Cursor().Seek(seek)
}

func (b *boltIter) Next() (key, value []byte) {
	return b.dbBucket.Cursor().Next()
}

func (b *boltIter) Prev() (key, value []byte) {
	return b.dbBucket.Cursor().Prev()
}

func (b *boltIter) Close() (err error) {
	return b.tx.Rollback()
}
