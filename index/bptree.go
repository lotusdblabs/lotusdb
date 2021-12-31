package index

import (
	"go.etcd.io/bbolt"
	"sync"
	"time"
)

type BboltdbConfig struct {
	IndexComponentType string
	BatchSize          int
	DBName             string
	BucketName         []byte
	FilePath           string

	// todo
	MaxDataSize int64
}

type bboltdb struct {
	db   *bbolt.DB
	conf *BboltdbConfig

	// todo
	metedatadb *bbolt.DB
}

type boltManager struct {
	boltdbMap map[string]*bboltdb
	sync.RWMutex
}

const (
	defaultBatchLoopNum = 1
	defaultBatchSize    = 10000
	IndexComponentTyp   = "boltdb"
)

var manager *boltManager

func init() {
	manager = &boltManager{boltdbMap: make(map[string]*bboltdb)}
}

func (bc *BboltdbConfig) SetType(typ string) {
	bc.IndexComponentType = typ
}

func (bc *BboltdbConfig) SetDbName(dbname string) {
	bc.DBName = dbname
}

func (bc *BboltdbConfig) SetFilePath(filePath string) {
	bc.FilePath = filePath
}

func (bc *BboltdbConfig) GetType() (typ string) {
	return bc.IndexComponentType
}

func (bc *BboltdbConfig) GetDbName() (dbname string) {
	return bc.DBName
}

func (bc *BboltdbConfig) GetFilePath() (filePath string) {
	return bc.FilePath
}

func checkBboltdbConf(conf *BboltdbConfig) error {
	if conf.DBName == "" {
		return ErrDBNameNil
	}

	if conf.FilePath == "" {
		return ErrFilePathNil
	}

	if conf.BucketName == nil || len(conf.BucketName) == 0 {
		return ErrBucketNameNil
	}

	if conf.BatchSize < defaultBatchSize {
		conf.BatchSize = defaultBatchSize
	}
	return nil
}

// NewBboltdb create a boltdb instance.
// A file can only be opened once. if not, file lock competition will occur.
func NewBboltdb(conf *BboltdbConfig) (*bboltdb, error) {
	if err := checkBboltdbConf(conf); err != nil {
		return nil, err
	}

	// check lock check -- clc
	// check
	manager.RLock()
	if db, ok := manager.boltdbMap[conf.GetDbName()]; ok {
		manager.RUnlock()
		return db, nil
	}

	// lock
	manager.RUnlock()
	manager.Lock()
	defer manager.Unlock()

	// check
	if db, ok := manager.boltdbMap[conf.GetDbName()]; ok {
		return db, nil
	}

	// open metadatadb and db
	metaDatadb, err := bbolt.Open(conf.DBName, 0600, &bbolt.Options{
		Timeout:         1 * time.Second,
		NoSync:          true,
		InitialMmapSize: 1024,
	})
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(conf.FilePath, 0600, &bbolt.Options{
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

	if _, err := tx.CreateBucketIfNotExists(conf.BucketName); err != nil {
		return nil, err
	}

	// commit operation
	if err := metaDatadbTx.Commit(); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	b := &bboltdb{
		metedatadb: metaDatadb,
		db:         db,
		conf:       conf,
	}

	manager.boltdbMap[conf.GetDbName()] = b
	return b, nil
}

// Put The put method starts a transaction.
// This method writes kv according to the bucket,
// and creates it if the bucket name does not exist.
func (b *bboltdb) Put(k, v []byte) (err error) {
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
func (b *bboltdb) PutBatch(kv []IndexerKvnode) (offset int, err error) {
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

func (b *bboltdb) Delete(key []byte) error {
	tx, err := b.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Commit()

	return tx.Bucket(b.conf.BucketName).Delete(key)
}

// Get The put method starts a transaction.
// This method reads the value from the bucket with key,
func (b *bboltdb) Get(k []byte) (value []byte, err error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return tx.Bucket(b.conf.BucketName).Get(k), nil
}

func (b *bboltdb) Close() (err error) {
	if err := b.db.Close(); err != nil {
		return err
	}
	if err := b.metedatadb.Close(); err != nil {
		return err
	}
	return nil
}

type boltIter struct {
	b        *bboltdb
	dbBucket *bbolt.Bucket
	tx       *bbolt.Tx
}

func (b *bboltdb) Iter() (IndexerIter, error) {
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
