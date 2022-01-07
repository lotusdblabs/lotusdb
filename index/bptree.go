package index

import (
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

const (
	defaultBatchLoopNum = 1
	defaultBatchSize    = 10000
)

var manager *BPTreeManager

func init() {
	manager = &BPTreeManager{treeMap: make(map[string]*BPTree)}
}

type BPTreeOptions struct {
	IndexType        IndexerType
	ColumnFamilyName string
	BucketName       []byte
	DirPath          string

	BatchSize   int
	MaxDataSize int64
}

type BPTree struct {
	db   *bbolt.DB
	opts *BPTreeOptions

	// todo
	metedatadb *bbolt.DB
}

type BPTreeManager struct {
	treeMap map[string]*BPTree
	sync.RWMutex
}

func (bo *BPTreeOptions) SetType(typ IndexerType) {
	bo.IndexType = typ
}

func (bo *BPTreeOptions) SetColumnFamilyName(cfName string) {
	bo.ColumnFamilyName = cfName
}

func (bo *BPTreeOptions) SetDirPath(dirPath string) {
	bo.DirPath = dirPath
}

func (bo *BPTreeOptions) GetType() IndexerType {
	return bo.IndexType
}

func (bo *BPTreeOptions) GetColumnFamilyName() string {
	return bo.ColumnFamilyName
}

func (bo *BPTreeOptions) GetDirPath() string {
	return bo.DirPath
}

// BptreeBolt create a boltdb instance.
// A file can only be opened once. if not, file lock competition will occur.
func BptreeBolt(opt *BPTreeOptions) (*BPTree, error) {
	if err := checkBPTreeOptions(opt); err != nil {
		return nil, err
	}

	// check
	manager.RLock()
	if db, ok := manager.treeMap[opt.GetColumnFamilyName()]; ok {
		manager.RUnlock()
		return db, nil
	}

	// lock
	manager.RUnlock()
	manager.Lock()
	defer manager.Unlock()

	// check
	if db, ok := manager.treeMap[opt.GetColumnFamilyName()]; ok {
		return db, nil
	}

	// open metadatadb and db
	path := opt.DirPath + separator + opt.GetColumnFamilyName()
	metaDatadb, err := bbolt.Open(path+metaFileSuffixName, 0600, &bbolt.Options{
		Timeout:         1 * time.Second,
		NoSync:          true,
		InitialMmapSize: 1024,
	})
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(path+indexFileSuffixName, 0600, &bbolt.Options{
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

	b := &BPTree{
		metedatadb: metaDatadb,
		db:         db,
		opts:       opt,
	}
	manager.treeMap[opt.GetColumnFamilyName()] = b
	return b, nil
}

// Put The put method starts a transaction.
// This method writes kv according to the bucket,
// and creates it if the bucket name does not exist.
func (b *BPTree) Put(k, v []byte) (err error) {
	var tx *bbolt.Tx
	tx, err = b.db.Begin(true)
	if err != nil {
		return
	}

	bucket := tx.Bucket(b.opts.BucketName)

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
func (b *BPTree) PutBatch(nodes []*IndexerNode) (offset int, err error) {
	var batchLoopNum = defaultBatchLoopNum
	if len(nodes) > b.opts.BatchSize {
		batchLoopNum = len(nodes) / b.opts.BatchSize
		if len(nodes)%b.opts.BatchSize > 0 {
			batchLoopNum++
		}
	}

	batchlimit := b.opts.BatchSize
	for batchIdx := 0; batchIdx < batchLoopNum; batchIdx++ {
		offset = batchIdx * batchlimit
		tx, err := b.db.Begin(true)
		if err != nil {
			return offset, err
		}

		bucket := tx.Bucket(b.opts.BucketName)

	itemLoop:
		for itemIdx := offset; itemIdx < (offset + b.opts.BatchSize - 1); itemIdx++ {
			if itemIdx >= len(nodes) {
				break itemLoop
			}
			meta := EncodeMeta(nodes[itemIdx].Meta)
			if err := bucket.Put(nodes[itemIdx].Key, meta); err != nil {
				tx.Rollback()
				return offset, err
			}
		}
		if err := tx.Commit(); err != nil {
			return offset, err
		}
	}
	return len(nodes) - 1, nil
}

func (b *BPTree) Delete(key []byte) error {
	tx, err := b.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Commit()

	return tx.Bucket(b.opts.BucketName).Delete(key)
}

// Get The put method starts a transaction.
// This method reads the value from the bucket with key,
func (b *BPTree) Get(key []byte) (*IndexerMeta, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	buf := tx.Bucket(b.opts.BucketName).Get(key)
	return DecodeMeta(buf), nil
}

func (b *BPTree) Close() (err error) {
	if err := b.db.Close(); err != nil {
		return err
	}
	if err := b.metedatadb.Close(); err != nil {
		return err
	}
	return nil
}

func checkBPTreeOptions(opt *BPTreeOptions) error {
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

type BPTreeIter struct {
	bpTree *BPTree
	bucket *bbolt.Bucket
	tx     *bbolt.Tx
}

func (b *BPTree) Iter() (IndexerIter, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}

	bucket := tx.Bucket(b.opts.BucketName)
	if bucket == nil {
		return nil, ErrBucketNotInit
	}

	return &BPTreeIter{
		bpTree: b,
		bucket: bucket,
		tx:     tx,
	}, nil
}

func (b *BPTreeIter) First() (key, value []byte) {
	return b.bucket.Cursor().First()
}

func (b *BPTreeIter) Last() (key, value []byte) {
	return b.bucket.Cursor().Last()
}

func (b *BPTreeIter) Seek(seek []byte) (key, value []byte) {
	return b.bucket.Cursor().Seek(seek)
}

func (b *BPTreeIter) Next() (key, value []byte) {
	return b.bucket.Cursor().Next()
}

func (b *BPTreeIter) Prev() (key, value []byte) {
	return b.bucket.Cursor().Prev()
}

func (b *BPTreeIter) Close() (err error) {
	return b.tx.Rollback()
}
