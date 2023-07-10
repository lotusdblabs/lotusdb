package index

import (
	"strconv"
	"strings"
	"time"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

var bucketName = []byte("lotusdb-index")

const (
	defaultPartitionNum = 1
	defaultBatchSize    = 100000
	fillPercent         = 0.9
)

// BPTreeOptions options for creating a new bptree.
type BPTreeOptions struct {
	// DirPath path to store index data.
	DirPath string
	// IndexType bptree(bolt).
	IndexType IndexerType
	// ColumnFamilyName db column family name, must be unique.
	ColumnFamilyName string
	// BatchSize flush batch size.
	BatchSize int
	// PartitionNum
	PartitionNum int
	// Hash fn
	Hash func(string) uint32
}

// BPTree is a standard b+tree used to store index data.
type BPTree struct {
	opts  BPTreeOptions
	trees []*bbolt.DB
}

// SetType self-explanatory.
func (bo *BPTreeOptions) SetType(typ IndexerType) {
	bo.IndexType = typ
}

// SetColumnFamilyName self-explanatory.
func (bo *BPTreeOptions) SetColumnFamilyName(cfName string) {
	bo.ColumnFamilyName = cfName
}

// SetDirPath self-explanatory.
func (bo *BPTreeOptions) SetDirPath(dirPath string) {
	bo.DirPath = dirPath
}

// SetPartitionNum self-explanatory.
func (bo *BPTreeOptions) SetPartitionNum(partitionNum int) {
	bo.PartitionNum = partitionNum
}

// SetHash self-explanatory.
func (bo *BPTreeOptions) SetHash(fn func(string) uint32) {
	bo.Hash = fn
}

// GetType self-explanatory.
func (bo *BPTreeOptions) GetType() IndexerType {
	return bo.IndexType
}

// GetColumnFamilyName self-explanatory.
func (bo *BPTreeOptions) GetColumnFamilyName() string {
	return bo.ColumnFamilyName
}

// GetDirPath self-explanatory.
func (bo *BPTreeOptions) GetDirPath() string {
	return bo.DirPath
}

// GetPartitionNum self-explanatory.
func (bo *BPTreeOptions) GetPartitionNum() int {
	return bo.PartitionNum
}

// GetHash self-explanatory.
func (bo *BPTreeOptions) GetHash() func(string) uint32 {
	return bo.Hash
}

func NewBPTree(opt BPTreeOptions) (*BPTree, error) {
	if err := checkBPTreeOptions(&opt); err != nil {
		return nil, err
	}
	trees := make([]*bbolt.DB, opt.PartitionNum)
	// open metadatadb and db
	path := opt.DirPath + separator + strings.ToUpper(opt.GetColumnFamilyName())
	for i := 0; i < int(opt.PartitionNum); i++ {
		db, err := bbolt.Open(path+"_"+strconv.Itoa(i)+indexFileSuffixName, 0600, &bbolt.Options{
			Timeout:         1 * time.Second,
			NoSync:          true,
			InitialMmapSize: 1024,
			FreelistType:    bbolt.FreelistMapType,
		})
		if err != nil {
			return nil, err
		}
		tx, err := db.Begin(true)
		if err != nil {
			return nil, err
		}

		// cas create bucket
		if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
			return nil, err
		}
		// commit operation
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		trees[i] = db
	}
	b := &BPTree{
		trees: trees,
		opts:  opt,
	}
	return b, nil
}

func (bt *BPTree) Get(key []byte) (meta *IndexerMeta, err error) {
	var indexMeta *IndexerMeta
	tree := bt.getTree(key)
	if err := tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			meta = DecodeMeta(value)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return indexMeta, nil
}

func (bt *BPTree) Put(key []byte, value []byte) (err error) {
	tree := bt.getTree(key)
	return bt.putTree(tree, key, value)
}

func (bt *BPTree) putTree(tree *bbolt.DB, key []byte, value []byte) (err error) {
	if err := tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		return bucket.Put(key, value)
	}); err != nil {
		return err
	}
	return nil
}

func (bt *BPTree) PutBatch(nodes []*IndexerNode) (offset int, err error) {
	batchLoopNum := len(nodes) / bt.opts.BatchSize
	if len(nodes)%bt.opts.BatchSize > 0 {
		batchLoopNum++
	}

	batchlimit := bt.opts.BatchSize
	for batchIdx := 0; batchIdx < batchLoopNum; batchIdx++ {
		nodeBucket := make([][]*IndexerNode, bt.opts.PartitionNum)
		for i := range nodeBucket {
			nodeBucket[i] = make([]*IndexerNode, 0)
		}
		offset = batchIdx * batchlimit

		for itemIdx := offset; itemIdx < offset+bt.opts.BatchSize && itemIdx < len(nodes); itemIdx++ {
			node := nodes[itemIdx]
			treeIndex := bt.getTreeIndex(node.Key)
			nodeBucket[treeIndex] = append(nodeBucket[treeIndex], node)
		}

		g := new(errgroup.Group)
		for i := range nodeBucket {
			i := i
			g.Go(func() error {
				tree := bt.trees[i]
				err = tree.Batch(func(tx *bbolt.Tx) error {
					bucket := tx.Bucket(bucketName)
					for _, node := range nodeBucket[i] {
						meta := EncodeMeta(node.Meta)
						if err := bucket.Put(node.Key, meta); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					return err
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return offset, err
		}
	}
	return len(nodes) - 1, nil
}

func (bt *BPTree) Delete(key []byte) error {
	tree := bt.getTree(key)
	return tree.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Delete(key)
	})
}

func (bt *BPTree) DeleteBatch(keys [][]byte) (err error) {
	batchLoopNum := len(keys) / bt.opts.BatchSize
	if len(keys)%bt.opts.BatchSize > 0 {
		batchLoopNum++
	}

	batchlimit := bt.opts.BatchSize
	for batchIdx := 0; batchIdx < batchLoopNum; batchIdx++ {
		keyBucket := make([][][]byte, bt.opts.PartitionNum)
		for i := range keyBucket {
			keyBucket[i] = make([][]byte, 0)
		}
		offset := batchIdx * batchlimit

		for itemIdx := offset; itemIdx < offset+bt.opts.BatchSize && itemIdx < len(keys); itemIdx++ {
			key := keys[itemIdx]
			treeIndex := bt.getTreeIndex(key)
			keyBucket[treeIndex] = append(keyBucket[treeIndex], key)
		}

		g := new(errgroup.Group)
		for i := range keyBucket {
			i := i
			g.Go(func() error {
				tree := bt.trees[i]
				err = tree.Batch(func(tx *bbolt.Tx) error {
					bucket := tx.Bucket(bucketName)
					for _, key := range keyBucket[i] {
						if err := bucket.Delete(key); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					return err
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func (bt *BPTree) Close() error {
	for _, tree := range bt.trees {
		err := tree.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (bt *BPTree) Sync() error {
	for _, tree := range bt.trees {
		err := tree.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func (bt *BPTree) Size() int {
	var size int
	for _, tree := range bt.trees {
		if err := tree.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketName)
			size += bucket.Stats().KeyN
			return nil
		}); err != nil {
			return -1
		}
	}
	return size
}

func (bt *BPTree) getTree(key []byte) *bbolt.DB {
	if key == nil || bt.opts.PartitionNum == defaultPartitionNum {
		return bt.trees[0]
	} else {
		return bt.trees[bt.getTreeIndex(key)]
	}
}

func (bt *BPTree) getTreeIndex(key []byte) uint32 {
	if key == nil {
		return 0
	}
	hashFn := bt.opts.GetHash()
	return hashFn(string(key)) % uint32(bt.opts.PartitionNum)
}

func checkBPTreeOptions(opt *BPTreeOptions) error {
	if opt.ColumnFamilyName == "" {
		return ErrColumnFamilyNameNil
	}

	if opt.DirPath == "" {
		return ErrDirPathNil
	}

	if opt.PartitionNum < defaultPartitionNum {
		opt.PartitionNum = defaultPartitionNum
	}

	if opt.BatchSize < defaultBatchSize {
		opt.BatchSize = defaultBatchSize
	}

	if opt.Hash == nil {
		opt.Hash = util.FnvNew32a
	}

	return nil
}
