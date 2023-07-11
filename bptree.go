package lotusdb

import (
	"fmt"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
	"path/filepath"
)

var bucketName = []byte("lotusdb-index")

type BPTree struct {
	options indexOptions
	trees   []*bbolt.DB
}

func openIndexBoltDB(options indexOptions) (*BPTree, error) {
	trees := make([]*bbolt.DB, options.partitionNum)

	for i := 0; i < options.partitionNum; i++ {
		tree, err := bbolt.Open(
			filepath.Join(options.dirPath, fmt.Sprintf(indexFileExt, i)),
			0600,
			&bbolt.Options{
				NoSync:          true,
				InitialMmapSize: 1024,
				FreelistType:    bbolt.FreelistMapType,
			},
		)
		if err != nil {
			return nil, err
		}

		tx, err := tree.Begin(true)
		if err != nil {
			return nil, err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		trees[i] = tree
	}

	return &BPTree{trees: trees, options: options}, nil
}

func (bt *BPTree) Get(key []byte) (*wal.ChunkPosition, error) {
	tree := bt.getTreeByKey(key)
	position := new(wal.ChunkPosition)

	if err := tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		value := bucket.Get(key)
		// decode to chunkPosition todo
		println(value)
		return nil
	}); err != nil {
		return nil, err
	}

	return position, nil
}

func (bt *BPTree) PutBatch(records []*IndexRecord) error {
	if len(records) == 0 {
		return nil
	}

	batchLoopNum := len(records) / bt.options.batchSize
	if len(records)%bt.options.batchSize != 0 {
		batchLoopNum++
	}

	for batchIdx := 0; batchIdx < batchLoopNum; batchIdx++ {
		nodeBucket := make([][]*IndexRecord, bt.options.partitionNum)
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

func (bt *BPTree) DeleteBatch(keys [][]byte) error {
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

func (bt *BPTree) getTreeByKey(key []byte) *bbolt.DB {
	if bt.options.partitionNum == defaultPartitionNum {
		return bt.trees[0]
	} else {
		return bt.trees[bt.getKeyPartition(key)]
	}
}

func (bt *BPTree) getKeyPartition(key []byte) uint32 {
	hashFn := bt.options.hashKeyFunction
	return hashFn(string(key)) % uint32(bt.options.partitionNum)
}
