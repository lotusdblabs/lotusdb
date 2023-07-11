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

	partitionRecords := make([][]*IndexRecord, bt.options.partitionNum)
	for _, record := range records {
		p := bt.getKeyPartition(record.key)
		partitionRecords[p] = append(partitionRecords[p], record)
	}

	for i, pr := range partitionRecords {
		if len(pr) == 0 {
			continue
		}
		g := new(errgroup.Group)
		g.Go(func() error {
			tree := bt.trees[i]
			return tree.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(bucketName)
				for _, record := range pr {
					// encode chunk position todo
					if err := bucket.Put(record.key, nil); err != nil {
						return err
					}
				}
				return nil
			})
		})
		if err := g.Wait(); err != nil {
			return err
		}
	}

	return nil
}

func (bt *BPTree) DeleteBatch(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	partitionKeys := make([][][]byte, bt.options.partitionNum)
	for _, key := range keys {
		p := bt.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
	}

	for i, pk := range partitionKeys {
		if len(pk) == 0 {
			continue
		}
		g := new(errgroup.Group)
		g.Go(func() error {
			tree := bt.trees[i]
			return tree.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(bucketName)
				for _, key := range pk {
					if err := bucket.Delete(key); err != nil {
						return err
					}
				}
				return nil
			})
		})
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
