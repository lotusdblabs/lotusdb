package lotusdb

import (
	"fmt"
	"path/filepath"

	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

// bucket name for bolt db to store index data
var boltBucketName = []byte("lotusdb-index")

// BPTree is the BoltDB index implementation.
type BPTree struct {
	options indexOptions
	trees   []*bbolt.DB
}

// openIndexBoltDB opens a BoltDB index.
// Actually, it opens a BoltDB for each partition.
// The partition number is specified by the index options.
func openIndexBoltDB(options indexOptions) (*BPTree, error) {
	trees := make([]*bbolt.DB, options.partitionNum)

	for i := 0; i < options.partitionNum; i++ {
		// open bolt db
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

		// begin a writable transaction to create the bucket if not exists
		tx, err := tree.Begin(true)
		if err != nil {
			return nil, err
		}
		if _, err := tx.CreateBucketIfNotExists(boltBucketName); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		trees[i] = tree
	}

	return &BPTree{trees: trees, options: options}, nil
}

func (bt *BPTree) Get(key []byte) (*partPosition, error) {
	tree := bt.getTreeByKey(key)
	var position *partPosition

	if err := tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(boltBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			position.walPosition = wal.DecodeChunkPosition(value)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	position.partIndex = bt.getKeyPartition(key)

	return position, nil
}

func (bt *BPTree) PutBatch(records []*IndexRecord) error {
	if len(records) == 0 {
		return nil
	}

	partitionRecords := make([][]*IndexRecord, bt.options.partitionNum)
	for _, record := range records {
		p := record.position.partIndex
		partitionRecords[p] = append(partitionRecords[p], record)
	}

	for i, prs := range partitionRecords {
		if len(prs) == 0 {
			continue
		}
		g := new(errgroup.Group)
		g.Go(func() error {
			// get the bolt db instance for this partition
			tree := bt.trees[i]
			return tree.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(boltBucketName)
				// put each record into the bucket
				for _, record := range prs {
					encPos := record.position.Encode()
					if err := bucket.Put(record.key, encPos); err != nil {
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
				// get the bolt db instance for this partition
				bucket := tx.Bucket(boltBucketName)
				// delete each key from the bucket
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
