package lotusdb

import (
	"context"
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

// openBTreeIndex opens a BoltDB(On-Disk BTree) index.
// Actually, it opens a BoltDB for each partition.
// The partition number is specified by the index options.
func openBTreeIndex(options indexOptions) (*BPTree, error) {
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

func (bt *BPTree) Get(key []byte) (*KeyPosition, error) {
	p := bt.options.getKeyPartition(key)
	tree := bt.trees[p]
	var keyPos *KeyPosition

	if err := tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(boltBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			keyPos = new(KeyPosition)
			keyPos.key, keyPos.partition = key, uint32(p)
			keyPos.position = wal.DecodeChunkPosition(value)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return keyPos, nil
}

func (bt *BPTree) PutBatch(positions []*KeyPosition) error {
	if len(positions) == 0 {
		return nil
	}

	partitionRecords := make([][]*KeyPosition, bt.options.partitionNum)
	for _, pos := range positions {
		p := pos.partition
		partitionRecords[p] = append(partitionRecords[p], pos)
	}

	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionRecords {
		partition := i
		if len(partitionRecords[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			// get the bolt db instance for this partition
			tree := bt.trees[partition]
			return tree.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(boltBucketName)
				// put each record into the bucket
				for _, record := range partitionRecords[partition] {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						encPos := record.position.Encode()
						if err := bucket.Put(record.key, encPos); err != nil {
							return err
						}
					}
				}
				return nil
			})
		})
	}
	return g.Wait()
}

func (bt *BPTree) DeleteBatch(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	partitionKeys := make([][][]byte, bt.options.partitionNum)
	for _, key := range keys {
		p := bt.options.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
	}
	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionKeys {
		partition := i
		if len(partitionKeys[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			tree := bt.trees[partition]
			return tree.Update(func(tx *bbolt.Tx) error {
				// get the bolt db instance for this partition
				bucket := tx.Bucket(boltBucketName)
				// delete each key from the bucket
				for _, key := range partitionKeys[partition] {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						if err := bucket.Delete(key); err != nil {
							return err
						}
					}
				}
				return nil
			})
		})
	}
	return g.Wait()
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
