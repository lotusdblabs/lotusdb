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

func (bt *BPTree) Get(key []byte) (*KeyPosition, error) {
	p := bt.getKeyPartition(key)
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

	groups := make([]errgroup.Group, len(partitionRecords))
	for i := 0; i < len(partitionRecords); i++ {
		if len(partitionRecords[i]) == 0 {
			continue
		}

		part := i
		groups[i].Go(func() error {
			// get the bolt db instance for this partition
			tree := bt.trees[part]
			return tree.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(boltBucketName)
				// put each record into the bucket
				for _, record := range partitionRecords[part] {
					encPos := record.position.Encode()
					if err := bucket.Put(record.key, encPos); err != nil {
						return err
					}
				}
				return nil
			})
		})
	}

	for i := 0; i < len(partitionRecords); i++ {
		if err := groups[i].Wait(); err != nil {
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

func (bt *BPTree) getKeyPartition(key []byte) int {
	hashFn := bt.options.hashKeyFunction
	return int(hashFn(key) % uint64(bt.options.partitionNum))
}
