package lotusdb

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"

	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

// bucket name for bolt db to store index data
var indexBucketName = []byte("lotusdb-index")

// BPTree is the BoltDB index implementation.
type BPTree struct {
	options indexOptions
	trees   []*bbolt.DB
}

// openBTreeIndex opens a BoltDB(On-Disk BTree) index.
// Actually, it opens a BoltDB for each partition.
// The partition number is specified by the index options.
func openBTreeIndex(options indexOptions, _ ...diskhash.MatchKeyFunc) (*BPTree, error) {
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
		if _, err := tx.CreateBucketIfNotExists(indexBucketName); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		trees[i] = tree
	}

	return &BPTree{trees: trees, options: options}, nil
}

// Get gets the position of the specified key.
func (bt *BPTree) Get(key []byte, _ ...diskhash.MatchKeyFunc) (*KeyPosition, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	p := bt.options.getKeyPartition(key)
	tree := bt.trees[p]
	var keyPos *KeyPosition

	if err := tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
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

// PutBatch puts the specified key positions into the index.
func (bt *BPTree) PutBatch(positions []*KeyPosition, _ ...diskhash.MatchKeyFunc) error {
	if len(positions) == 0 {
		return nil
	}

	// group positions by partition
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
				bucket := tx.Bucket(indexBucketName)
				// put each record into the bucket
				for _, record := range partitionRecords[partition] {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						encPos := record.position.Encode()
						if err := bucket.Put(record.key, encPos); err != nil {
							if err == bbolt.ErrKeyRequired {
								return ErrKeyIsEmpty
							}
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

// DeleteBatch deletes the specified keys from the index.
func (bt *BPTree) DeleteBatch(keys [][]byte, _ ...diskhash.MatchKeyFunc) error {
	if len(keys) == 0 {
		return nil
	}

	// group keys by partition
	partitionKeys := make([][][]byte, bt.options.partitionNum)
	for _, key := range keys {
		p := bt.options.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
	}

	// delete keys from each partition
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
				bucket := tx.Bucket(indexBucketName)
				// delete each key from the bucket
				for _, key := range partitionKeys[partition] {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						if len(key) == 0 {
							return ErrKeyIsEmpty
						}
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

// Close releases all boltdb database resources.
// It will block waiting for any open transactions to finish
// before closing the database and returning.
func (bt *BPTree) Close() error {
	for _, tree := range bt.trees {
		err := tree.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Sync executes fdatasync() against the database file handle.
func (bt *BPTree) Sync() error {
	for _, tree := range bt.trees {
		err := tree.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

type bptreeIterator struct {
	k       []byte
	v       []byte
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	options IteratorOptions
}

func NewBptreeIterator(tx *bbolt.Tx, options IteratorOptions) (*bptreeIterator, error) {
	b := tx.Bucket(indexBucketName)
	c := b.Cursor()
	return &bptreeIterator{
		cursor:  c,
		options: options,
		tx:      tx,
	}, nil
}

// Rewind seek the first key in the iterator.
func (bi *bptreeIterator) Rewind() {
	if bi.options.Reverse {
		bi.k, bi.v = bi.cursor.Last()
	} else {
		bi.k, bi.v = bi.cursor.First()
	}
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (bi *bptreeIterator) Seek(key []byte) {
	bi.k, bi.v = bi.cursor.Seek(key)
	if !bytes.Equal(bi.k, key) && bi.options.Reverse {
		bi.k, bi.v = bi.cursor.Prev()
	}
}

// Next moves the iterator to the next key.
func (bi *bptreeIterator) Next() {
	if bi.options.Reverse {
		bi.k, bi.v = bi.cursor.Prev()
	} else {
		bi.k, bi.v = bi.cursor.Next()
	}
}

// Key get the current key.
func (bi *bptreeIterator) Key() []byte {
	return bi.k
}

// Value get the current value.
func (ci *bptreeIterator) Value() any {
	return ci.v
}

// Valid returns whether the iterator is exhausted.
func (ci *bptreeIterator) Valid() bool {
	return ci.k != nil
}

// Close the iterator.
func (ci *bptreeIterator) Close() error {
	return ci.tx.Rollback()
}
