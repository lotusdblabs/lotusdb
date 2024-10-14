package lotusdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

const (
	defaultFileMode        os.FileMode = 0600
	defaultInitialMmapSize int         = 1024
)

// bucket name for bolt db to store index data.
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
			defaultFileMode,
			&bbolt.Options{
				NoSync:          true,
				InitialMmapSize: defaultInitialMmapSize,
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
		if _, err = tx.CreateBucketIfNotExists(indexBucketName); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
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
			err := keyPos.uid.UnmarshalBinary(value[:len(keyPos.uid)])
			if err != nil {
				return err
			}
			keyPos.position = wal.DecodeChunkPosition(value[len(keyPos.uid):])
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return keyPos, nil
}

// PutBatch puts the specified key positions into the index.
//
//nolint:gocognit
func (bt *BPTree) PutBatch(positions []*KeyPosition, _ ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(positions) == 0 {
		return nil, nil
	}

	// group positions by partition
	partitionRecords := make([][]*KeyPosition, bt.options.partitionNum)
	for _, pos := range positions {
		p := pos.partition
		partitionRecords[p] = append(partitionRecords[p], pos)
	}

	// create chan to collect deprecated entry
	deprecatedChan := make(chan []*KeyPosition, len(partitionRecords))
	g, ctx := errgroup.WithContext(context.Background())

	for i := range partitionRecords {
		partition := i
		if len(partitionRecords[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			// get the bolt db instance for this partition
			tree := bt.trees[partition]
			partitionDeprecatedKeyPosition := make([]*KeyPosition, 0)
			err := tree.Update(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(indexBucketName)
				// put each record into the bucket
				for _, record := range partitionRecords[partition] {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						uidBytes, _ := record.uid.MarshalBinary()
						encPos := record.position.Encode()
						//nolint:gocritic // Need to combine uidbytes with encPos and place them in bptree
						valueBytes := append(uidBytes, encPos...)
						if err, oldValue := bucket.Put(record.key, valueBytes); err != nil {
							if errors.Is(err, bbolt.ErrKeyRequired) {
								return ErrKeyIsEmpty
							}
							return err
						} else if oldValue != nil {
							keyPos := new(KeyPosition)
							keyPos.key, keyPos.partition = record.key, record.partition
							err = keyPos.uid.UnmarshalBinary(oldValue[:len(keyPos.uid)])
							if err != nil {
								return err
							}
							keyPos.position = wal.DecodeChunkPosition(oldValue[len(keyPos.uid):])
							partitionDeprecatedKeyPosition = append(partitionDeprecatedKeyPosition, keyPos)
						}
					}
				}
				return nil
			})
			// send deprecateduuid uuid slice to chan
			deprecatedChan <- partitionDeprecatedKeyPosition
			return err
		})
	}
	// Close the channel after all goroutines are done
	go func() {
		_ = g.Wait()
		close(deprecatedChan)
	}()

	var deprecatedKeyPosition []*KeyPosition
	for partitionDeprecatedKeyPosition := range deprecatedChan {
		deprecatedKeyPosition = append(deprecatedKeyPosition, partitionDeprecatedKeyPosition...)
	}

	// Wait for all goroutines to finish
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return deprecatedKeyPosition, nil
}

// DeleteBatch deletes the specified keys from the index.
//
//nolint:gocognit
func (bt *BPTree) DeleteBatch(keys [][]byte, _ ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// group keys by partition
	partitionKeys := make([][][]byte, bt.options.partitionNum)
	for _, key := range keys {
		p := bt.options.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
	}

	// create chan to collect deprecated entry
	deprecatedChan := make(chan []*KeyPosition, len(partitionKeys))

	// delete keys from each partition
	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionKeys {
		partition := i
		if len(partitionKeys[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			tree := bt.trees[partition]
			partitionDeprecatedKeyPosition := make([]*KeyPosition, 0)
			err := tree.Update(func(tx *bbolt.Tx) error {
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
						if err, oldValue := bucket.Delete(key); err != nil {
							return err
						} else if oldValue != nil {
							keyPos := new(KeyPosition)
							keyPos.key, keyPos.partition = key, uint32(partition)
							err = keyPos.uid.UnmarshalBinary(oldValue[:len(keyPos.uid)])
							if err != nil {
								return err
							}
							keyPos.position = wal.DecodeChunkPosition(oldValue[len(keyPos.uid):])
							partitionDeprecatedKeyPosition = append(partitionDeprecatedKeyPosition, keyPos)
						}
					}
				}
				return nil
			})
			// send deprecateduuid uuid slice to chan
			deprecatedChan <- partitionDeprecatedKeyPosition
			return err
		})
	}
	// Close the channel after all goroutines are done
	go func() {
		_ = g.Wait()
		close(deprecatedChan)
	}()

	var deprecatedKeyPosition []*KeyPosition
	for partitionDeprecatedKeyPosition := range deprecatedChan {
		deprecatedKeyPosition = append(deprecatedKeyPosition, partitionDeprecatedKeyPosition...)
	}

	// Wait for all goroutines to finish
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return deprecatedKeyPosition, nil
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

// bptreeIterator implement baseIterator.
type bptreeIterator struct {
	key     []byte
	value   []byte
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	options IteratorOptions
}

// create a boltdb based btree iterator.
func newBptreeIterator(tx *bbolt.Tx, options IteratorOptions) *bptreeIterator {
	return &bptreeIterator{
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		options: options,
		tx:      tx,
	}
}

// Rewind seek the first key in the iterator.
func (bi *bptreeIterator) Rewind() {
	if bi.options.Reverse {
		bi.key, bi.value = bi.cursor.Last()
		if len(bi.options.Prefix) == 0 {
			return
		}
		for bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = bi.cursor.Prev()
		}
	} else {
		bi.key, bi.value = bi.cursor.First()
		if len(bi.options.Prefix) == 0 {
			return
		}
		for bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = bi.cursor.Next()
		}
	}
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (bi *bptreeIterator) Seek(key []byte) {
	bi.key, bi.value = bi.cursor.Seek(key)
	if !bytes.Equal(bi.key, key) && bi.options.Reverse {
		bi.key, bi.value = bi.cursor.Prev()
	}
	if len(bi.options.Prefix) == 0 {
		return
	}
	if !bytes.HasPrefix(bi.Key(), bi.options.Prefix) {
		bi.Next()
	}
}

// Next moves the iterator to the next key.
func (bi *bptreeIterator) Next() {
	if bi.options.Reverse {
		bi.key, bi.value = bi.cursor.Prev()
		if len(bi.options.Prefix) == 0 {
			return
		}
		// prefix scan
		for bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = bi.cursor.Prev()
		}
	} else {
		bi.key, bi.value = bi.cursor.Next()
		if len(bi.options.Prefix) == 0 {
			return
		}
		// prefix scan
		for bi.key != nil && !bytes.HasPrefix(bi.key, bi.options.Prefix) {
			bi.key, bi.value = bi.cursor.Next()
		}
	}
}

// Key get the current key.
func (bi *bptreeIterator) Key() []byte {
	return bi.key
}

// Value get the current value.
func (bi *bptreeIterator) Value() any {
	var uid uuid.UUID
	return bi.value[len(uid):]
}

// Valid returns whether the iterator is exhausted.
func (bi *bptreeIterator) Valid() bool {
	return bi.key != nil
}

// Close the iterator.
func (bi *bptreeIterator) Close() error {
	return bi.tx.Rollback()
}
