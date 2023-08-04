package lotusdb

import (
	"bytes"
	"container/heap"
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
			keyPos = DecodeKeyPosition(key, value, uint32(p))
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
		p := bt.getKeyPartition(key)
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

func (bt *BPTree) getKeyPartition(key []byte) int {
	hashFn := bt.options.hashKeyFunction
	return int(hashFn(key) % uint64(bt.options.partitionNum))
}

func (bpt *BPTree) Iterator(reverse bool) indexIterator {
	return openBptreeIterator(bpt.trees, reverse)
}

type bptreeIterator struct {
	marks        []bool
	txs          []*bbolt.Tx
	cursors      []*bbolt.Cursor
	h            *keyPositionHeap
	partitionNum int
	reverse      bool
}

func openBptreeIterator(trees []*bbolt.DB, reverse bool) *bptreeIterator {
	partitionNum := len(trees)
	mark := make([]bool, partitionNum)
	cursors := make([]*bbolt.Cursor, 0, partitionNum)
	txs := make([]*bbolt.Tx, 0, partitionNum)
	h := NewKeyPositionHeap(reverse, partitionNum)

	for i := 0; i < partitionNum; i++ {
		tx, err := trees[i].Begin(false)
		if err != nil {
			panic("failed to begin a transaction")
		}
		txs = append(txs, tx)
		cursors = append(cursors, tx.Bucket(boltBucketName).Cursor())
	}

	bpi := &bptreeIterator{
		marks:        mark,
		txs:          txs,
		cursors:      cursors,
		h:            h,
		partitionNum: partitionNum,
		reverse:      reverse,
	}
	bpi.Rewind()
	return bpi
}

// Rewind seek the first key in the index iterator.
func (bpi *bptreeIterator) Rewind() {
	h := bpi.h
	h.Clear()
	var k, v []byte

	for i, c := range bpi.cursors {
		bpi.validCursor(i)
		if bpi.reverse {
			k, v = c.Last()
		} else {
			k, v = c.First()
		}
		if k == nil {
			bpi.unValidCursor(uint32(i))
			continue
		}
		heap.Push(h, DecodeKeyPosition(k, v, uint32(i)))
	}
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (bpi *bptreeIterator) Seek(key []byte) {
	h := bpi.h
	h.Clear()

	var k, v []byte
	for i, c := range bpi.cursors {
		bpi.validCursor(i)
		k, v = c.Seek(key)
		if bpi.reverse {
			if k == nil || bytes.Compare(k, key) == 1 {
				k, v = c.Prev()
				if k == nil {
					bpi.unValidCursor(uint32(i))
					continue
				}
			}
		}
		if k == nil {
			bpi.unValidCursor(uint32(i))
			continue
		}
		heap.Push(h, DecodeKeyPosition(k, v, uint32(i)))
	}
}

// Next moves the iterator to the next key.
func (bpi *bptreeIterator) Next() {
	h := bpi.h
	top := heap.Pop(h).(*KeyPosition)
	if !bpi.isCursorValid(int(top.partition)) {
		return
	}
	var k, v []byte
	c := bpi.cursors[top.partition]
	if bpi.reverse {
		k, v = c.Prev()
	} else {
		k, v = c.Next()
	}
	if k == nil {
		bpi.unValidCursor(top.partition)
		return
	}
	heap.Push(h, DecodeKeyPosition(k, v, top.partition))

}

// Key get the current key.
func (bpi *bptreeIterator) Key() []byte {
	k := bpi.h.Top()
	if k == nil {
		return nil
	}
	return k.(*KeyPosition).key
}

// Value get the current value.
func (bpi *bptreeIterator) Value() *KeyPosition {
	k := bpi.h.Top()
	if k == nil {
		return nil
	}
	return k.(*KeyPosition)
}

// Valid returns whether the iterator is exhausted.
func (bpi *bptreeIterator) Valid() bool {
	return bpi.h.Len() > 0
}

// Close the iterator.
func (bpi *bptreeIterator) Close() {
	for _, tx := range bpi.txs {
		_ = tx.Rollback()
	}
}

func (bpi *bptreeIterator) isCursorValid(idx int) bool {
	return bpi.marks[idx]
}

func (bpi *bptreeIterator) unValidCursor(idx uint32) {
	bpi.marks[idx] = false
}

func (bpi *bptreeIterator) validCursor(idx int) {
	bpi.marks[idx] = true
}

type keyPositionHeap struct {
	values  []*KeyPosition
	reverse bool
}

func (h keyPositionHeap) Len() int { return len(h.values) }

func (h keyPositionHeap) Less(i, j int) bool {
	if bytes.Compare(h.values[i].key, h.values[j].key) == -1 {
		return !h.reverse
	} else {
		return h.reverse
	}
}

func (h keyPositionHeap) Swap(i, j int) { h.values[i], h.values[j] = h.values[j], h.values[i] }

func (h *keyPositionHeap) Push(x any) {
	h.values = append(h.values, x.(*KeyPosition))
}

func (h *keyPositionHeap) Pop() any {
	old := h.values
	n := len(old)
	x := old[n-1]
	h.values = old[0 : n-1]
	return x
}

func (h *keyPositionHeap) Top() any {
	if len(h.values) == 0 {
		return nil
	}
	return h.values[0]
}

func (h *keyPositionHeap) Clear() {
	h.values = h.values[:0]
}

func NewKeyPositionHeap(reverse bool, partitionNum int) *keyPositionHeap {
	return &keyPositionHeap{
		values:  make([]*KeyPosition, 0, partitionNum),
		reverse: reverse,
	}
}

func DecodeKeyPosition(key, value []byte, partition uint32) *KeyPosition {
	return &KeyPosition{
		key:       key,
		partition: uint32(partition),
		position:  wal.DecodeChunkPosition(value),
	}
}
