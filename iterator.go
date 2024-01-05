package lotusdb

import (
	"bytes"
	"container/heap"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

// IteratorI
type IteratorI interface {
	// Rewind seek the first key in the iterator.
	Rewind()
	// Seek move the iterator to the key which is
	// greater(less when reverse is true) than or equal to the specified key.
	Seek(key []byte)
	// Next moves the iterator to the next key.
	Next()
	// Key get the current key.
	Key() []byte
	// Value get the current value.
	Value() any
	// Valid returns whether the iterator is exhausted.
	Valid() bool
	// Close the iterator.
	Close() error
}

// MergeIterator holds a heap and a set of iterators that implement the IteratorI interface
type MergeIterator struct {
	h    IterHeap
	itrs []*SingleIter // used for rebuilding heap
	db   *DB
}

// Rewind seek the first key in the iterator.
func (mi *MergeIterator) Rewind() {
	for _, v := range mi.itrs {
		v.iter.Rewind()
	}
	h := IterHeap(mi.itrs)
	heap.Init(&h)
	mi.h = h
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (mi *MergeIterator) Seek(key []byte) {
	for i, v := range mi.h {
		v.iter.Seek(key)
		if !v.iter.Valid() {
			heap.Remove(&mi.h, i)
		}
	}
}

// cleanKey Remove all unused keys from all iterators.
// If the iterators become empty after clearing, remove them from the heap.
func (mi *MergeIterator) cleanKey(oldKey []byte, rank int) {
	defer func() {
		if r := recover(); r != nil {
			mi.db.mu.Unlock()
		}
	}()
	// delete all key == oldKey && rank < t.rank
	copyedItrs := make([]*SingleIter, len(mi.itrs))
	// becouse heap.Remove heap.Fix may alter the order of elements in the slice.
	copy(copyedItrs, mi.itrs)
	for i := 0; i < len(copyedItrs); i++ {
		singleIter := copyedItrs[i]
		if singleIter.rank == rank || !singleIter.iter.Valid() {
			continue
		}
		// 这里说明之前还是valid的
		for singleIter.iter.Valid() &&
			bytes.Equal(singleIter.iter.Key(), oldKey) {
			if singleIter.rank > rank {
				panic("rank error")
			}
			singleIter.iter.Next()
		}
		if !singleIter.iter.Valid() {
			heap.Remove(&mi.h, singleIter.idx)
		} else {
			heap.Fix(&mi.h, singleIter.idx)
		}
	}
}

// Next moves the iterator to the next key.
func (mi *MergeIterator) Next() {
	// top item
	singleIter := mi.h[0]
	oldKey := singleIter.iter.Key()
	mi.cleanKey(oldKey, singleIter.rank)
	if !singleIter.iter.Valid() {
		return
	}
	singleIter.iter.Next()

	if singleIter.iType == MemItr {
		// check is deleteKey
		for singleIter.iter.Valid() {
			if valStruct, ok := singleIter.iter.Value().(y.ValueStruct); ok && valStruct.Meta == LogRecordDeleted {
				mi.cleanKey(singleIter.iter.Key(), singleIter.rank)
				if !singleIter.iter.Valid() {
					return
				}
				singleIter.iter.Next()
			} else {
				break
			}
		}
	}
	if !singleIter.iter.Valid() {
		heap.Remove(&mi.h, 0)
	} else {
		heap.Fix(&mi.h, 0)
	}
}

// Key get the current key.
func (mi *MergeIterator) Key() []byte {
	return mi.h[0].iter.Key()
}

// Value get the current value.
func (mi *MergeIterator) Value() []byte {
	defer func() {
		if r := recover(); r != nil {
			mi.db.mu.Unlock()
		}
	}()
	singleIter := mi.h[0]
	if singleIter.iType == BptreeItr {
		keyPos := new(KeyPosition)
		keyPos.key = singleIter.iter.Key()
		keyPos.partition = uint32(mi.db.vlog.getKeyPartition(singleIter.iter.Key()))
		keyPos.position = wal.DecodeChunkPosition(singleIter.iter.Value().([]byte))
		record, err := mi.db.vlog.read(keyPos)
		if err != nil {
			panic(err)
		}
		return record.value
	} else if singleIter.iType == MemItr {
		return singleIter.iter.Value().(y.ValueStruct).Value
	} else {
		panic("iType not support")
	}
}

// Valid returns whether the iterator is exhausted.
func (mi *MergeIterator) Valid() bool {
	if mi.h.Len() == 0 {
		return false
	}
	singleIter := mi.h[0]
	if singleIter.iType == MemItr && singleIter.iter.Value().(y.ValueStruct).Meta == LogRecordDeleted {
		mi.cleanKey(singleIter.iter.Key(), singleIter.rank)
		if !singleIter.iter.Valid() {
			return false
		}
		singleIter.iter.Next()
		if singleIter.iter.Valid() {
			heap.Fix(&mi.h, 0)
			return mi.Valid()
		} else {
			heap.Remove(&mi.h, 0)
		}
	} else if singleIter.iType == BptreeItr && !singleIter.iter.Valid() {
		heap.Remove(&mi.h, 0)
	}
	return mi.h.Len() > 0
}

// Close the iterator.
func (mi *MergeIterator) Close() error {
	for _, v := range mi.itrs {
		err := v.iter.Close()
		if err != nil {
			mi.db.mu.Unlock()
			return err
		}
	}
	mi.db.mu.Unlock()
	return nil
}
