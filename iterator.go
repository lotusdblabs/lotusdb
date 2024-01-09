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
	seekItrs := make([]*SingleIter, 0)
	for _, v := range mi.itrs {
		v.iter.Seek(key)
		if v.iter.Valid() {
			// reset idx
			v.idx = len(seekItrs)
			seekItrs = append(seekItrs, v)
		}
	}
	h := IterHeap(seekItrs)
	heap.Init(&h)
	mi.h = h
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
		itr := copyedItrs[i]
		if itr.rank == rank || !itr.iter.Valid() {
			continue
		}
		for itr.iter.Valid() &&
			bytes.Equal(itr.iter.Key(), oldKey) {
			if itr.rank > rank {
				panic("rank error")
			}
			itr.iter.Next()
		}
		if !itr.iter.Valid() {
			heap.Remove(&mi.h, itr.idx)
		} else {
			heap.Fix(&mi.h, itr.idx)
		}
	}
}

// Next moves the iterator to the next key.
func (mi *MergeIterator) Next() {
	if mi.h.Len() == 0 {
		return
	}

	// Get the top item from the heap
	topIter := mi.h[0]
	mi.cleanKey(topIter.iter.Key(), topIter.rank)

	// Check if the top item is a MemItr and a deleteKey
	if topIter.iType == MemItr {
		if valStruct, ok := topIter.iter.Value().(y.ValueStruct); ok && valStruct.Meta == LogRecordDeleted {
			// Move to the next key and update the heap
			topIter.iter.Next()
			if !topIter.iter.Valid() {
				heap.Remove(&mi.h, topIter.idx)
			} else {
				heap.Fix(&mi.h, topIter.idx)
			}
			// Call Next recursively to handle consecutive deleteKeys
			mi.Next()
		}
	}

	// Move to the next key and update the heap
	topIter.iter.Next()
	if !topIter.iter.Valid() {
		heap.Remove(&mi.h, topIter.idx)
	} else {
		heap.Fix(&mi.h, topIter.idx)
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
	topIter := mi.h[0]
	if topIter.iType == BptreeItr {
		keyPos := new(KeyPosition)
		keyPos.key = topIter.iter.Key()
		keyPos.partition = uint32(mi.db.vlog.getKeyPartition(topIter.iter.Key()))
		keyPos.position = wal.DecodeChunkPosition(topIter.iter.Value().([]byte))
		record, err := mi.db.vlog.read(keyPos)
		if err != nil {
			panic(err)
		}
		return record.value
	} else if topIter.iType == MemItr {
		return topIter.iter.Value().(y.ValueStruct).Value
	} else {
		panic("iType not support")
	}
}

// Valid returns whether the iterator is exhausted.
func (mi *MergeIterator) Valid() bool {
	if mi.h.Len() == 0 {
		return false
	}
	topIter := mi.h[0]
	if topIter.iType == MemItr && topIter.iter.Value().(y.ValueStruct).Meta == LogRecordDeleted {
		mi.cleanKey(topIter.iter.Key(), topIter.rank)
		topIter.iter.Next()
		if topIter.iter.Valid() {
			heap.Fix(&mi.h, topIter.idx)
			return mi.Valid()
		} else {
			heap.Remove(&mi.h, topIter.idx)
		}
	} else if topIter.iType == BptreeItr && !topIter.iter.Valid() {
		heap.Remove(&mi.h, topIter.idx)
	}
	return mi.h.Len() > 0
}

// Close the iterator.
func (mi *MergeIterator) Close() error {
	for _, itr := range mi.itrs {
		err := itr.iter.Close()
		if err != nil {
			mi.db.mu.Unlock()
			return err
		}
	}
	mi.db.mu.Unlock()
	return nil
}
