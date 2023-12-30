package lotusdb

import (
	"bytes"
	"container/heap"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

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

type MergeIterator struct {
	h  IterHeap
	db *DB
}

// Rewind seek the first key in the iterator.
func (mi *MergeIterator) Rewind() {
	for _, v := range mi.h {
		v.iter.Rewind()
	}
	heap.Init(&mi.h)
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

func (mi *MergeIterator) cleanKey(oldKey []byte, rank int) {
	// delete all key == oldKey && rank < t.rank
	for i := 0; i < mi.h.Len(); i++ {
		singleIter := mi.h[i]
		for singleIter.iter.Valid() &&
			bytes.Equal(singleIter.iter.Key(), oldKey) {
			if singleIter.rank > rank {
				panic("rank error")
			}
			singleIter.iter.Next()
		}
		if !singleIter.iter.Valid() {
			heap.Remove(&mi.h, i)
		} else {
			heap.Fix(&mi.h, i)
		}
	}
}

// Next moves the iterator to the next key.
func (mi *MergeIterator) Next() {
	singleIter := heap.Pop(&mi.h).(*SingleIter)
	singleIter.iter.Next()
	if singleIter.iType == MemItr {
		// check is deleteKey
		for singleIter.iter.Valid() {
			if valStruct, ok := singleIter.iter.Value().(y.ValueStruct); ok && valStruct.Meta == LogRecordDeleted {
				mi.cleanKey(singleIter.iter.Key(), singleIter.rank)
				singleIter.iter.Next()
			} else {
				break
			}
		}
		if !singleIter.iter.Valid() {
			heap.Remove(&mi.h, 0)
		} else {
			heap.Fix(&mi.h, 0)
		}
	}
}

// Key get the current key.
func (mi *MergeIterator) Key() []byte {
	return mi.h[0].iter.Key()
}

// Value get the current value.
func (mi *MergeIterator) Value() []byte {
	singleIter := heap.Pop(&mi.h).(*SingleIter)
	val := singleIter.iter.Value().([]byte)
	if singleIter.iType == CursorItr {
		keyPos := new(KeyPosition)
		keyPos.key = singleIter.iter.Key()
		keyPos.partition = uint32(mi.db.vlog.getKeyPartition(singleIter.iter.Key()))
		keyPos.position = wal.DecodeChunkPosition(val)
		record, err := mi.db.vlog.read(keyPos)
		if err != nil {
			panic(err)
		}
		return record.value
	} else if singleIter.iType == MemItr {
		return singleIter.iter.Value().(y.ValueStruct).Value
	} else {
		panic("iType wrong")
	}
}

// Valid returns whether the iterator is exhausted.
func (mi *MergeIterator) Valid() bool {
	return mi.h.Len() > 0
}

// Close the iterator.
func (mi *MergeIterator) Close() error {
	defer func() {
		if r := recover(); r != nil {
			mi.db.mu.Unlock()
		}
	}()
	for _, v := range mi.h {
		err := v.iter.Close()
		if err != nil {
			return err
		}
	}
	mi.db.mu.Unlock()
	return nil
}
