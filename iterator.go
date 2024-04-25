package lotusdb

import (
	"bytes"
	"container/heap"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

// baseIterator.
type baseIterator interface {
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

// Iterator holds a heap and a set of iterators that implement the baseIterator interface.
type Iterator struct {
	h       iterHeap
	itrs    []*singleIter       // used for rebuilding heap
	rankMap map[int]*singleIter // map rank->singleIter
	db      *DB
}

// Rewind seek the first key in the iterator.
func (mi *Iterator) Rewind() {
	for _, v := range mi.itrs {
		v.iter.Rewind()
	}
	h := iterHeap(mi.itrs)
	heap.Init(&h)
	mi.h = h
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (mi *Iterator) Seek(key []byte) {
	seekItrs := make([]*singleIter, 0)
	for _, v := range mi.itrs {
		v.iter.Seek(key)
		if v.iter.Valid() {
			// reset idx
			v.idx = len(seekItrs)
			seekItrs = append(seekItrs, v)
		}
	}
	h := iterHeap(seekItrs)
	heap.Init(&h)
	mi.h = h
}

// cleanKey Remove all unused keys from all iterators.
// If the iterators become empty after clearing, remove them from the heap.
func (mi *Iterator) cleanKey(oldKey []byte, rank int) {
	defer func() {
		if r := recover(); r != nil {
			mi.db.mu.Unlock()
		}
	}()
	for i := 0; i < len(mi.itrs); i++ {
		if i == rank {
			continue
		}
		itr := mi.rankMap[i]
		if !itr.iter.Valid() {
			continue
		}
		for itr.iter.Valid() &&
			bytes.Equal(itr.iter.Key(), oldKey) {
			if itr.rank > rank {
				panic("rank error")
			}
			itr.iter.Next()
		}
		if itr.iter.Valid() {
			heap.Fix(&mi.h, itr.idx)
		} else {
			heap.Remove(&mi.h, itr.idx)
		}
	}
}

// Next moves the iterator to the next key.
func (mi *Iterator) Next() {
	if mi.h.Len() == 0 {
		return
	}

	// Get the top item from the heap
	topIter := mi.h[0]
	mi.cleanKey(topIter.iter.Key(), topIter.rank)

	// Move to the next key and update the heap
	topIter.iter.Next()
	if !topIter.iter.Valid() {
		heap.Remove(&mi.h, topIter.idx)
	} else {
		heap.Fix(&mi.h, topIter.idx)
	}
}

// Key get the current key.
func (mi *Iterator) Key() []byte {
	return mi.h[0].iter.Key()
}

// Value get the current value.
func (mi *Iterator) Value() []byte {
	defer func() {
		if r := recover(); r != nil {
			mi.db.mu.Unlock()
		}
	}()
	topIter := mi.h[0]
	switch topIter.iType {
	case BptreeItr:
		keyPos := new(KeyPosition)
		keyPos.key = topIter.iter.Key()
		keyPos.partition = uint32(mi.db.vlog.getKeyPartition(topIter.iter.Key()))
		keyPos.position = wal.DecodeChunkPosition(topIter.iter.Value().([]byte))
		record, err := mi.db.vlog.read(keyPos)
		if err != nil {
			panic(err)
		}
		return record.value
	case MemItr:
		return topIter.iter.Value().(y.ValueStruct).Value
	default:
		panic("iType not support")
	}
}

// Valid returns whether the iterator is exhausted.
func (mi *Iterator) Valid() bool {
	if mi.h.Len() == 0 {
		return false
	}
	topIter := mi.h[0]
	if topIter.iType == MemItr && topIter.iter.Value().(y.ValueStruct).Meta == LogRecordDeleted {
		mi.cleanKey(topIter.iter.Key(), topIter.rank)
		topIter.iter.Next()
		if topIter.iter.Valid() {
			heap.Fix(&mi.h, topIter.idx)
		} else {
			heap.Remove(&mi.h, topIter.idx)
		}
		return mi.Valid()
	}
	return true
}

// Close the iterator.
func (mi *Iterator) Close() error {
	defer mi.db.mu.Unlock()
	for _, itr := range mi.itrs {
		err := itr.iter.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// NewIterator returns a new iterator.
// The iterator will iterate all the keys in DB.
// It's the caller's responsibility to call Close when iterator is no longer
// used, otherwise resources will be leaked.
// The iterator is not goroutine-safe, you should not use the same iterator
// concurrently from multiple goroutines.
func (db *DB) NewIterator(options IteratorOptions) (*Iterator, error) {
	if db.options.IndexType == Hash {
		return nil, ErrDBIteratorUnsupportedTypeHASH
	}
	db.mu.Lock()
	defer func() {
		if r := recover(); r != nil {
			db.mu.Unlock()
		}
	}()

	itrs := make([]*singleIter, 0, db.options.PartitionNum+len(db.immuMems)+1)
	itrsM := make(map[int]*singleIter)
	rank := 0
	index, ok := db.index.(*BPTree)
	if !ok {
		panic("index type not support")
	}

	for i := 0; i < db.options.PartitionNum; i++ {
		tx, err := index.trees[i].Begin(false)
		if err != nil {
			return nil, err
		}
		itr := newBptreeIterator(
			tx,
			options,
		)
		itr.Rewind()
		// is empty
		if !itr.Valid() {
			_ = itr.Close()
			continue
		}
		itrs = append(itrs, &singleIter{
			iType:   BptreeItr,
			options: options,
			rank:    rank,
			idx:     rank,
			iter:    itr,
		})
		itrsM[rank] = itrs[len(itrs)-1]
		rank++
	}
	memtableList := make([]*memtable, len(db.immuMems)+1)
	copy(memtableList, append(db.immuMems, db.activeMem))
	for i := 0; i < len(memtableList); i++ {
		itr := newMemtableIterator(options, memtableList[i])
		itr.Rewind()
		// is empty
		if !itr.Valid() {
			_ = itr.Close()
			continue
		}
		itrs = append(itrs, &singleIter{
			iType:   MemItr,
			options: options,
			rank:    rank,
			idx:     rank,
			iter:    itr,
		})
		itrsM[rank] = itrs[len(itrs)-1]
		rank++
	}

	h := iterHeap(itrs)
	heap.Init(&h)

	return &Iterator{
		h:       h,
		itrs:    itrs,
		rankMap: itrsM,
		db:      db,
	}, nil
}
