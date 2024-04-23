package lotusdb

import (
	"bytes"
)

type iterType uint8

const (
	BptreeItr iterType = iota
	MemItr
)

// singleIter element used to construct the heap，implementing the container.heap interface.
type singleIter struct {
	iType   iterType
	options IteratorOptions
	rank    int //  A higher rank indicates newer data.
	idx     int //  idx in heap
	iter    baseIterator
}

type iterHeap []*singleIter

// Len is the number of elements in the collection.
func (ih *iterHeap) Len() int {
	return len(*ih)
}

// Less reports whether the element with index i
// must sort before the element with index j.
//
// If both Less(i, j) and Less(j, i) are false,
// then the elements at index i and j are considered equal.
// Sort may place equal elements in any order in the final result,
// while Stable preserves the original input order of equal elements.
//
// Less must describe a transitive ordering:
//   - if both Less(i, j) and Less(j, k) are true, then Less(i, k) must be true as well.
//   - if both Less(i, j) and Less(j, k) are false, then Less(i, k) must be false as well.
//
// Note that floating-point comparison (the < operator on float32 or float64 values)
// is not a transitive ordering when not-a-number (NaN) values are involved.
// See Float64Slice.Less for a correct implementation for floating-point values.
func (ih *iterHeap) Less(i int, j int) bool {
	ki, kj := (*ih)[i].iter.Key(), (*ih)[j].iter.Key()
	if bytes.Equal(ki, kj) {
		return (*ih)[i].rank > (*ih)[j].rank
	}
	if (*ih)[i].options.Reverse {
		return bytes.Compare(ki, kj) == 1
	}
	return bytes.Compare(ki, kj) == -1
}

// Swap swaps the elements with indexes i and j.
func (ih *iterHeap) Swap(i int, j int) {
	(*ih)[i], (*ih)[j] = (*ih)[j], (*ih)[i]
	(*ih)[i].idx, (*ih)[j].idx = i, j
}

// Push add x as element Len().
func (ih *iterHeap) Push(x any) {
	*ih = append(*ih, x.(*singleIter))
}

// Pop remove and return element Len() - 1.
func (ih *iterHeap) Pop() any {
	old := *ih
	n := len(old)
	x := old[n-1]
	*ih = old[0 : n-1]
	return x
}
