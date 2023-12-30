package lotusdb

import (
	"bytes"
)

type iterType uint8

const (
	CursorItr iterType = iota
	MemItr
)

type SingleIter struct {
	iType  iterType
	options IteratorOptions
	rank    int // rank 越大，说明越新
	iter    IteratorI
}
type IterHeap []*SingleIter

// Len is the number of elements in the collection.
func (ih IterHeap) Len() int {
	return len(ih)
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
func (ih IterHeap) Less(i int, j int) bool {
	ki, kj := ih[i].iter.Key(), ih[j].iter.Key()
	if bytes.Equal(ki, kj) {
		return ih[i].rank > ih[j].rank
	}
	if ih[i].options.Reverse {
		return bytes.Compare(ki, kj) == 1
	} else {
		return bytes.Compare(ki, kj) == -1
	}
}

// Swap swaps the elements with indexes i and j.
func (ih IterHeap) Swap(i int, j int) {
	ih[i], ih[j] = ih[j], ih[i]
}

func (ih *IterHeap) Push(x any) {
	*ih = append(*ih, x.(*SingleIter))
}

func (ih *IterHeap) Pop() any {
	old := *ih
	n := len(old)
	x := old[n-1]
	*ih = old[0 : n-1]
	return x
}
