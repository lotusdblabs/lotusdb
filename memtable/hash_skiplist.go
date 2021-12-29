package memtable

import (
	"github.com/spaolacci/murmur3"
)

const (
	MaxSize = 10
)

// HashSkipList
type HashSkipList struct {
	skls map[int64]*SkipList
}

func NewHashSkipList() *HashSkipList {
	return &HashSkipList{
		make(map[int64]*SkipList),
	}
}

func getHash(key []byte) int64 {
	hash32 := murmur3.New32()
	if _, err := hash32.Write(key); err != nil {
		return 0
	}

	return int64(hash32.Sum32() % MaxSize)
}

func (h *HashSkipList) getSkipList(index int64) *SkipList {
	return h.skls[index]
}

func (h *HashSkipList) Put(key []byte, value interface{}) *Element {
	skl := h.getSkipList(getHash(key))
	if skl == nil {
		skl = NewSkipList()
	}

	return skl.Put(key, value)
}

func (h *HashSkipList) Get(key []byte) *Element {
	skl := h.getSkipList(getHash(key))
	if skl == nil {
		skl = NewSkipList()
		return nil
	}

	return skl.Get(key)
}

func (h *HashSkipList) Exist(key []byte) bool {
	skl := h.getSkipList(getHash(key))
	if skl == nil {
		skl = NewSkipList()
		return false
	}

	return skl.Exist(key)
}

func (h *HashSkipList) Remove(key []byte) *Element {
	skl := h.getSkipList(getHash(key))
	if skl == nil {
		skl = NewSkipList()
		return nil
	}

	return skl.Get(key)
}
