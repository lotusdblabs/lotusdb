package memtable

import (
	"github.com/flowercorp/lotusdb/logfile"
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
	if skl, ok := h.skls[index]; ok {
		return skl
	}

	h.skls[index] = NewSkipList()
	return h.skls[index]
}

func (h *HashSkipList) Put(key []byte, value []byte) *logfile.LogEntry {
	skl := h.getSkipList(getHash(key))

	return skl.Put(key, value)
}

func (h *HashSkipList) Get(key []byte) *logfile.LogEntry {
	skl := h.getSkipList(getHash(key))

	return skl.Get(key)
}

func (h *HashSkipList) Exist(key []byte) bool {
	skl := h.getSkipList(getHash(key))

	return skl.Exist(key)
}

func (h *HashSkipList) Remove(key []byte) *logfile.LogEntry {
	skl := h.getSkipList(getHash(key))

	return skl.Remove(key)
}

func (h *HashSkipList) MemSize() int64 {
	var size int64

	for _, skl := range h.skls {
		size += skl.MemSize()
	}

	return size
}
