package memtable

// HashSkipList
type HashSkipList struct {
	skls map[int64]*SkipList
}
