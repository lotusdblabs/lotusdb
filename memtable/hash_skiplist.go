package memtable

// HashSkipList
type HashSkipList struct {
	skls map[int64]*SkipList
}

func (h HashSkipList) Put(key []byte, value interface{}) *Element {
	panic("implement me")
}

func (h HashSkipList) Get(key []byte) *Element {
	panic("implement me")
}

func (h HashSkipList) Exist(key []byte) bool {
	panic("implement me")
}

func (h HashSkipList) Remove(key []byte) *Element {
	panic("implement me")
}

func (h HashSkipList) Foreach(fun handleEle) {
	panic("implement me")
}

func (h HashSkipList) FindPrefix(prefix []byte) *Element {
	panic("implement me")
}

