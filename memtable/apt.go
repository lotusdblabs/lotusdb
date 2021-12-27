package memtable

type apt struct {

}

func (a apt) Put(key []byte, value interface{}) *Element {
	panic("implement me")
}

func (a apt) Get(key []byte) *Element {
	panic("implement me")
}

func (a apt) Exist(key []byte) bool {
	panic("implement me")
}

func (a apt) Remove(key []byte) *Element {
	panic("implement me")
}

func (a apt) Foreach(fun handleEle) {
	panic("implement me")
}

func (a apt) FindPrefix(prefix []byte) *Element {
	panic("implement me")
}
