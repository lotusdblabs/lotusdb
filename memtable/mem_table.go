package memtable

type Memtable interface {
	Put(key []byte, value interface{}) *Element
	Get(key []byte) *Element
	Exist(key []byte) bool
	Remove(key []byte) *Element
	Foreach(fun handleEle)
	FindPrefix(prefix []byte) *Element
}

type (
	// Node the skip list node.
	Node struct {
		next []*Element
	}

	// Element element is the data stored.
	Element struct {
		Node
		key   []byte
		value interface{}
	}
)

// Key the key of the Element.
func (e *Element) Key() []byte {
	return e.key
}

// Value the value of the Element.
func (e *Element) Value() interface{} {
	return e.value
}

// SetValue set the element value.
func (e *Element) SetValue(val interface{}) {
	e.value = val
}

// Next the first-level index of the skip list is the original data, which is arranged in an orderly manner.
// A linked list of all data in series can be obtained according to the Next method.
func (e *Element) Next() *Element {
	return e.next[0]
}
