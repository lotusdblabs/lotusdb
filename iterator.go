package lotusdb

type Iterator struct {
}

// Rewind seek the first key in the iterator.
func (it *Iterator) Rewind() {

}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (it *Iterator) Seek(key []byte) {

}

// Next moves the iterator to the next key.
func (it *Iterator) Next() {

}

// Key get the current key.
func (it *Iterator) Key() []byte {
	return nil
}

// Value get the current value.
func (it *Iterator) Value() ([]byte, error) {
	return nil, nil
}

// Valid returns whether the iterator is exhausted.
func (it *Iterator) Valid() bool {
	return false
}

// Close the iterator.
func (it *Iterator) Close() {

}
