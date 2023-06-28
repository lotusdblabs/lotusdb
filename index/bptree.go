package index

import "go.etcd.io/bbolt"

type BPTree struct {
	tree *bbolt.DB
}

func NewBPTree() *BPTree {
	return nil
}

func (bt *BPTree) Get(key []byte) error {
	return nil
}

// Put Batch

// Delete Batch

func (bt *BPTree) Close() error {
	return nil
}

func (bt *BPTree) Sync() error {
	return nil
}
