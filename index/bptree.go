package index

import "go.etcd.io/bbolt"

var bucketName = []byte("lotusdb-index")

type BPTree struct {
	tree *bbolt.DB
}

func NewBPTree() *BPTree {
	// open boltdb

	// create bucket if not exists

	return nil
}

func (bt *BPTree) Get(key []byte) error {
	// get from bolt
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
