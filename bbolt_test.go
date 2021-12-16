package lotusdb

import (
	bolt "go.etcd.io/bbolt"
	"testing"
)

func TestBBolt(t *testing.T) {
	opt := &bolt.Options{}
	opt.NoSync = true
	db, _ := bolt.Open("/tmp/bbolt", 0644, opt)
	db.Update(func(tx *bolt.Tx) error {
		bucket, _ := tx.CreateBucketIfNotExists([]byte("test-bucket"))
		err := bucket.Put([]byte("test-key"), []byte("test-val"))
		return err
	})
}
