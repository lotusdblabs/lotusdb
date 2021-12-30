package boltdb

import (
	"github.com/flowercorp/lotusdb/index/domain"
	"go.etcd.io/bbolt"
)

type boltIter struct {
	b        *bboltdb
	dbBucket *bbolt.Bucket
	tx       *bbolt.Tx
}

func (b *bboltdb) Iter() (domain.IndexIter, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}

	bucket := tx.Bucket(b.conf.BucketName)
	if bucket == nil {
		return nil, ErrBucketNotInit
	}

	return &boltIter{
		b:        b,
		dbBucket: bucket,
		tx:       tx,
	}, nil
}

func (b *boltIter) First() (key, value []byte) {
	return b.dbBucket.Cursor().First()
}

func (b *boltIter) Last() (key, value []byte) {
	return b.dbBucket.Cursor().Last()
}

func (b *boltIter) Seek(seek []byte) (key, value []byte) {
	return b.dbBucket.Cursor().Seek(seek)
}

func (b *boltIter) Next() (key, value []byte) {
	return b.dbBucket.Cursor().Next()
}

func (b *boltIter) Prev() (key, value []byte) {
	return b.dbBucket.Cursor().Prev()
}

func (b *boltIter) Close() (err error) {
	return b.tx.Rollback()
}
