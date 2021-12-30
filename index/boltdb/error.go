package boltdb

import "errors"

var (
	ErrDBNameNil     = errors.New("DB name is nil")
	ErrBucketNameNil = errors.New("bucket name is nil")
	ErrFilePathNil   = errors.New("file path is nil")
	ErrTXNil         = errors.New("transaction is nil")

	ErrTypeFalse = errors.New("transaction type error")

	ErrDbNotInit     = errors.New("db not init")
	ErrBucketNotInit = errors.New("bucket not init")
)
