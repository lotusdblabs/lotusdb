package boltdb

import (
	"errors"
	"time"

	"github.com/flowercorp/lotusdb/index/domain"
	"go.etcd.io/bbolt"
)

type BboltdbConfig struct {
	ReadIndexComponentType string
	BatchSize              int
	// todo
	MaxDataSize int64
	DBName      string
	// todo
	FilePath   string
	BucketName []byte
}

type bboltdb struct {
	db *bbolt.DB
	// todo
	metedatadb *bbolt.DB
	conf       *BboltdbConfig
}

type Tx struct {
	tx        *bbolt.Tx
	writeFlag bool
}

const (
	defaultBatchLoopNum = 1
	defaultBatchSize    = 10000

	ReadIndexComponentTyp = "boltdb"
)

func (bc *BboltdbConfig) SetType(typ string) {
	bc.ReadIndexComponentType = typ
}

func (bc *BboltdbConfig) SetDbName(dbname string) {
	bc.DBName = dbname
}

func (bc *BboltdbConfig) SetFilePath(filePath string) {
	bc.FilePath = filePath
}

func (bc *BboltdbConfig) GetType() (typ string) {
	return bc.ReadIndexComponentType
}

func (bc *BboltdbConfig) GetDbName() (dbname string) {
	return bc.DBName
}

func (bc *BboltdbConfig) GetFilePath() (filePath string) {
	return bc.FilePath
}

func checkBboltdbConf(conf *BboltdbConfig) error {
	if conf.DBName == "" {
		return ErrDBNameNil
	}

	if conf.FilePath == "" {
		return ErrFilePathNil
	}

	if conf.BucketName == nil || len(conf.BucketName) == 0 {
		return ErrBucketNameNil
	}

	if conf.BatchSize < defaultBatchSize {
		conf.BatchSize = defaultBatchSize
	}
	return nil
}

// NewBboltdb() create a boltdb instance.
// A file can only be opened once. if not, file lock competition will occur.
func NewBboltdb(conf *BboltdbConfig) (*bboltdb, error) {
	if err := checkBboltdbConf(conf); err != nil {
		return nil, err
	}

	// open metadatadb and db
	metaDatadb, err := bbolt.Open(conf.DBName, 0600, &bbolt.Options{
		Timeout:         1 * time.Second,
		NoSync:          true,
		InitialMmapSize: 1024,
	})
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(conf.FilePath, 0600, &bbolt.Options{
		Timeout:         1 * time.Second,
		NoSync:          true,
		InitialMmapSize: 1024,
	})
	if err != nil {
		return nil, err
	}

	// open metadatadb and db TX
	metaDatadbTx, err := metaDatadb.Begin(true)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}

	// cas create bucket
	if _, err := metaDatadbTx.CreateBucketIfNotExists([]byte("meta")); err != nil {
		return nil, err
	}

	if _, err := tx.CreateBucketIfNotExists(conf.BucketName); err != nil {
		return nil, err
	}

	// commit operation
	if err := metaDatadbTx.Commit(); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &bboltdb{
		metedatadb: metaDatadb,
		db:         db,
		conf:       conf,
	}, err
}

// Begin start a transaction.
// If you need to write a type of transaction,
// please pass `true` for writeFlag, otherwise `false`.
func (b *bboltdb) Begin(writeFlag bool) (domain.ReadIndexTx, error) {
	tx, err := b.db.Begin(writeFlag)
	if err != nil {
		return nil, err
	}

	return &Tx{
		tx:        tx,
		writeFlag: writeFlag,
	}, nil

}

func (b *bboltdb) Rollback(tx domain.ReadIndexTx) error {
	t, ok := tx.(*Tx)
	if !ok {
		return ErrTypeFalse
	}
	return t.tx.Rollback()
}

// The put method starts a transaction.
// This method writes kv according to the bucket,
// and creates it if the bucket name does not exist.
func (b *bboltdb) Put(k, v []byte) (err error) {
	var tx *bbolt.Tx
	tx, err = b.db.Begin(true)
	if err != nil {
		return
	}

	bucket := tx.Bucket(b.conf.BucketName)

	err = bucket.Put(k, v)
	if err != nil {
		return
	}

	return tx.Commit()
}

// PutBatch is used for batch writing scenarios.
// The offset marks the transaction write position of the current batch.
// If this function fails during execution, we can write again from the offset position.
// If offset == len(kv) - 1 , all writes are successful.
func (b *bboltdb) PutBatch(kv []domain.ReadIndexComKvnode) (offset int, err error) {
	var batchLoopNum = defaultBatchLoopNum
	if len(kv) > b.conf.BatchSize {
		batchLoopNum = len(kv) / b.conf.BatchSize
		if len(kv)%b.conf.BatchSize > 0 {
			batchLoopNum++
		}
	}

	batchlimit := b.conf.BatchSize
	for batchIdx := 0; batchIdx < batchLoopNum; batchIdx++ {
		offset = batchIdx * batchlimit
		tx, err := b.db.Begin(true)
		if err != nil {
			return offset, err
		}

		bucket := tx.Bucket(b.conf.BucketName)

	itemLoop:
		for itemIdx := offset; itemIdx < (offset + b.conf.BatchSize - 1); itemIdx++ {
			if itemIdx > len(kv) {
				break itemLoop
			}
			if err := bucket.Put(kv[itemIdx].Key, kv[itemIdx].Value); err != nil {
				tx.Rollback()
				return offset, err
			}
		}

		if err := tx.Commit(); err != nil {
			return offset, err
		}
	}
	return len(kv) - 1, nil
}

func (b *bboltdb) Delete(key []byte) error {
	tx, err := b.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Commit()

	return tx.Bucket(b.conf.BucketName).Delete(key)
}

func (b *bboltdb) DeleteWithTx(tx domain.ReadIndexTx, key []byte) error {
	t, ok := tx.(*Tx)
	if !ok {
		return ErrTypeFalse
	}

	return t.tx.Bucket(b.conf.BucketName).Delete(key)
}

// The put method starts a transaction.
// This method reads the value from the bucket with key,
func (b *bboltdb) Get(k []byte) (value []byte, err error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return tx.Bucket(b.conf.BucketName).Get(k), nil
}

// The put method in a transaction.
// This method reads the value from the bucket with key,
func (b *bboltdb) GetWithTx(tx domain.ReadIndexTx, k []byte) (value []byte, err error) {
	t, ok := tx.(*Tx)
	if !ok {
		return nil, ErrTypeFalse
	}
	return t.tx.Bucket(b.conf.BucketName).Get(k), nil
}

func (b *bboltdb) First() (key, value []byte) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, nil
	}
	defer tx.Rollback()

	return tx.Bucket(b.conf.BucketName).Cursor().First()
}

func (b *bboltdb) FirstWhitTx(tx domain.ReadIndexTx) (key, value []byte, err error) {
	t, ok := tx.(*Tx)
	if !ok {
		return nil, nil, ErrTypeFalse
	}
	key, value = t.tx.Bucket(b.conf.BucketName).Cursor().First()
	return
}

func (b *bboltdb) Last() (key, value []byte) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, nil
	}
	defer tx.Rollback()

	return tx.Bucket(b.conf.BucketName).Cursor().Last()
}

func (b *bboltdb) LastWhitTx(tx domain.ReadIndexTx) (key, value []byte, err error) {
	t, ok := tx.(*Tx)
	if !ok {
		return nil, nil, ErrTypeFalse
	}
	key, value = t.tx.Bucket(b.conf.BucketName).Cursor().Last()
	return
}

func (b *bboltdb) Seek(seek []byte) (key, value []byte) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, nil
	}
	defer tx.Rollback()

	return tx.Bucket(b.conf.BucketName).Cursor().Seek(seek)
}

func (b *bboltdb) SeekWithTx(tx domain.ReadIndexTx, seek []byte) (key, value []byte, err error) {
	t, ok := tx.(*Tx)
	if !ok {
		return nil, nil, ErrTypeFalse
	}
	key, value = t.tx.Bucket(b.conf.BucketName).Cursor().Seek(seek)
	return
}

func (b *bboltdb) Next() (key, value []byte) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, nil
	}
	defer tx.Rollback()

	return tx.Bucket(b.conf.BucketName).Cursor().Next()
}

func (b *bboltdb) NextWithTx(tx domain.ReadIndexTx) (key, value []byte, err error) {
	t, ok := tx.(*Tx)
	if !ok {
		return nil, nil, ErrTypeFalse
	}
	key, value = t.tx.Bucket(b.conf.BucketName).Cursor().Next()
	return
}

func (b *bboltdb) Prev() (key, value []byte) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, nil
	}
	defer tx.Rollback()

	return tx.Bucket(b.conf.BucketName).Cursor().Prev()
}

func (b *bboltdb) PrevWithTx(tx domain.ReadIndexTx) (key, value []byte, err error) {
	t, ok := tx.(*Tx)
	if !ok {
		return nil, nil, ErrTypeFalse
	}
	key, value = t.tx.Bucket(b.conf.BucketName).Cursor().Prev()
	return
}

func (b *bboltdb) IterDelete() error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Commit()

	return tx.Bucket(b.conf.BucketName).Cursor().Delete()
}

func (b *bboltdb) IterDeleteWithTx(tx domain.ReadIndexTx) error {
	t, ok := tx.(*Tx)
	if !ok {
		return ErrTypeFalse
	}
	return t.tx.Bucket(b.conf.BucketName).Cursor().Delete()
}

func (b *bboltdb) Commit(tx domain.ReadIndexTx) error {
	t := tx.(*Tx)
	return t.tx.Commit()
}

// Update executes a function within the context of a read-write managed transaction.
func (b *bboltdb) Update(fn func(tx domain.ReadIndexTx) error) (err error) {
	txIf, err := b.Begin(true)
	if err != nil {
		return
	}

	t := txIf.(*Tx)

	defer func() {
		if r := recover(); r != nil {
			t.tx.Rollback()
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknow panic")
			}
			return
		}
	}()

	if err := fn(t); err != nil {
		t.tx.Rollback()
		return err
	}

	return t.tx.Commit()
}

func (b *bboltdb) View(fn func(tx domain.ReadIndexTx) error) (err error) {
	txIf, err := b.Begin(false)
	if err != nil {
		return
	}
	t := txIf.(*Tx)

	defer func() {
		if r := recover(); r != nil {
			t.tx.Rollback()
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknow panic")
			}
			return
		}
	}()

	if err = fn(t); err != nil {
		t.tx.Rollback()
		return err
	}

	return t.tx.Rollback()
}

func (b *bboltdb) Close() (err error) {
	if err := b.db.Close(); err != nil {
		return err
	}
	if err := b.metedatadb.Close(); err != nil {
		return err
	}
	return nil
}
