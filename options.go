package lotusdb

import (
	"os"

	"github.com/cespare/xxhash/v2"
)

type Options struct {
	// DirPath specifies the directory path where all the database files will be stored.
	DirPath string

	// MemtableSize represents the maximum size in bytes for a memtable.
	// It means that each memtable will occupy so much memory.
	// Default value is 64MB.
	MemtableSize uint32

	// MemtableNums represents maximum number of memtables to keep in memory before flushing.
	// Default value is 15.
	MemtableNums int

	// BlockCache specifies the size of the block cache in number of bytes.
	// A block cache is used to store recently accessed data blocks, improving read performance.
	// If BlockCache is set to 0, no block cache will be used.
	BlockCache uint32

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	Sync bool

	// BytesPerSync specifies the number of bytes to write before calling fsync.
	BytesPerSync uint32

	// PartitionNum specifies the number of partitions to use for the index and value log.
	PartitionNum int

	// KeyHashFunction specifies the hash function for sharding.
	// It is used to determine which partition a key belongs to.
	// Default value is xxhash.
	KeyHashFunction func([]byte) uint64

	// ValueLogFileSize size of a single value log file.
	// Default value is 1GB.
	ValueLogFileSize int64

	// indexType.
	// default value is bptree.
	IndexType IndexType

	// writing entries to disk after reading the specified number of entries.
	CompactBatchCount int
}

// BatchOptions specifies the options for creating a batch.
type BatchOptions struct {
	// Sync has the same semantics as Options.Sync.
	Sync bool

	// ReadOnly specifies whether the batch is read only.
	ReadOnly bool
}

// WriteOptions set optional params for PutWithOptions and DeleteWithOptions.
// If you use Put and Delete (without options), that means to use the default values.
type WriteOptions struct {
	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.

	// Default value is false.
	Sync bool

	// DisableWal if true, writes will not first go to the write ahead log, and the write may get lost after a crash.
	// Setting true only if don`t care about the data loss.
	// Default value is false.
	DisableWal bool
}

var (
	// just used for internal
	defaultWriteOptions = WriteOptions{Sync: false, DisableWal: false}

	// WriteReliable provides a default parameter set. Sync: true, DisableWal: false
	WriteReliable = WriteOptions{Sync: true, DisableWal: false}

	// WriteFast provides a default parameter set. Sync: false, DisableWal: true
	WriteFast = WriteOptions{Sync: false, DisableWal: true}
)

// IteratorOptions is the options for the iterator.
type IteratorOptions struct {
	// Prefix filters the keys by prefix.
	Prefix []byte

	// Reverse indicates whether the iterator is reversed.
	// false is forward, true is backward.
	Reverse bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:           tempDBDir(),
	MemtableSize:      64 * MB,
	MemtableNums:      15,
	BlockCache:        0,
	Sync:              false,
	BytesPerSync:      0,
	PartitionNum:      3,
	KeyHashFunction:   xxhash.Sum64,
	ValueLogFileSize:  1 * GB,
	IndexType:         BTree,
	CompactBatchCount: 10000,
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "lotusdb-temp")
	return dir
}
