package lotusdb

import (
	"os"
	"time"

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

	// writing entries to disk after reading the specified memory capacity of entries.
	CompactBatchCapacity int

	// deprecatedtable recommend compaction rate
	AdvisedCompactionRate float32

	// deprecatedtable force compaction rate
	ForceCompactionRate float32

	// sampling interval of diskIO, unit is millisecond
	DiskIOSamplingInterval int

	// window is used to retrieve the state of IO bury over a period of time
	DiskIOSamplingWindow int

	// rate of io time in the sampling time is used to represent the busy state of io
	DiskIOBusyRate float32

	// AutoCompactSupport support
	AutoCompactSupport bool

	// WaitMemSpaceTimeout specifies the timeout for waiting for space in the memtable.
	// When all memtables are full, it will be flushed to disk by the background goroutine.
	// But if the flush speed is slower than the write speed, there may be no space in the memtable.
	// So the write operation will wait for space in the memtable, and the timeout is specified by WaitMemSpaceTimeout.
	// If the timeout is exceeded, the write operation will fail, you can try again later.
	// Default value is 100ms.
	WaitMemSpaceTimeout time.Duration
}

// BatchOptions specifies the options for creating a batch.
type BatchOptions struct {
	// WriteOptions used in batch operation
	WriteOptions

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
	DirPath: tempDBDir(),
	//nolint:gomnd // default
	MemtableSize: 64 * MB,
	//nolint:gomnd // default
	MemtableNums: 15,
	Sync:         false,
	BytesPerSync: 0,
	//nolint:gomnd // default
	PartitionNum:     3,
	KeyHashFunction:  xxhash.Sum64,
	ValueLogFileSize: 1 * GB,
	IndexType:        BTree,
	//nolint:gomnd // default
	CompactBatchCapacity: 1 << 30,
	//nolint:gomnd // default
	AdvisedCompactionRate: 0.3,
	//nolint:gomnd // default
	ForceCompactionRate: 0.5,
	//nolint:gomnd // default
	DiskIOSamplingInterval: 100,
	//nolint:gomnd // default
	DiskIOSamplingWindow: 10,
	//nolint:gomnd // default
	DiskIOBusyRate:     0.5,
	AutoCompactSupport: true,
	//nolint:gomnd // default
	WaitMemSpaceTimeout: 100 * time.Millisecond,
}

var DefaultBatchOptions = BatchOptions{
	WriteOptions: DefaultWriteOptions,
	ReadOnly:     false,
}

var DefaultWriteOptions = WriteOptions{
	Sync:       false,
	DisableWal: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "lotusdb-temp")
	return dir
}
