package wal

import "os"

// Options represents the configuration options for a Write-Ahead Log (WAL).
type Options struct {
	// DirPath specifies the directory path where the WAL segment files will be stored.
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	SegmentSize int64

	// SegmentFileExt specifies the file extension of the segment files.
	// The file extension must start with a dot ".", default value is ".SEG".
	// It is used to identify the different types of files in the directory.
	// Now it is used by rosedb to identify the segment files and hint files.
	// Not a common usage for most users.
	SementFileExt string

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
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:       os.TempDir(),
	SegmentSize:   GB,
	SementFileExt: ".SEG",
	BlockCache:    32 * KB * 10,
	Sync:          false,
	BytesPerSync:  0,
}
