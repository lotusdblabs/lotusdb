package lotusdb

import (
	"github.com/rosedblabs/wal"
	"sync"
)

// valueLog value log is named after the concept in Wisckey paper
// https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf
const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

type (
	valueLog struct {
		sync.RWMutex
		wal *wal.WAL
	}
	VlogOptions func(config *wal.Options)
)

func WithSegmentSize(segmentSize int64) VlogOptions {
	return func(config *wal.Options) {
		config.SegmentSize = segmentSize
	}
}
func WithSegmentFileExt(segmentFileExt string) VlogOptions {
	return func(config *wal.Options) {
		config.SementFileExt = segmentFileExt
	}
}
func WithBlockCache(blockCache uint32) VlogOptions {
	return func(config *wal.Options) {
		config.BlockCache = blockCache
	}
}
func WithSync(sync bool) VlogOptions {
	return func(config *wal.Options) {
		config.Sync = sync
	}
}
func WithBytesPerSync(bytesPerSync uint32) VlogOptions {
	return func(config *wal.Options) {
		config.BytesPerSync = bytesPerSync
	}
}
func defaultVlogConfig() *wal.Options {
	return &wal.Options{
		SegmentSize:   wal.DefaultOptions.SegmentSize,
		SementFileExt: ".VLOG",
		BlockCache:    wal.DefaultOptions.BlockCache,
		Sync:          wal.DefaultOptions.Sync,
		BytesPerSync:  wal.DefaultOptions.BytesPerSync,
	}
}
func openValueLog(dirPath string, options ...VlogOptions) (*valueLog, error) {
	walOpts := defaultVlogConfig()
	walOpts.DirPath = dirPath
	for _, op := range options {
		op(walOpts)
	}
	vLogWal, err := wal.Open(*walOpts)
	if err != nil {
		return nil, err
	}
	return &valueLog{wal: vLogWal}, nil
}

func (vlog *valueLog) read(position *wal.ChunkPosition) ([]byte, error) {
	// read from wal
	value, err := vlog.wal.Read(position)
	if err != nil {
		return nil, err
	}
	logRecord := decodeLogRecord(value)
	return logRecord.Value, nil
}

// The data here should be converted from LogRecord by encodingLogRecord
func (vlog *valueLog) write(data []byte) (*wal.ChunkPosition, error) {
	// write to wal
	pos, err := vlog.wal.Write(data)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (vlog *valueLog) sync() error {
	return vlog.wal.Sync()
}

func (vlog *valueLog) close() error {
	return vlog.wal.Close()
}
func handleCompaction() {

}
