package lotusdb

import (
	"github.com/rosedblabs/wal"
)

const (
	valueLogFileExt = ".VLOG"
)

// valueLog value log is named after the concept in Wisckey paper
// https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf
type valueLog struct {
	wal *wal.WAL
}

type valueLogOptions struct {
	// DirPath specifies the directory path where the WAL segment files will be stored.
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	SegmentSize int64

	// BlockCache specifies the size of the block cache in number of bytes.
	// A block cache is used to store recently accessed data blocks, improving read performance.
	// If BlockCache is set to 0, no block cache will be used.
	BlockCache uint32
}

func openValueLog(options valueLogOptions) (*valueLog, error) {
	vLogWal, err := wal.Open(wal.Options{
		DirPath:        options.DirPath,
		SegmentSize:    options.SegmentSize,
		SegmentFileExt: valueLogFileExt,
		BlockCache:     options.BlockCache,
		Sync:           false,
		BytesPerSync:   0,
	})
	if err != nil {
		return nil, err
	}
	return &valueLog{wal: vLogWal}, nil
}

func (vlog *valueLog) read(position *wal.ChunkPosition) ([]byte, error) {
	value, err := vlog.wal.Read(position)
	if err != nil {
		return nil, err
	}
	logRecord := decodeLogRecord(value)
	return logRecord.Value, nil
}

func (vlog *valueLog) write(data []byte) (*wal.ChunkPosition, error) {
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
