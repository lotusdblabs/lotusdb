package lotusdb

import (
	"github.com/rosedblabs/wal"
)

// valueLog value log is named after the concept in Wisckey paper
// https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf
type valueLog struct {
	wal *wal.WAL
}

func openValueLog() (*valueLog, error) {
	return nil, nil
}

func (vlog *valueLog) read(position *wal.ChunkPosition) ([]byte, error) {
	// read from wal
	return nil, nil
}

func (vlog *valueLog) write(data []byte) (*wal.ChunkPosition, error) {
	// write to wal
	return nil, nil
}

func (vlog *valueLog) sync() error {
	return vlog.wal.Sync()
}

func (vlog *valueLog) close() error {
	return vlog.wal.Close()
}
