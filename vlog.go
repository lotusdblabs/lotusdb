package lotusdb

import "github.com/rosedblabs/wal"

// valueLog value log is named after the concept in Wisckey paper
// (https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).
type valueLog struct {
	wal *wal.WAL
}
