package wal

import (
	"github.com/flowercorp/lotusdb/logfile"
)

type Wal struct {
	logfile.LogFileIO
	path string
}

func NewWal(path string) *Wal {
	return &Wal{
		path: path,
	}
}
