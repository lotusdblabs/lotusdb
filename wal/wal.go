package wal

import (
	"github.com/flowercorp/lotusdb/logfile"
)

type Wal struct {
	logfile.LogFile
	path string
}

func NewWal(path string) *Wal {
	return &Wal{
		path: path,
	}
}
