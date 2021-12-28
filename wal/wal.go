package wal

import (
	"github.com/flowercorp/lotusdb/logfile"
)

type Wal struct {
	logFile *logfile.LogFile
	path    string
}

func NewWal(path string) *Wal {
	return &Wal{
		path: path,
	}
}

func (w *Wal) Sync() error {
	return nil
}
