package logfile

import "fmt"

const (
	// FilePerm default permission of the new created log file.
	FilePerm = 0644
)

type LogFileType int8

const (
	WAL LogFileType = iota
	ValueLog
)

type LogFile interface {
	Write([]byte) error
	Read(off int64) *logEntry
	Sync() error
	Close() error
}

func getLogFileName(path string, fid uint32, ftype LogFileType) string {
	fname := path + fmt.Sprintf("%9d", fid)
	if ftype == WAL {
		fname += ".wal"
	}
	if ftype == ValueLog {
		fname += ".vlog"
	}
	return fname
}
