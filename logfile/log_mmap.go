package logfile

import (
	"github.com/flowercorp/lotusdb/mmap"
	"os"
)

type LogMMap struct {
	buf      []byte // a buffer of mmap.
	fd       *os.File
	writeOff int64
}

func OpenLogMMap(path string, fid uint32, ftype LogFileType, fsize int64) (lm LogFile, err error) {
	fileName := getLogFileName(path, fid, ftype)
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	var buf []byte
	buf, err = mmap.Mmap(fd, true, fsize)
	if err != nil {
		return
	}

	lm = &LogMMap{buf: buf, fd: fd, writeOff: 0}
	return
}

func (lm *LogMMap) Write([]byte) error {
	return nil
}

func (lm *LogMMap) Read(off int64) *logEntry {
	return nil
}

func (lm *LogMMap) Sync() error {
	return nil
}

func (lm *LogMMap) Close() error {
	return nil
}
