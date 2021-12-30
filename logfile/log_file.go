package logfile

import (
	"errors"
	"fmt"
	"github.com/flowercorp/lotusdb/ioselector"
	"io"
	"os"
	"sync"
)

var (
	ErrInvalidCrc        = errors.New("logfile: invalid crc")
	ErrWriteSizeNotEqual = errors.New("write size is not equal to entry size")
)

const (
	// PathSeparator default path separator.
	PathSeparator = string(os.PathSeparator)

	// WalSuffixName log file suffix name of write ahead log.
	WalSuffixName = ".wal"

	// VLogSuffixName log file suffix name of value log.
	VLogSuffixName = ".vlog"

	InitialLogFileId = 0
)

// FileType log file of wal and value log.
type FileType int8

const (
	// WAL write ahead log.
	WAL FileType = iota

	// ValueLog value log.
	ValueLog
)

type IOType int8

const (
	FileIO IOType = iota
	MMap
)

type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IoSelector ioselector.IOSelector
}

func OpenLogFile(path string, fid uint32, fsize int64, ftype FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{Fid: fid}
	fileName := lf.getLogFileName(path, fid, ftype)

	var selector ioselector.IOSelector
	switch ioType {
	case FileIO:
		if selector, err = ioselector.NewFileIOSelector(fileName, fsize); err != nil {
			return
		}
	case MMap:
		if selector, err = ioselector.NewMMapSelector(fileName, fsize); err != nil {
			return
		}
	default:
		panic(fmt.Sprintf("unsupported io type : %d", ioType))
	}

	lf.IoSelector = selector
	return
}

// Read .
func (lf *LogFile) Read(offset int64) (*LogEntry, error) {
	// read entry header.
	headerBuf, err := lf.readBytes(offset, maxHeaderSize)
	if err != nil {
		return nil, err
	}
	header, size := decodeHeader(headerBuf)

	kSize, vSize := int64(header.kSize), int64(header.vSize)
	if kSize == 0 && vSize == 0 {
		return nil, io.EOF
	}

	// read entry key and value.
	kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
	if err != nil {
		return nil, err
	}

	e := &LogEntry{
		Key:       kvBuf[:kSize],
		Value:     kvBuf[kSize:],
		ExpiredAt: header.expiredAt,
	}
	// crc32 check.
	if crc := getEntryCrc(e, headerBuf); crc != header.crc32 {
		return nil, ErrInvalidCrc
	}
	return e, nil
}

// Write .
func (lf *LogFile) Write(e *LogEntry) error {
	buf, size := encodeEntry(e)
	n, err := lf.IoSelector.Write(buf, lf.WriteAt)
	if err != nil {
		return err
	}
	if n != size {
		return ErrWriteSizeNotEqual
	}
	lf.WriteAt += int64(size)
	return nil
}

// Sync .
func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

// Close .
func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}

func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)
	return
}

func (lf *LogFile) getLogFileName(path string, fid uint32, ftype FileType) string {
	fname := path + PathSeparator + fmt.Sprintf("%09d", fid)
	switch ftype {
	case WAL:
		return fname + WalSuffixName
	case ValueLog:
		return fname + VLogSuffixName
	default:
		panic(fmt.Sprintf("unsupported log file type: %d", ftype))
	}
}
