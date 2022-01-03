package ioselector

import (
	"io"
	"os"

	"github.com/flowercorp/lotusdb/mmap"
)

type MMapSelector struct {
	fd     *os.File
	buf    []byte // a buffer of mmap
	bufLen int64
}

// NewMMapSelector create a new mmap.
func NewMMapSelector(fName string, fsize int64) (IOSelector, error) {
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.Mmap(file, true, fsize)
	if err != nil {
		return nil, err
	}

	return &MMapSelector{fd: file, buf: buf, bufLen: int64(len(buf))}, nil
}

func (lm *MMapSelector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length+offset >= lm.bufLen {
		return 0, io.EOF
	}
	return copy(lm.buf[offset:], b), nil
}

func (lm *MMapSelector) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= lm.bufLen {
		return 0, io.EOF
	}
	if offset+int64(len(b)) >= lm.bufLen {
		return 0, io.EOF
	}
	return copy(b, lm.buf[offset:]), nil
}

func (lm *MMapSelector) Sync() error {
	return mmap.Msync(lm.buf)
}

func (lm *MMapSelector) Close() error {
	err := mmap.Munmap(lm.buf)
	if err != nil {
		return err
	}
	return lm.fd.Close()
}
