package flock

import (
	"fmt"
	"os"
)

type FileLockGuard struct {
	fd *os.File
}

func AcquireFileLock(path string, readOnly bool) (*FileLockGuard, error) {
	var (
		flag int
		mode os.FileMode
	)
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_RDWR
		mode = os.ModeExclusive
	}

	file, err := os.OpenFile(path, flag, mode)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, mode|0644)
	}
	if err != nil {
		return nil, err
	}
	return &FileLockGuard{fd: file}, nil
}

func SyncDir(path string) error {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	err = fd.Sync()
	closeErr := fd.Close()
	if err != nil {
		return fmt.Errorf("sync dir err: %+v", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close dir err: %+v", err)
	}
	return nil
}

func (fl *FileLockGuard) Release() error {
	return fl.fd.Close()
}
