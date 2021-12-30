package ioselector

import (
	"os"
)

// FilePerm default permission of the newly created log file.
const FilePerm = 0644

// IOSelector io selector for fileio and mmap
type IOSelector interface {
	// Write a log entry to log file.
	Write(b []byte, offset int64) (int, error)

	// Read a log entry from offset.
	Read(b []byte, offset int64) (int, error)

	// Sync commits the current contents of the file to stable storage.
	// Typically, this means flushing the file system's in-memory copy
	// of recently written data to disk.
	Sync() error

	// Close closes the File, rendering it unusable for I/O.
	// It will return an error if it has already been called.
	Close() error
}

// open file and truncate it if necessary.
func openFile(fName string, fsize int64) (*os.File, error) {
	fd, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < fsize {
		if err := fd.Truncate(fsize); err != nil {
			return nil, err
		}
	}
	return fd, nil
}
