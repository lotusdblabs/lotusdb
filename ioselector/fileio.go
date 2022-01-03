package ioselector

import "os"

// FileIOSelector log file is used by wal and value log.
type FileIOSelector struct {
	fd *os.File // system file descriptor.
}

// NewFileIOSelector create a new file io.
func NewFileIOSelector(fName string, fsize int64) (IOSelector, error) {
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{fd: file}, nil
}

func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}

func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}

func (fio *FileIOSelector) Delete() error {
	_ = fio.fd.Close()
	return os.Remove(fio.fd.Name())
}
