package logfile

import "os"

// LogFileIO log file is used by wal and value log.
type LogFileIO struct {
	fd       *os.File
	writeOff int64
}

func OpenLogFileIO(path string, fid uint32, ftype LogFileType) (LogFile, error) {
	fileName := getLogFileName(path, fid, ftype)
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	lf := &LogFileIO{fd: fd, writeOff: 0}
	return lf, nil
}

func (lf *LogFileIO) Write([]byte) error {

	return nil
}

func (lf *LogFileIO) Read(off int64) *logEntry {
	return nil
}

func (lf *LogFileIO) Sync() error {
	return nil
}

func (lf *LogFileIO) Close() error {
	return nil
}
