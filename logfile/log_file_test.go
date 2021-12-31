package logfile

//
//import (
//	"github.com/stretchr/testify/assert"
//	"testing"
//	"time"
//)
//
//const (
//	dirname = "/tmp"
//	fszie   = 16 * 1024 * 1024
//)
//
//func TestOpenLogFile(t *testing.T) {
//	t.Run("ftype:wal iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 0, fszie, WAL, FileIO)
//		assert.Nil(t, err)
//		assert.NotNil(t, lf)
//	})
//
//	t.Run("ftype:wal iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 1, fszie, WAL, MMap)
//		assert.Nil(t, err)
//		assert.NotNil(t, lf)
//	})
//
//	t.Run("ftype:valuelog iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 2, fszie, ValueLog, FileIO)
//		assert.Nil(t, err)
//		assert.NotNil(t, lf)
//	})
//
//	t.Run("ftype:valuelog iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 3, fszie, ValueLog, MMap)
//		assert.Nil(t, err)
//		assert.NotNil(t, lf)
//	})
//
//	t.Run("ftype:unsupported iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 0, fszie, 2, MMap)
//		assert.Nil(t, err)
//		assert.NotNil(t, lf)
//	})
//
//	t.Run("ftype:valuelog iotype:unsupported", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 0, fszie, ValueLog, 2)
//		assert.Nil(t, err)
//		assert.NotNil(t, lf)
//	})
//
//}
//
//func TestLogFile_Read(t *testing.T) {
//	t.Run("ftype:wal iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 0, fszie, WAL, FileIO)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//
//		entry, err := lf.Read(0)
//		assert.Nil(t, err)
//
//		assert.Equal(t, getLogEntry(), entry)
//	})
//
//	t.Run("ftype:wal iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 1, fszie, WAL, MMap)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//
//		entry, err := lf.Read(0)
//		assert.Nil(t, err)
//
//		assert.Equal(t, getLogEntry(), entry)
//	})
//
//	t.Run("ftype:valuelog iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 2, fszie, ValueLog, FileIO)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//
//		entry, err := lf.Read(0)
//		assert.Nil(t, err)
//
//		assert.Equal(t, getLogEntry(), entry)
//	})
//
//	t.Run("ftype:valuelog iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 3, fszie, ValueLog, MMap)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//
//		entry, err := lf.Read(0)
//		assert.Nil(t, err)
//
//		assert.Equal(t, getLogEntry(), entry)
//	})
//}
//
//func TestLogFile_Write(t *testing.T) {
//	t.Run("ftype:wal iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 0, fszie, WAL, FileIO)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:wal iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 1, fszie, WAL, MMap)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:valuelog iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 2, fszie, ValueLog, FileIO)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:valuelog iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 3, fszie, ValueLog, MMap)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//	})
//}
//
//func TestLogFile_Sync(t *testing.T) {
//	t.Run("ftype:wal iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 0, fszie, WAL, FileIO)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//
//		err = lf.Sync()
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:wal iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 1, fszie, WAL, MMap)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//
//		err = lf.Sync()
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:valuelog iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 2, fszie, ValueLog, FileIO)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//
//		err = lf.Sync()
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:valuelog iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 3, fszie, ValueLog, MMap)
//		assert.Nil(t, err)
//		err = lf.Write(getLogEntry())
//		assert.Nil(t, err)
//
//		err = lf.Sync()
//		assert.Nil(t, err)
//	})
//
//}
//
//func TestLogFile_Close(t *testing.T) {
//	t.Run("ftype:wal iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 0, fszie, WAL, FileIO)
//		assert.Nil(t, err)
//
//		err = lf.Close()
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:wal iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 1, fszie, WAL, MMap)
//		assert.Nil(t, err)
//
//		err = lf.Close()
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:valuelog iotype:file", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 2, fszie, ValueLog, FileIO)
//		assert.Nil(t, err)
//
//		err = lf.Close()
//		assert.Nil(t, err)
//	})
//
//	t.Run("ftype:valuelog iotype:mmap", func(t *testing.T) {
//		lf, err := OpenLogFile(dirname, 3, fszie, ValueLog, MMap)
//		assert.Nil(t, err)
//
//		err = lf.Close()
//		assert.Nil(t, err)
//	})
//}
//
//func getLogEntry() *LogEntry {
//	return &LogEntry{
//		Key:       []byte("lotusdb"),
//		Value:     []byte("lotusdb"),
//		ExpiredAt: uint64(time.Now().Add(time.Hour).Unix()),
//	}
//}
