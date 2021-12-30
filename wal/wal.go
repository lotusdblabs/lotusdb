package wal

import (
	"github.com/flowercorp/lotusdb/logfile"
	"hash/crc32"
	"os"
	"sync"
)

const (
	FILE_MAX_SIZE = 1 << 22
)

type Config struct {
	path        string
	maxFileSize int64
}

type Wal struct {
	logFile     *logfile.LogFile
	CurrentSize uint64
	index       Index
	Path        string
	FileMaxSize uint64
}

type Index struct {
	KeyOffsetMap   map[uint32]uint64
	ValueOffsetMap map[uint32]uint64
	sync.RWMutex
}

func NewIndex() Index {
	return Index{
		KeyOffsetMap:   make(map[uint32]uint64),
		ValueOffsetMap: make(map[uint32]uint64),
	}
}

func (idx *Index) setKeyOffset(keyId uint32, offset uint64) {
	idx.Lock()
	defer idx.Unlock()
	idx.KeyOffsetMap[keyId] = offset
}

func (idx *Index) setValueOffset(valueId uint32, offset uint64) {
	idx.Lock()
	defer idx.Unlock()
	idx.ValueOffsetMap[valueId] = offset
}

func (idx *Index) getOffsetByKeyId(kId uint32) uint64 {
	idx.RLock()
	defer idx.Unlock()
	id, ok := idx.KeyOffsetMap[kId]
	if !ok {
		return FILE_MAX_SIZE + 1
	}
	return id
}

func (idx *Index) getOffsetByValueId(vId uint32) uint64 {
	idx.RLock()
	defer idx.Unlock()
	return idx.ValueOffsetMap[vId]
}

func (wal *Wal) getOffsetByKeyId(kId uint32) uint64 {
	return wal.index.getOffsetByValueId(kId)
}

func (wal *Wal) setKeyOffset(keyId uint32, offset uint64) {
	wal.index.setValueOffset(keyId, offset)
}

func OpenWal(path string, fid uint32, fsize int64, ioType logfile.IOType) (*Wal, error) {
	logFile, err := logfile.OpenLogFile(path, fid, fsize, logfile.WAL, ioType)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		logFile: logFile,
	}
	return wal, nil
}

func (wal *Wal) Write(entry *logfile.LogEntry) error {
	hash32 := getKeyHash(entry.Key)
	wal.setKeyOffset(hash32, wal.CurrentSize)
	wal.CurrentSize += uint64(entry.Size())

	return wal.logFile.Write(entry)
}

func (wal *Wal) Get(key []byte) (*logfile.LogEntry, error) {
	keyId := getKeyHash(key)
	offset := wal.getOffsetByKeyId(keyId)

	return wal.logFile.Read(int64(offset))
}

func (wal *Wal) Read(offset int64) (*logfile.LogEntry, error) {
	return wal.logFile.Read(offset)
}

func (wal *Wal) Delete() bool {
	err := os.Remove(wal.Path)

	return err == nil
}

func (wal *Wal) Sync() error {
	return wal.logFile.Sync()
}

func (wal *Wal) SetOffset(offset int64) {
	wal.logFile.WriteAt = offset
}

func getKeyHash(key []byte) uint32 {
	return hash(key)
}

func hash(buf []byte) uint32 {
	return crc32.ChecksumIEEE(buf)
}
