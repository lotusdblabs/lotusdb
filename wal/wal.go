package wal

import (
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/util"
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
	currentSize uint64
	index       Index
	path        string
	FileMaxSize uint64
}

type Index struct {
	KeyOffsetMap   map[uint64]uint64
	ValueOffsetMap map[uint64]uint64
	sync.RWMutex
}

func NewIndex() Index {
	return Index{
		KeyOffsetMap:   make(map[uint64]uint64),
		ValueOffsetMap: make(map[uint64]uint64),
	}
}
func (idx *Index) SetKeyOffset(keyId, offset uint64) {
	idx.Lock()
	defer idx.Unlock()
	idx.KeyOffsetMap[keyId] = offset
}
func (idx *Index) SetValueOffset(valueId, offset uint64) {
	idx.Lock()
	defer idx.Unlock()
	idx.ValueOffsetMap[valueId] = offset
}
func (idx *Index) GetOffsetByKeyId(kId uint64) uint64 {
	idx.RLock()
	defer idx.Unlock()
	id, ok := idx.KeyOffsetMap[kId]
	if !ok {
		return FILE_MAX_SIZE + 1
	}
	return id
}
func (idx *Index) GetOffsetByValueId(vId uint64) uint64 {
	idx.RLock()
	defer idx.Unlock()
	return idx.ValueOffsetMap[vId]
}

func (wal *Wal) Open(path string) error {
	logFile, err := logfile.OpenLogFile(path, 1, 1<<20, logfile.WAL, logfile.MMap)
	if err != nil {
		return err
	}
	wal.logFile = logFile
	return nil
}

func (wal *Wal) Write(entry *logfile.LogEntry) error {
	hash := getKeyHash(entry.Key)
	wal.index.SetKeyOffset(hash, wal.currentSize)
	wal.currentSize += uint64(entry.Size())
	return wal.logFile.Write(entry)
}
func (wal *Wal) Get(key []byte) (*logfile.LogEntry, error) {
	keyId := getKeyHash(key)
	offset := wal.getOffsetByKeyId(keyId)
	return wal.logFile.Read(int64(offset))
}

func (wal *Wal) getOffsetByKeyId(kId uint64) uint64 {
	return wal.index.GetOffsetByKeyId(kId)
}

func (wal *Wal) Delete() bool {
	err := os.Remove(wal.path)
	return err == nil
}

func getKeyHash(key []byte) uint64 {
	return hash(key)
}
func hash(buf []byte) uint64 {
	return util.MemHash(buf)
}

func NewWal(path string) *Wal {
	return &Wal{
		path:        path,
		currentSize: 0,
		FileMaxSize: FILE_MAX_SIZE,
		index:       NewIndex(),
	}
}

func (w *Wal) Sync() error {
	return nil
}
