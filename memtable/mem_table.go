package memtable

import (
	"fmt"
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/wal"
	"io"
)

type TableType int8

const (
	SkipListRep TableType = iota
	HashSkipListRep
)

type (
	IMemtable interface {
		Put(key []byte, value []byte) *logfile.LogEntry
		Get(key []byte) *logfile.LogEntry
		Exist(key []byte) bool
		Remove(key []byte) *logfile.LogEntry
		MemSize() int64
	}

	Memtable struct {
		mem     IMemtable
		wal     *wal.Wal
		memSize int64
	}
)

func OpenMemTable(path string, fid uint32, fsize int64, tableType TableType, ioType logfile.IOType) (*Memtable, error) {
	mem := getIMemtable(tableType)
	table := &Memtable{mem: mem}

	openedWal, err := wal.OpenWal(path, fid, fsize, ioType)
	if err != nil {
		return nil, err
	}

	// load entries.
	var offset int64 = 0
	if openedWal != nil {
		for {
			if entry, err := openedWal.Read(offset); err == nil {
				offset += int64(entry.Size())
				mem.Put(entry.Key, entry.Value)
			} else {
				if err == io.EOF {
					break
				}
				return nil, err
			}
		}
		table.wal = openedWal
	}
	return table, nil
}

func (mt *Memtable) Put(key []byte, value []byte) error {
	entry := &logfile.LogEntry{
		Key:   key,
		Value: value,
	}

	if mt.wal != nil {
		if err := mt.wal.Write(entry); err != nil {
			return err
		}
	}

	mt.mem.Put(key, value)
	return nil
}

func (mt *Memtable) SyncWAL() error {
	return mt.wal.Sync()
}

func (mt *Memtable) Get(key []byte) []byte {
	entry := mt.mem.Get(key)
	if entry == nil {
		return nil
	}

	return entry.Value
}

func (mt *Memtable) IsFull(size int64) bool {
	if mt.mem.MemSize()+size >= mt.memSize {
		return true
	}

	if mt.wal == nil {
		return false
	}

	return mt.wal.WriteAt+size >= mt.memSize
}

func getIMemtable(tType TableType) IMemtable {
	switch tType {
	case HashSkipListRep:
		return NewHashSkipList()
	case SkipListRep:
		return NewSkipList()
	default:
		panic(fmt.Sprintf("unsupported table type: %d", tType))
	}
}
