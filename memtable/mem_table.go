package memtable

import (
	"fmt"
	"io"

	"github.com/flowercorp/lotusdb/logfile"
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
	}

	Memtable struct {
		mem IMemtable
		wal *logfile.LogFile
	}
)

func OpenMemTable(path string, fid uint32, size int64, tableType TableType, ioType logfile.IOType) (*Memtable, error) {
	mem := getIMemtable(tableType)
	table := &Memtable{mem: mem}

	wal, err := logfile.OpenLogFile(path, fid, size*2, logfile.WAL, ioType)
	if err != nil {
		return nil, err
	}

	// load entries.
	var offset int64 = 0
	if wal != nil {
		for {
			if entry, size, err := wal.Read(offset); err == nil {
				offset += size
				mem.Put(entry.Key, entry.Value)
			} else {
				if err == io.EOF {
					break
				}
				return nil, err
			}
		}
		table.wal = wal
	}
	return table, nil
}

func (mt *Memtable) Put(key []byte, value []byte) error {
	entry := &logfile.LogEntry{
		Key:   key,
		Value: value,
	}

	if mt.wal != nil {
		buf, _ := logfile.EncodeEntry(entry)
		if err := mt.wal.Write(buf); err != nil {
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

func (mt *Memtable) IsFull() bool {
	return false
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
