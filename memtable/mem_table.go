package memtable

import (
	"fmt"
	"github.com/flowercorp/lotusdb/logfile"
	"io"
)

type TableType int8

const (
	SkipListRep TableType = iota
	HashSkipListRep
)

type (
	IMemtable interface {
		Put(key []byte, value []byte)
		Get(key []byte) *logfile.LogEntry
		Remove(key []byte) *logfile.LogEntry
		Iterator(reversed bool) MemIterator
	}

	MemIterator interface {
		Next()
		Prev()
		Rewind()
		Seek([]byte)
		Key() []byte
		Value() []byte
		Valid() bool
	}

	Memtable struct {
		mem IMemtable
		wal *logfile.LogFile
		opt Options
		// memCost represents how much memory is used.
		// This is an inaccurate field to do so, we will use a efficient way(like Arena) to replace it in near future.
		memCost int64
	}

	Options struct {
		// options for opening a memtable.
		Path     string
		Fid      uint32
		Fsize    int64
		TableTyp TableType
		IoType   logfile.IOType

		// options for writing.
		Sync       bool
		DisableWal bool
		ExpiredAt  int64
	}
)

func OpenMemTable(opts Options) (*Memtable, error) {
	mem := getIMemtable(opts.TableTyp)
	table := &Memtable{mem: mem, opt: opts}

	// open wal log file.
	wal, err := logfile.OpenLogFile(opts.Path, opts.Fid, opts.Fsize*2, logfile.WAL, opts.IoType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	// load entries.
	var offset int64 = 0
	for {
		if entry, size, err := wal.ReadLogEntry(offset); err == nil {
			offset += size
			mem.Put(entry.Key, entry.Value)
		} else {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return table, nil
}

func (mt *Memtable) Put(key []byte, value []byte, opts Options) error {
	entry := &logfile.LogEntry{
		Key:   key,
		Value: value,
	}
	if opts.ExpiredAt > 0 {
		entry.ExpiredAt = opts.ExpiredAt
	}

	buf, eSize := logfile.EncodeEntry(entry)
	if !opts.DisableWal && mt.wal != nil {
		if err := mt.wal.Write(buf); err != nil {
			return err
		}
		if opts.Sync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}

	mt.mem.Put(key, value)
	mt.memCost += int64(eSize)
	return nil
}

func (mt *Memtable) Delete(key []byte, opts Options) error {
	entry := &logfile.LogEntry{
		Key:  key,
		Type: logfile.TypeDelete,
	}

	buf, eSize := logfile.EncodeEntry(entry)
	if !opts.DisableWal && mt.wal != nil {
		if err := mt.wal.Write(buf); err != nil {
			return err
		}

		if opts.Sync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}
	removed := mt.mem.Remove(key)
	if removed != nil {
		eSize += len(removed.Value)
	}
	mt.memCost -= int64(eSize)
	return nil
}

func (mt *Memtable) Get(key []byte) []byte {
	entry := mt.mem.Get(key)
	if entry == nil {
		return nil
	}

	return entry.Value
}

func (mt *Memtable) SyncWAL() error {
	return mt.wal.Sync()
}

func (mt *Memtable) IsFull() bool {
	if mt.memCost >= mt.opt.Fsize {
		return true
	}
	return mt.wal.WriteAt >= mt.opt.Fsize
}

// DeleteWal delete wal.
func (mt *Memtable) DeleteWal() error {
	return mt.wal.Delete()
}

func (mt *Memtable) LogFileId() uint32 {
	return mt.wal.Fid
}

// NewIterator .
func (mt *Memtable) NewIterator(reversed bool) MemIterator {
	return mt.mem.Iterator(reversed)
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
