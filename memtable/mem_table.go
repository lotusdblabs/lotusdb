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
		Put(key []byte, value []byte) *logfile.LogEntry
		Get(key []byte) *logfile.LogEntry
		Exist(key []byte) bool
		Remove(key []byte) *logfile.LogEntry
	}

	Memtable struct {
		mem IMemtable
		wal *logfile.LogFile
		opt Options
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

	var count = 0

	// load entries.
	var offset int64 = 0
	for {
		if entry, size, err := wal.Read(offset); err == nil {
			offset += size
			mem.Put(entry.Key, entry.Value)
			count++
		} else {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	fmt.Println("写入到 MemTable 的数据量 : ", count)

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

	if !opts.DisableWal && mt.wal != nil {
		buf, _ := logfile.EncodeEntry(entry)
		if err := mt.wal.Write(buf); err != nil {
			fmt.Println(err)
			return err
		}

		if opts.Sync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}

	mt.mem.Put(key, value)
	return nil
}

func (mt *Memtable) Delete(key []byte, opts Options) error {
	entry := &logfile.LogEntry{
		Key:  key,
		Type: logfile.TypeDelete,
	}

	if !opts.DisableWal && mt.wal != nil {
		buf, _ := logfile.EncodeEntry(entry)
		if err := mt.wal.Write(buf); err != nil {
			return err
		}

		if opts.Sync {
			if err := mt.wal.Sync(); err != nil {
				return err
			}
		}
	}
	mt.mem.Remove(key)
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
