package memtable

import (
	"errors"
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/wal"
)

type TableType int8

const (
	SkipListRep TableType = iota
	HashSkipListRep
)

var ErrOptionsNil = errors.New("options is null")

type (
	IMemtable interface {
		Put(key []byte, value interface{}) *Element
		Get(key []byte) *Element
		Exist(key []byte) bool
		Remove(key []byte) *Element
	}

	Memtable struct {
		mem IMemtable
		wal *wal.Wal
	}

	options struct {
		size       int64
		mode       TableType
		walMMap    bool
		disableWal bool
		fileType   logfile.FileType
		ioType     logfile.IOType
	}
)

func OpenMemTable(path string, fid uint32, opt *options) (*Memtable, error) {
	if opt == nil {
		return nil, ErrOptionsNil
	}

	var memtable Memtable
	imem := getIMemtable(opt.mode)
	memtable.mem = imem

	if !opt.disableWal {

		_, err := logfile.OpenLogFile(path, fid, opt.size, opt.fileType, opt.ioType)
		if err != nil {
			return nil, err
		}

		err = memtable.UpdateMemtable()
		if err != nil {
			return nil, err
		}
	}

	return &memtable, nil
}

func MewMemTable(path string, mode TableType) *Memtable {
	return &Memtable{
		mem: getIMemtable(mode),
		wal: wal.NewWal(path),
	}
}

func (mt *Memtable) Put(key []byte, value interface{}) error {
	if mt.wal != nil {
		mt.wal.Write(key, value)
	}

	mt.mem.Put(key, value)
	return nil
}

func (mt *Memtable) SyncWAL() error {
	return mt.wal.Sync()
}

func (mt *Memtable) Get(key []byte) interface{} {
	return mt.mem.Get(key).Value()
}

func (mt *Memtable) UpdateMemtable() error {
	if mt.wal != nil {
		return mt.wal.Read()
	}

	return nil
}

func getIMemtable(mode TableType) IMemtable {
	switch mode {
	case HashSkipListRep:
		return NewHashSkipList()
	case SkipListRep:
		return NewSkipList()
	default:
		return NewSkipList()
	}
}
