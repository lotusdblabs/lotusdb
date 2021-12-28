package memtable

import (
	"errors"
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/wal"
)

type MemAlg int

var (
	writeWALErr = errors.New("can not write wal file")
)

const (
	Skl MemAlg = iota
	HashSkl
)

type IMemtable interface {
	Put(key []byte, value interface{}) *Element
	Get(key []byte) *Element
	Exist(key []byte) bool
	Remove(key []byte) *Element
	Foreach(fun handleEle)
	FindPrefix(prefix []byte) *Element
}

type Memtable struct {
	mem IMemtable
	wal *wal.Wal
}

type MemOption struct {
	size       int64  // memtable size
	mode       MemAlg // algorithm kind
	walMMap    bool   //
	disableWal bool   //
	fileType   int
	ioType     int
}

func getIMemtable(mode MemAlg) IMemtable {
	switch mode {
	case Skl:
		return NewSkipList()
	case HashSkl:
		return nil
	default:
		return NewSkipList()
	}
}

func OpenMemTable(path string, fid uint32, opt *MemOption) *Memtable {
	var memtable Memtable

	imem := getIMemtable(opt.mode)
	memtable.mem = imem

	if !opt.disableWal {

		lf, err := logfile.OpenLogFile(path, fid, opt.size, FileType(opt.fileType), IOType(opt.ioType))
		if err != nil {
			return nil
		}
		memtable.wal = lf
		memtable.UpdateMemtable()
	}

	return &memtable
}

func newMemTable(path string, mode MemAlg) *Memtable {
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

func (mt *Memtable) UpdateMemtable() {
	if mt.wal != nil {
		mt.wal.Read()
	}
}
