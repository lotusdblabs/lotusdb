package memtable

import (
	"errors"
	"github.com/flowercorp/lotusdb/wal"
)

type memAlg int

const (
	Skl memAlg = iota
	HashSkl
	APT
)

var (
	writeWALErr = errors.New("can not write wal file")
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

func getIMemtable(mode memAlg) IMemtable {
	switch mode {
	case Skl:
		return NewSkipList()
	case HashSkl:
		return nil
	case APT:
		return nil
	default:
		return NewSkipList()
	}
}

func newMemTable(path string, mode memAlg) *Memtable {
	return &Memtable{
		mem: getIMemtable(mode),
		wal: wal.NewWal(path),
	}
}

func (mt *Memtable) Put(key []byte, value interface{}) error {
	if mt.wal != nil {
		// TODO 需要修改成写入一个entry
		if err := mt.wal.Write(key); err != nil {
			return writeWALErr
		}
	}

	mt.mem.Put(key, value)
	return nil
}

func (mt *Memtable) SyncWAL() error {
	return mt.wal.Sync()
}
