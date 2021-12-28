package memtable

import (
	"github.com/flowercorp/lotusdb/wal"
)

type TableType int8

const (
	SkipListRep TableType = iota
	HashSkipListRep
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

func MewMemTable(path string, mode TableType) *Memtable {
	return &Memtable{
		mem: getIMemtable(mode),
		wal: wal.NewWal(path),
	}
}

func (mt *Memtable) Put(key []byte, value interface{}) error {
	if mt.wal != nil {

	}

	mt.mem.Put(key, value)
	return nil
}

func (mt *Memtable) SyncWAL() error {
	return mt.wal.Sync()
}

func getIMemtable(mode TableType) IMemtable {
	switch mode {
	case HashSkipListRep:
		// NewHashKsipList
		return nil
	default:
		return NewSkipList()
	}
}
