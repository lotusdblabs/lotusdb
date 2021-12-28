package memtable

import (
	"errors"
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

func newMemTable(path string, mode MemAlg) *Memtable {
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
