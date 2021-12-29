package memtable

import (
	"errors"
	"fmt"
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/wal"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
)

type TableType int8

const (
	SkipListRep TableType = iota
	HashSkipListRep
)

var (
	ErrOptionsNil = errors.New("options is null")
	ErrOpenWal    = errors.New("can not open %s")
	ErrUpdateMem  = errors.New("can not update memtable:%s")
	ErrWriteWal   = errors.New("can not write %s")
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
		wal *wal.Wal
	}

	options struct {
		fsize      int64
		tableType  TableType
		disableWal bool
		readOnly   bool
		ioType     logfile.IOType
	}
)

func OpenMenTables(path string) (*Memtable, []*Memtable, error) {
	opt := &options{}

	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}

	var fids []uint32
	for _, file := range fileInfos {
		if strings.Contains(file.Name(), logfile.WalSuffixName) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return nil, nil, err
			}
			fids = append(fids, uint32(fid))
		}
	}

	// load in order.
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	// open memtable only.
	mem, err := openMemTable(path, fids[len(fids)-1], opt)
	if err != nil {
		return nil, nil, err
	}

	// load other immemtables .
	var immuMems []*Memtable
	for i := 0; i < len(fids)-1; i++ {
		immuMem, err := openMemTable(path, fids[i], opt)
		if err != nil {
			return nil, nil, err
		}
		immuMems = append(immuMems, immuMem)
	}

	return mem, immuMems, nil
}

func openMemTable(path string, fid uint32, opt *options) (*Memtable, error) {
	if opt == nil {
		return nil, ErrOptionsNil
	}

	var memtable Memtable
	imem := getIMemtable(opt.tableType)
	memtable.mem = imem

	if !opt.disableWal {

		memtable.wal = wal.NewWal(path)
		err := memtable.wal.Open(path, fid, opt.fsize, opt.ioType)
		if err != nil {
			return nil, fmt.Errorf(ErrOpenWal.Error(), path)
		}

		err = memtable.UpdateMemtable()
		if err != nil {
			return nil, fmt.Errorf(ErrUpdateMem.Error(), err.Error())
		}

	}

	return &memtable, nil
}

func (mt *Memtable) Put(key []byte, value []byte) error {
	entry := &logfile.LogEntry{
		Key:   key,
		Value: value,
	}

	if mt.wal != nil {
		if err := mt.wal.Write(entry); err != nil {
			return fmt.Errorf(ErrWriteWal.Error(), mt.wal.Path)
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

func (mt *Memtable) UpdateMemtable() error {
	if mt.wal == nil || mt.mem == nil {
		return nil
	}

	var index int64 = 0

	if mt.wal != nil {
		for {
			if entry, err := mt.wal.Read(index); err == nil {
				index += int64(entry.Size())
				mt.mem.Put(entry.Key, entry.Value)
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}
	}

	mt.wal.SetOffset(index)

	return nil
}

func (mt *Memtable) IsFull() bool {
	if mt.wal == nil {
		return false
	}

	return mt.wal.CurrentSize >= mt.wal.FileMaxSize
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
