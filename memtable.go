package lotusdb

import (
	"encoding/binary"
	"github.com/flower-corp/lotusdb/arenaskl"
	"github.com/flower-corp/lotusdb/logfile"
	"github.com/flower-corp/lotusdb/logger"
	"io"
	"time"
)

type (
	Memtable struct {
		sklIter *arenaskl.Iterator
		wal     *logfile.LogFile
		opts    memOptions
	}

	memOptions struct {
		path    string
		fid     uint32
		fsize   int64
		ioType  logfile.IOType
		memSize int64
	}

	memValue struct {
		value     []byte
		expiredAt int64
	}
)

func openMemtable(opts memOptions) (*Memtable, error) {
	// init skip list and arena.
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(uint32(opts.memSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &Memtable{opts: opts, sklIter: sklIter}

	// open wal log file.
	wal, err := logfile.OpenLogFile(opts.path, opts.fid, opts.fsize*2, logfile.WAL, opts.ioType)
	if err != nil {
		return nil, err
	}
	table.wal = wal

	// load entries.
	var offset int64 = 0
	for {
		if entry, size, err := wal.ReadLogEntry(offset); err == nil {
			offset += size
			err := table.sklIter.Put(entry.Key, entry.Value, uint16(entry.Type))
			if err != nil {
				logger.Errorf("put value into skip list err.%+v", err)
				return nil, err
			}
		} else {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return table, nil
}

// Put .
func (mt *Memtable) Put(key []byte, value []byte, deleted bool, opts WriteOptions) error {
	entry := &logfile.LogEntry{
		Key:   key,
		Value: value,
	}
	if opts.ExpiredAt > 0 {
		entry.ExpiredAt = opts.ExpiredAt
	}
	if deleted {
		entry.Type = logfile.TypeDelete
	}

	// write entrt into wal first.
	buf, _ := logfile.EncodeEntry(entry)
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

	// write data into skip list in memory.
	mv := memValue{value: value, expiredAt: entry.ExpiredAt}
	mvBuf := mv.encode()
	err := mt.sklIter.Put(key, mvBuf, uint16(entry.Type))
	return err
}

// Get .
func (mt *Memtable) Get(key []byte) []byte {
	found := mt.sklIter.Seek(key)
	if !found || mt.sklIter.Meta() == uint16(logfile.TypeDelete) {
		return nil
	}

	mv := decodeMemValue(mt.sklIter.Value())
	if len(mv.value) == 0 || mv.expiredAt <= time.Now().Unix() {
		return nil
	}
	return mv.value
}

func (mv *memValue) encode() []byte {
	head := make([]byte, 10)
	var index int
	index += binary.PutVarint(head[index:], mv.expiredAt)

	buf := make([]byte, len(mv.value)+index)
	copy(buf[:index], head[:])
	copy(buf[index:], mv.value)
	return buf
}

func decodeMemValue(buf []byte) memValue {
	var index int
	ex, n := binary.Varint(buf[index:])
	index += n
	return memValue{expiredAt: ex, value: buf[index:]}
}
