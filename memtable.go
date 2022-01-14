package lotusdb

import (
	"encoding/binary"
	"github.com/flower-corp/lotusdb/arenaskl"
	"github.com/flower-corp/lotusdb/logfile"
	"github.com/flower-corp/lotusdb/logger"
	"io"
	"sync/atomic"
	"time"
)

type (
	memtable struct {
		sklIter *arenaskl.Iterator
		skl     *arenaskl.Skiplist
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

func openMemtable(opts memOptions) (*memtable, error) {
	// init skip list and arena.
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(uint32(opts.memSize))
	skl := arenaskl.NewSkiplist(arena)
	sklIter.Init(skl)
	table := &memtable{opts: opts, skl: skl, sklIter: sklIter}

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
func (mt *memtable) put(key []byte, value []byte, deleted bool, opts WriteOptions) error {
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
func (mt *memtable) get(key []byte) []byte {
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

func (mt *memtable) delete(key []byte, opts WriteOptions) error {
	return mt.put(key, nil, true, opts)
}

func (mt *memtable) syncWAL() error {
	mt.wal.RLock()
	defer mt.wal.RUnlock()
	return mt.wal.Sync()
}

func (mt *memtable) isFull() bool {
	if int64(mt.skl.Size()) >= mt.opts.memSize {
		return true
	}
	if mt.wal == nil {
		return false
	}

	walSize := atomic.LoadInt64(&mt.wal.WriteAt)
	return walSize >= mt.opts.memSize
}

func (mt *memtable) logFileId() uint32 {
	return mt.wal.Fid
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
