package lotusdb

import (
	"encoding/binary"
	"github.com/flower-corp/lotusdb/arenaskl"
	"github.com/flower-corp/lotusdb/logfile"
	"github.com/flower-corp/lotusdb/logger"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const paddedSize = 64

type (
	// memtable is an in-memory data structure holding data before they are flushed into indexer and value log.
	// Currently the only supported data structure is skip list, see arenaskl.Skiplist.
	// New writes always insert data to memtable, and reads has query from memtable before reading from indexer and vlog, because memtable`s data is newer.
	// Once a memtable is full(memtable has its threshold, see MemtableSize in options), it becomes immutable and replaced by a new memtable.
	// A background goroutine will flush the content of memtable into indexer or vlog, after which the memtable can be deleted.
	memtable struct {
		sync.RWMutex
		sklIter      *arenaskl.Iterator
		skl          *arenaskl.Skiplist
		wal          *logfile.LogFile
		bytesWritten uint32 // number of bytes written, used for flush wal file.
		opts         memOptions
	}

	// options held by memtable for opening new memtables.
	memOptions struct {
		path       string
		fid        uint32
		fsize      int64
		ioType     logfile.IOType
		memSize    uint32
		bytesFlush uint32
	}

	// in-memory values stored in memtable.
	memValue struct {
		value     []byte
		expiredAt int64
		typ       byte
	}
)

// memtable holds a wal(write ahead log), so when opening a memtable, actually it open the corresponding wal file.
// and load all entries from wal to rebuild the content of the skip list.
func openMemtable(opts memOptions) (*memtable, error) {
	// init skip list and arena.
	var sklIter = new(arenaskl.Iterator)
	arena := arenaskl.NewArena(opts.memSize + uint32(arenaskl.MaxNodeSize))
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
			// No need to use atomic updates.
			// This function is only be executed in one goroutine at startup.
			wal.WriteAt += size

			mv := &memValue{
				value:     entry.Value,
				expiredAt: entry.ExpiredAt,
				typ:       byte(entry.Type),
			}
			mvBuf := mv.encode()
			var err error
			if table.sklIter.Seek(entry.Key) {
				err = table.sklIter.Set(mvBuf)
			} else {
				err = table.sklIter.Put(entry.Key, mvBuf)
			}
			if err != nil {
				logger.Errorf("put value into skip list err.%+v", err)
				return nil, err
			}
		} else {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
	}
	return table, nil
}

// put new writes to memtable.
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

	// write entrty into wal first.
	buf, sz := logfile.EncodeEntry(entry)
	if !opts.DisableWal && mt.wal != nil {
		if err := mt.wal.Write(buf); err != nil {
			return err
		}

		// if Sync is ture in WriteOptions or bytesWritten has reached bytesFlush, syncWal will be true.
		var syncWal = opts.Sync
		if mt.opts.bytesFlush > 0 {
			writes := atomic.AddUint32(&mt.bytesWritten, uint32(sz))
			if writes > mt.opts.bytesFlush {
				syncWal = true
				atomic.StoreUint32(&mt.bytesWritten, 0)
			}
		}
		if syncWal {
			if err := mt.syncWAL(); err != nil {
				return err
			}
		}
	}

	// write data into skip list in memory.
	mv := memValue{value: value, expiredAt: entry.ExpiredAt, typ: byte(entry.Type)}
	mvBuf := mv.encode()
	if mt.sklIter.Seek(key) {
		return mt.sklIter.Set(mvBuf)
	}
	return mt.sklIter.Put(key, mvBuf)
}

// get value from memtable.
// if the specified key is marked as deleted or expired, a true bool value is returned.
func (mt *memtable) get(key []byte) (bool, []byte) {
	mt.Lock()
	defer mt.Unlock()
	if found := mt.sklIter.Seek(key); !found {
		return false, nil
	}

	mv := decodeMemValue(mt.sklIter.Value())
	// ignore deleted key.
	if mv.typ == byte(logfile.TypeDelete) {
		return true, nil
	}
	// ignore expired key.
	if mv.expiredAt > 0 && mv.expiredAt <= time.Now().Unix() {
		return true, nil
	}
	return false, mv.value
}

// delete operation is to put a key and a special tombstone value.
func (mt *memtable) delete(key []byte, opts WriteOptions) error {
	return mt.put(key, nil, true, opts)
}

func (mt *memtable) syncWAL() error {
	mt.wal.RLock()
	defer mt.wal.RUnlock()
	return mt.wal.Sync()
}

func (mt *memtable) isFull(delta uint32) bool {
	if mt.skl.Size()+delta+paddedSize >= mt.opts.memSize {
		return true
	}
	if mt.wal == nil {
		return false
	}

	walSize := atomic.LoadInt64(&mt.wal.WriteAt)
	return walSize >= int64(mt.opts.memSize)
}

func (mt *memtable) logFileId() uint32 {
	return mt.wal.Fid
}

func (mt *memtable) deleteWal() error {
	mt.wal.Lock()
	defer mt.wal.Unlock()
	return mt.wal.Delete()
}

func (mv *memValue) encode() []byte {
	head := make([]byte, 11)
	head[0] = mv.typ
	var index = 1
	index += binary.PutVarint(head[index:], mv.expiredAt)

	buf := make([]byte, len(mv.value)+index)
	copy(buf[:index], head[:])
	copy(buf[index:], mv.value)
	return buf
}

func decodeMemValue(buf []byte) memValue {
	var index = 1
	ex, n := binary.Varint(buf[index:])
	index += n
	return memValue{typ: buf[0], expiredAt: ex, value: buf[index:]}
}
