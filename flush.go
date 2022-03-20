package lotusdb

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flower-corp/lotusdb/index"
	"github.com/flower-corp/lotusdb/logfile"
	"github.com/flower-corp/lotusdb/logger"
)

func (cf *ColumnFamily) waitWritesMemSpace(size uint32) error {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	if !cf.activeMem.isFull(size) {
		return nil
	}

	timer := time.NewTimer(cf.opts.MemSpaceWaitTimeout)
	defer timer.Stop()
	select {
	case cf.flushChn <- cf.activeMem:
		cf.immuMems = append(cf.immuMems, cf.activeMem)
		// open a new active memtable.
		var ioType = logfile.FileIO
		if cf.opts.WalMMap {
			ioType = logfile.MMap
		}
		memOpts := memOptions{
			path:       cf.opts.DirPath,
			fid:        cf.activeMem.logFileId() + 1,
			fsize:      int64(cf.opts.MemtableSize),
			ioType:     ioType,
			memSize:    cf.opts.MemtableSize,
			bytesFlush: cf.opts.WalBytesFlush,
		}
		if table, err := openMemtable(memOpts); err != nil {
			return err
		} else {
			cf.activeMem = table
		}
	case <-timer.C:
		return ErrWaitMemSpaceTimeout
	}
	return nil
}

func (cf *ColumnFamily) listenAndFlush() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case table := <-cf.flushChn:
			var nodes []*index.IndexerNode
			var deletedKeys [][]byte

			iterateTable := func() error {
				table.Lock()
				defer table.Unlock()
				iter := table.sklIter
				// iterate and write data to bptree and value log(if any).
				for iter.SeekToFirst(); iter.Valid(); iter.Next() {
					key := iter.Key()
					node := &index.IndexerNode{Key: key}
					mv := decodeMemValue(iter.Value())

					// delete invalid keys from indexer.
					if mv.typ == byte(logfile.TypeDelete) || (mv.expiredAt != 0 && mv.expiredAt <= time.Now().Unix()) {
						deletedKeys = append(deletedKeys, key)
					} else {
						valuePos, esize, err := cf.vlog.Write(&logfile.LogEntry{
							Key:       key,
							Value:     mv.value,
							ExpiredAt: mv.expiredAt,
						})
						if err != nil {
							return err
						}
						node.Meta = &index.IndexerMeta{
							Fid:       valuePos.Fid,
							Offset:    valuePos.Offset,
							EntrySize: esize,
						}
						nodes = append(nodes, node)
					}
				}
				return nil
			}

			if err := iterateTable(); err != nil {
				logger.Errorf("listenAndFlush: handle iterate table err.%+v", err)
				return
			}
			if err := cf.flushUpdateIndex(nodes, deletedKeys); err != nil {
				logger.Errorf("listenAndFlush: update index err.%+v", err)
				return
			}
			// delete wal after flush to indexer.
			if err := table.deleteWal(); err != nil {
				logger.Errorf("listenAndFlush: delete wal log file err.%+v", err)
			}

			cf.mu.Lock()
			if len(cf.immuMems) > 1 {
				cf.immuMems = cf.immuMems[1:]
			} else {
				cf.immuMems = cf.immuMems[:0]
			}
			cf.mu.Unlock()
		// cf has closed or force quit.
		case <-sig:
		case <-cf.closedC:
			return
		}
	}
}

func (cf *ColumnFamily) flushUpdateIndex(nodes []*index.IndexerNode, keys [][]byte) error {
	cf.flushLock.Lock()
	defer cf.flushLock.Unlock()
	// must put and delete in batch.
	writeOpts := index.WriteOptions{SendDiscard: true}
	if _, err := cf.indexer.PutBatch(nodes, writeOpts); err != nil {
		return err
	}
	if len(keys) > 0 {
		if err := cf.indexer.DeleteBatch(keys, writeOpts); err != nil {
			return err
		}
	}
	// must fsync before delete wal.
	if err := cf.indexer.Sync(); err != nil {
		return err
	}
	return nil
}
