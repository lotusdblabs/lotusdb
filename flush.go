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

func (cf *ColumnFamily) waitMemSpace(size uint32) error {
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
			path:    cf.opts.DirPath,
			fid:     cf.activeMem.logFileId() + 1,
			fsize:   int64(cf.opts.MemtableSize),
			ioType:  ioType,
			memSize: cf.opts.MemtableSize,
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
			iter := table.sklIter
			// iterate and write data to bptree and value log(if any).
			for table.sklIter.SeekToFirst(); iter.Valid(); iter.Next() {
				node := &index.IndexerNode{Key: iter.Key()}
				mv := decodeMemValue(iter.Value())
				key := iter.Key()

				// delete invalid keys from indexer.
				if mv.typ == byte(logfile.TypeDelete) || (mv.expiredAt != 0 && mv.expiredAt <= time.Now().Unix()) {
					deletedKeys = append(deletedKeys, key)
				} else {
					if len(mv.value) >= cf.opts.ValueThreshold {
						valuePos, err := cf.vlog.Write(&logfile.LogEntry{Key: key, Value: mv.value})
						if err != nil {
							logger.Errorf("write to value log err.%+v", err)
							return
						}
						node.Meta = &index.IndexerMeta{
							Fid:    valuePos.Fid,
							Offset: valuePos.Offset,
						}
					} else {
						node.Meta = &index.IndexerMeta{Value: mv.value}
					}
					nodes = append(nodes, node)
				}
			}

			if _, err := cf.indexer.PutBatch(nodes); err != nil {
				logger.Errorf("write to indexer err.%+v", err)
				break
			}
			if err := cf.indexer.DeleteBatch(deletedKeys); err != nil {
				logger.Errorf("delete keys in indexer err.%+v", err)
				break
			}
			// must fsync before delete wal.
			if err := cf.indexer.Sync(); err != nil {
				logger.Errorf("sync indexer err.%+v", err)
				break
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
		case <-sig:
			return
			// cf has closed.
		case <-cf.closedC:
			return
		}
	}
}
