package lotusdb

import (
	"context"
	"time"

	"github.com/flower-corp/lotusdb/index"
	"github.com/flower-corp/lotusdb/logfile"
	"github.com/flower-corp/lotusdb/logger"
	"github.com/flower-corp/lotusdb/memtable"
)

func (cf *ColumnFamily) waitMemSpace() error {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	if !cf.activeMem.IsFull() {
		return nil
	}

	select {
	case cf.flushChn <- cf.activeMem:
		cf.immuMems = append(cf.immuMems, cf.activeMem)
		// open a new active memtable.
		var ioType = logfile.FileIO
		if cf.opts.WalMMap {
			ioType = logfile.MMap
		}
		memOpts := memtable.Options{
			Path:     cf.opts.WalDir,
			Fid:      cf.activeMem.LogFileId() + 1,
			Fsize:    cf.opts.MemtableSize,
			TableTyp: cf.getMemtableType(),
			IoType:   ioType,
			MemSize:  cf.opts.MemtableSize,
		}
		if table, err := memtable.OpenMemTable(memOpts); err != nil {
			return err
		} else {
			cf.activeMem = table
		}
	case <-time.After(cf.opts.MemSpaceWaitTimeout):
		return ErrWaitMemSpaceTimeout
	}
	return nil
}

func (cf *ColumnFamily) listenAndFlush(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// do some thing... eg: cf.Close()
			return
		case table := <-cf.flushChn:
			// iterate and write data to bptree.
			iter := table.NewIterator(false)
			var nodes []*index.IndexerNode
			for iter.Rewind(); iter.Valid(); iter.Next() {
				node := &index.IndexerNode{Key: iter.Key()}
				if len(iter.Value()) >= cf.opts.ValueThreshold {
					valuePos, err := cf.vlog.Write(&logfile.VlogEntry{
						Key:   iter.Key(),
						Value: iter.Value(),
					})
					if err != nil {
						logger.Errorf("write to value log err.%+v", err)
						break
					}
					node.Meta = &index.IndexerMeta{
						Fid:    valuePos.Fid,
						Size:   valuePos.Size,
						Offset: valuePos.Offset,
					}
				} else {
					node.Meta = &index.IndexerMeta{Value: iter.Value()}
				}
				nodes = append(nodes, node)
			}

			if _, err := cf.indexer.PutBatch(nodes); err != nil {
				logger.Errorf("write to indexer err.%+v", err)
				break
			}

			// delete wal after flush to indexer.
			if err := table.DeleteWal(); err != nil {
				logger.Errorf("listenAndFlush: delete wal log file err.%+v", err)
			}

			cf.trimOneImmuMem()
		}
	}
}
