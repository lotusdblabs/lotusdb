package lotusdb

import (
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/logger"
	"github.com/flowercorp/lotusdb/memtable"
	"os"
	"os/signal"
	"syscall"
)

func (cf *ColumnFamily) sendFlushTask() {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	select {
	case cf.flushChn <- cf.activeMem:
		cf.immuMems = append([]*memtable.Memtable{cf.activeMem}, cf.immuMems...)
		// open a new active memtable.
		if len(cf.immuMems) < cf.opts.MemtableNums {
			var ioType = logfile.FileIO
			if cf.opts.WalMMap {
				ioType = logfile.MMap
			}
			memOpts := memtable.Options{
				Path:     cf.opts.WalDir,
				Fsize:    cf.opts.MemtableSize,
				TableTyp: cf.getMemtableType(),
				IoType:   ioType,
			}
			if table, err := memtable.OpenMemTable(memOpts); err != nil {
				logger.Errorf("open memtable err.%+v", err)
			} else {
				cf.activeMem = table
			}
		}
	default:
		logger.Errorf("can`t send flush task to flush chan.")
	}
}

func (cf *ColumnFamily) listenAndFlush() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		select {
		case table := <-cf.flushChn:
			// iterate and write data to bptree.

			// close wal log.
			table.SyncWAL()

			// remove wal log.

			// modify cf immuMems.
		case <-sig:
			return
			// db close or cf close
			//case <-closed:

		}
	}
}
