package lotusdb

import (
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/flowercorp/lotusdb/memtable"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func (cf *ColumnFamily) waitMemSpace() error {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	if !cf.activeMem.IsFull() {
		return nil
	}
	select {
	case cf.flushChn <- cf.activeMem:
		cf.immuMems = append([]*memtable.Memtable{cf.activeMem}, cf.immuMems...)
		// open a new active memtable.
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
			return err
		} else {
			cf.activeMem = table
		}
	case <-time.After(cf.opts.MemSpaceWaitTimeout):
		return ErrWaitMemSpaceTimeout
	}
	return nil
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

			// add a new active mem.(if necessary)
		case <-sig:
			return
		// db closed or cf closed
		//case <-closed:
		}
	}
}
