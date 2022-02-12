package lotusdb

import (
	"errors"
	"fmt"
	"github.com/flower-corp/lotusdb/index"
	"github.com/flower-corp/lotusdb/logfile"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrActiveLogFileNil active log file not exists.
	ErrActiveLogFileNil = errors.New("active log file not exists")

	// ErrLogFileNil log file not exists.
	ErrLogFileNil = errors.New("log file %d not exists")
)

type (
	// ValueLog value log is named after the concept in Wisckey paper(https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).
	// Values will be stored in value log if its size exceed ValueThreshold in options.
	ValueLog struct {
		sync.RWMutex
		opt           options
		activeLogFile *logfile.LogFile            // current active log file for writing.
		logFiles      map[uint32]*logfile.LogFile // all log files. Must hold the mutex before modify it.
		cf            *ColumnFamily
		ccl           []uint32 // ccl means compaction candidate list, which stores file ids that can be compacted.
	}

	// ValuePos value position.
	ValuePos struct {
		Fid    uint32
		Offset int64
	}

	options struct {
		path      string
		blockSize int64
		ioType    logfile.IOType
	}
)

// openValueLog create a new value log file.
func openValueLog(path string, blockSize int64, ioType logfile.IOType) (*ValueLog, error) {
	opt := options{
		path:      path,
		blockSize: blockSize,
		ioType:    ioType,
	}
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var fids []uint32
	for _, file := range fileInfos {
		if strings.HasSuffix(file.Name(), logfile.VLogSuffixName) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return nil, err
			}
			fids = append(fids, uint32(fid))
		}
	}

	// load in order.
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	if len(fids) == 0 {
		fids = append(fids, logfile.InitialLogFileId)
	}

	// open active log file only.
	logFile, err := logfile.OpenLogFile(path, fids[len(fids)-1], opt.blockSize, logfile.ValueLog, opt.ioType)
	if err != nil {
		return nil, err
	}
	vlog := &ValueLog{
		opt:           opt,
		activeLogFile: logFile,
		logFiles:      make(map[uint32]*logfile.LogFile),
	}

	// load other log files when reading from it.
	for i := 0; i < len(fids)-1; i++ {
		vlog.logFiles[fids[i]] = &logfile.LogFile{Fid: fids[i]}
	}

	if err := vlog.setLogFileState(); err != nil {
		return nil, err
	}
	return vlog, nil
}

// Read a VLogEntry from a specified vlog file at offset, returns an error, if any.
// If reading from a non-active log file, and the specified file is not open, then we will open it and set it into logFiles.
func (vlog *ValueLog) Read(fid uint32, offset int64) (*logfile.LogEntry, error) {
	var logFile *logfile.LogFile
	if fid == vlog.activeLogFile.Fid {
		logFile = vlog.activeLogFile
	} else {
		vlog.RLock()
		logFile = vlog.logFiles[fid]
		if logFile != nil && logFile.IoSelector == nil {
			opt := vlog.opt
			lf, err := logfile.OpenLogFile(opt.path, fid, opt.blockSize, logfile.ValueLog, opt.ioType)
			if err != nil {
				vlog.RUnlock()
				return nil, err
			}
			vlog.logFiles[fid] = lf
			logFile = lf
		}
		vlog.RUnlock()
	}
	if logFile == nil {
		return nil, fmt.Errorf(ErrLogFileNil.Error(), fid)
	}

	entry, _, err := logFile.ReadLogEntry(offset)
	if err == logfile.ErrEndOfEntry {
		return &logfile.LogEntry{}, nil
	}
	return entry, err
}

// Write new VLogEntry to value log file.
// If the active log file is full, it will be closed and a new active file will be created to replace it.
func (vlog *ValueLog) Write(ent *logfile.LogEntry) (*ValuePos, error) {
	buf, eSize := logfile.EncodeEntry(ent)
	// if active is reach to thereshold, close it and open a new one.
	if vlog.activeLogFile.WriteAt+int64(eSize) >= vlog.opt.blockSize {
		vlog.Lock()
		if err := vlog.Sync(); err != nil {
			return nil, err
		}
		vlog.logFiles[vlog.activeLogFile.Fid] = vlog.activeLogFile

		logFile, err := vlog.createLogFile()
		if err != nil {
			vlog.Unlock()
			return nil, err
		}
		vlog.activeLogFile = logFile
		vlog.Unlock()
	}
	err := vlog.activeLogFile.Write(buf)
	if err != nil {
		return nil, err
	}

	writeAt := atomic.LoadInt64(&vlog.activeLogFile.WriteAt)
	return &ValuePos{
		Fid:    vlog.activeLogFile.Fid,
		Offset: writeAt - int64(eSize),
	}, nil
}

// Sync only for the active log file.
func (vlog *ValueLog) Sync() error {
	if vlog.activeLogFile == nil {
		return ErrActiveLogFileNil
	}

	vlog.activeLogFile.Lock()
	defer vlog.activeLogFile.Unlock()
	return vlog.activeLogFile.Sync()
}

// Close only for the active log file.
func (vlog *ValueLog) Close() error {
	if vlog.activeLogFile == nil {
		return ErrActiveLogFileNil
	}

	vlog.activeLogFile.Lock()
	defer vlog.activeLogFile.Unlock()
	return vlog.activeLogFile.Close()
}

func (vlog *ValueLog) createLogFile() (*logfile.LogFile, error) {
	opt := vlog.opt
	fid := vlog.activeLogFile.Fid
	logFile, err := logfile.OpenLogFile(opt.path, fid+1, opt.blockSize, logfile.ValueLog, opt.ioType)
	if err != nil {
		return nil, err
	}
	return logFile, nil
}

func (vlog *ValueLog) setLogFileState() error {
	if vlog.activeLogFile == nil {
		return ErrActiveLogFileNil
	}
	var offset int64 = 0
	for {
		if _, size, err := vlog.activeLogFile.ReadLogEntry(offset); err == nil {
			offset += size
			// No need to use atomic updates.
			// This function is only be executed in one goroutine at startup.
			vlog.activeLogFile.WriteAt += size
		} else {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return err
		}
	}
	// if active file`s capacity is nearly close to block size, open a new active file.
	if vlog.activeLogFile.WriteAt+logfile.MaxHeaderSize >= vlog.opt.blockSize {
		vlog.logFiles[vlog.activeLogFile.Fid] = vlog.activeLogFile
		logFile, err := vlog.createLogFile()
		if err != nil {
			return err
		}
		vlog.activeLogFile = logFile
	}
	return nil
}

func (vlog *ValueLog) compact() error {
	opt := vlog.opt
	for _, fid := range vlog.ccl {
		file, err := logfile.OpenLogFile(opt.path, fid, opt.blockSize, logfile.ValueLog, opt.ioType)
		if err != nil {
			return err
		}
		var offset int64
		var valids []*logfile.LogEntry
		for {
			entry, _, err := file.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}
			meta, err := vlog.cf.indexer.Get(entry.Key)
			if err != nil {
				return err
			}
			// if value is stored in indexer, value in vlog must be old.
			if len(meta.Value) != 0 {
				continue
			}
			if meta.Fid != fid {
				continue
			}
			if meta.Offset != offset {
				continue
			}
			valids = append(valids, entry)
		}

		var nodes []*index.IndexerNode
		// rewrite valid log entries.
		for _, e := range valids {
			valuePos, err := vlog.Write(e)
			if err != nil {
				return err
			}
			nodes = append(nodes, &index.IndexerNode{
				Key:  e.Key,
				Meta: &index.IndexerMeta{Fid: valuePos.Fid, Offset: valuePos.Offset},
			})
		}
		if _, err = vlog.cf.indexer.PutBatch(nodes); err != nil {
			return err
		}
		// delete older vlog file.
		if err = file.Delete(); err != nil {
			return err
		}
	}
	return nil
}
