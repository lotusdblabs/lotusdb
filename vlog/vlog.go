package vlog

import (
	"errors"
	"fmt"
	"github.com/flower-corp/lotusdb/logfile"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrActiveLogFileNil .
	ErrActiveLogFileNil = errors.New("active log file not exists")
	// ErrLogFileNil .
	ErrLogFileNil = errors.New("log file %d not exists")
)

type (
	// ValueLog value log.
	ValueLog struct {
		sync.RWMutex
		opt           options
		activeLogFile *logfile.LogFile            // current active log file for writing.
		logFiles      map[uint32]*logfile.LogFile // all log files. Must hold the mutex before modify it.
	}

	// ValuePos value position.
	ValuePos struct {
		Fid    uint32
		Size   uint32
		Offset int64
	}

	options struct {
		path      string
		blockSize int64
		ioType    logfile.IOType
	}
)

// OpenValueLog create a new value log file.
func OpenValueLog(path string, blockSize int64, ioType logfile.IOType) (*ValueLog, error) {
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
		return fids[i] > fids[j]
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
	return vlog, nil
}

// Read .
func (vlog *ValueLog) Read(fid, size uint32, offset int64) (*logfile.VlogEntry, error) {
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

	b, err := logFile.Read(offset, size)
	if err != nil {
		return nil, err
	}
	return logfile.DecodeVlogEntry(b), nil
}

// Write .
func (vlog *ValueLog) Write(ve *logfile.VlogEntry) (*ValuePos, error) {
	buf, eSize := logfile.EncodeVlogEntry(ve)
	// if active is reach to thereshold, close it and open a new one.
	if vlog.activeLogFile.WriteAt+int64(eSize) >= vlog.opt.blockSize {
		vlog.Lock()
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
		Size:   uint32(eSize),
		Offset: writeAt - int64(eSize),
	}, nil
}

// Sync .
func (vlog *ValueLog) Sync() error {
	if vlog.activeLogFile == nil {
		return ErrActiveLogFileNil
	}

	vlog.activeLogFile.Lock()
	defer vlog.activeLogFile.Unlock()
	return vlog.activeLogFile.Sync()
}

// Close .
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

// do it later.
func (vlog *ValueLog) compact() {
	// todo
}
