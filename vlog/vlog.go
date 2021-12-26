package vlog

import "github.com/flowercorp/lotusdb/logfile"

type ValueLog struct {
	activeFid uint32                      // active file id.
	logFiles  map[uint32]*logfile.LogFile // all log files.
}

// Open create a new log file.
func (vlog *ValueLog) Open(path string, fisze int64) {

}

func (vlog *ValueLog) Close() error {
	return nil
}

func (vlog *ValueLog) Sync() error {
	return nil
}

//
func (vlog *ValueLog) runGC() {

}
