package wal

import (
	"github.com/flowercorp/lotusdb/logfile"
)

type Wal struct {
	logFile *logfile.LogFile
}
