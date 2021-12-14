package wal

import "os"

type Wal struct {
	logFile *os.File
}
