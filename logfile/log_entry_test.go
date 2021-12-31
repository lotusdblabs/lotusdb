package logfile

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestLogFile_Close2(t *testing.T) {
	m := math.MaxInt64
	b := make([]byte, 10)
	binary.PutVarint(b[0:], int64(m))
}