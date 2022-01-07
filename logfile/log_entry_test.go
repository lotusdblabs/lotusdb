package logfile

import (
	"hash/crc32"
	"testing"
)

func TestEncodeEntry(t *testing.T) {
	e := &LogEntry{
		Key:   []byte("rose"),
		Value: []byte("duan"),
	}

	buf, n := EncodeEntry(e)
	t.Log(buf)
	t.Log(n)

	headerBuf, i := decodeHeader(buf)
	t.Log(headerBuf.crc32)

	t.Log(getEntryCrc(e, buf[crc32.Size:i]))
}
