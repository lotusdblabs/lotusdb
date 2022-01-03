package logfile

import "testing"

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
	t.Log(i)

	t.Log(getEntryCrc(e, buf[:8]))
}
