package lotusdb

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestBasic(t *testing.T) {
	vlog, err := openValueLog(os.TempDir())
	require.Nil(t, err)

	// test vlog.put() and test vlog.get()
	entry1 := &LogRecord{
		Key:   []byte("key 1"),
		Value: []byte("value 1"),
		Type:  LogRecordNormal,
	}
	pos, err := vlog.write(encodeLogRecord(entry1))
	require.Nil(t, err)
	value1, err := vlog.read(pos)
	require.Nil(t, err)
	require.Equal(t, []byte("value 1"), value1)
	entry2 := &LogRecord{
		Key:   []byte("key 2"),
		Value: []byte("value 2"),
		Type:  LogRecordNormal,
	}
	pos2, err := vlog.write(encodeLogRecord(entry2))
	require.Nil(t, err)
	value2, err := vlog.read(pos2)
	require.Nil(t, err)
	require.Equal(t, []byte("value 2"), value2)

}

func TestOptions(t *testing.T) {
	vlog, err := openValueLog("./tmp/lotusdb", WithSegmentFileExt(".VLOGTEST"),
		WithSegmentSize(MB),
		WithBlockCache(32*KB*20),
		WithSync(true),
		WithBytesPerSync(MB))
	require.Nil(t, err)

	// test vlog.put() and test vlog.get()
	entry1 := &LogRecord{
		Key:   []byte("key 1"),
		Value: []byte("value 1"),
		Type:  LogRecordNormal,
	}
	pos, err := vlog.write(encodeLogRecord(entry1))
	require.Nil(t, err)
	value1, err := vlog.read(pos)
	require.Nil(t, err)
	require.Equal(t, []byte("value 1"), value1)
}
