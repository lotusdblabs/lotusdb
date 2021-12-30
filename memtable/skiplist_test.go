package memtable

import (
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSkipList_Put(t *testing.T) {
	var entry = &logfile.LogEntry{
		Key:   []byte("lotusdb"),
		Value: []byte("lotusdb"),
	}

	skl := NewSkipList()

	actEntry := skl.Put([]byte("lotusdb"), []byte("lotusdb"))
	require.Equal(t, entry, actEntry)
}

func TestSkipList_Get(t *testing.T) {
	var entry = &logfile.LogEntry{
		Key:   []byte("lotusdb"),
		Value: []byte("lotusdb"),
	}

	skl := NewSkipList()

	skl.Put([]byte("lotusdb"), []byte("lotusdb"))

	actEntry := skl.Get([]byte("lotusdb"))
	require.Equal(t, entry, actEntry)
}

func TestSkipList_Exist(t *testing.T) {
	var entry = &logfile.LogEntry{
		Key:   []byte("lotusdb"),
		Value: []byte("lotusdb"),
	}

	skl := NewSkipList()

	skl.Put([]byte("lotusdb"), []byte("lotusdb"))

	actEntry := skl.Get([]byte("lotusdb"))
	require.Equal(t, entry, actEntry)
	assert.True(t, skl.Exist([]byte("lotusdb")))

	skl.Remove([]byte("lotusdb"))
	assert.False(t, skl.Exist([]byte("lotusdb")))
}

func TestSkipList_Remove(t *testing.T) {
	var entry = &logfile.LogEntry{
		Key:   []byte("lotusdb"),
		Value: []byte("lotusdb"),
	}

	skl := NewSkipList()

	skl.Put([]byte("lotusdb"), []byte("lotusdb"))

	actEntry := skl.Get([]byte("lotusdb"))
	require.Equal(t, entry, actEntry)

	skl.Remove([]byte("lotusdb"))
	assert.Nil(t, skl.Get([]byte("lotusdb")))
}

func TestSkipList_MemSize(t *testing.T) {
	hashlist := NewHashSkipList()

	hashlist.Put([]byte("lotusdb"), []byte("lotusdb"))
	hashlist.Put([]byte("test"), []byte("test"))

	require.Equal(t, int64(22), hashlist.MemSize())
}
