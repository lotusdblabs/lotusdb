package memtable

import (
	"testing"

	"github.com/flower-corp/lotusdb/logfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashSkipList_Put(t *testing.T) {
	hashlist := NewHashSkipList()
	hashlist.Put([]byte("lotusdb"), []byte("lotusdb"))
}

func TestHashSkipList_Get(t *testing.T) {
	var entry = &logfile.LogEntry{
		Key:   []byte("lotusdb"),
		Value: []byte("lotusdb"),
	}

	hashlist := NewHashSkipList()

	hashlist.Put([]byte("lotusdb"), []byte("lotusdb"))

	actEntry := hashlist.Get([]byte("lotusdb"))
	require.Equal(t, entry, actEntry)
}

func TestHashSkipList_Remove(t *testing.T) {
	var entry = &logfile.LogEntry{
		Key:   []byte("lotusdb"),
		Value: []byte("lotusdb"),
	}

	hashlist := NewHashSkipList()

	hashlist.Put([]byte("lotusdb"), []byte("lotusdb"))

	actEntry := hashlist.Get([]byte("lotusdb"))
	require.Equal(t, entry, actEntry)

	hashlist.Remove([]byte("lotusdb"))
	assert.Nil(t, hashlist.Get([]byte("lotusdb")))
}

func TestHashSkipList_MemSize(t *testing.T) {
	hashlist := NewHashSkipList()

	hashlist.Put([]byte("lotusdb"), []byte("lotusdb"))
	hashlist.Put([]byte("test"), []byte("test"))

	require.Equal(t, int64(22), hashlist.MemSize())
}
