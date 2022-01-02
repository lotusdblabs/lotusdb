package memtable

import (
	"strconv"
	"testing"

	"github.com/flowercorp/lotusdb/logfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestSkipList_NewSklIterator(t *testing.T) {
	skl := NewSkipList()
	for i := 0; i < 100; i++ {
		v := strconv.Itoa(i)
		skl.Put([]byte(v), []byte("skl-val"))
	}

	iter := skl.NewSklIterator(false)
	for iter.Seek([]byte("5")); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
}
