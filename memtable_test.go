package lotusdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	var opts = memOptions{
		path:         "/tmp/lotusdb",
		dirId:        0,
		memSize:      64 << 20,
		walByteFlush: 100000,
		WALSync:      false,
	}
	memtable, err := openMemtable(&opts)
	require.Nil(t, err)

	entryOpts := EntryOptions{
		Sync:       false,
		DisableWal: false,
		ExpiredAt:  0,
	}

	// test memtable.put() and test memtable.get()
	err = memtable.put([]byte("key 1"), []byte("value 1"), false, entryOpts)
	require.Nil(t, err)
	_, value1 := memtable.get([]byte("key 1"))
	require.Equal(t, []byte("value 1"), value1)

	err = memtable.put([]byte("key 2"), []byte("value 2"), false, entryOpts)
	require.Nil(t, err)
	_, value2 := memtable.get([]byte("key 2"))
	require.Equal(t, []byte("value 2"), value2)

	err = memtable.put([]byte("key 2"), []byte("value 3"), false, entryOpts)
	require.Nil(t, err)
	_, value3 := memtable.get([]byte("key 2"))
	require.Equal(t, []byte("value 3"), value3)

	_, value4 := memtable.get([]byte("key 4"))
	require.Nil(t, value4)

	// test memtable.delete()
	err = memtable.delete([]byte("key 1"), entryOpts)
	require.Nil(t, err)
	_, value1_ := memtable.get([]byte("key 1"))
	require.Nil(t, value1_)
}

func TestOptions(t *testing.T) {
	var opts = memOptions{
		path:         "/tmp/lotusdb",
		dirId:        1,
		memSize:      64 << 20,
		walByteFlush: 100000,
		WALSync:      false,
	}
	memtable, err := openMemtable(&opts)
	require.Nil(t, err)

	entryOptions := []EntryOptions{
		{Sync: true, DisableWal: false, ExpiredAt: 0},
		{Sync: false, DisableWal: true, ExpiredAt: 0},
		{Sync: false, DisableWal: false, ExpiredAt: 1},
	}

	err = memtable.put([]byte("key 1"), []byte("value 1"), false, entryOptions[0])
	require.Nil(t, err)

	err = memtable.put([]byte("key 2"), []byte("value 2"), false, entryOptions[1])
	require.Nil(t, err)
	memtable.wal.Sync()
	memtable.wal.Close()
	memtable, err = openMemtable(&opts)
	_, value2 := memtable.get([]byte("key 2"))
	require.Nil(t, value2)

	err = memtable.put([]byte("key 2"), []byte("value 2"), false, entryOptions[2])
	_, value2 = memtable.get([]byte("key 2"))
	require.Equal(t, []byte("value 2"), value2)
	time.Sleep(time.Second * 2)
	_, value2 = memtable.get([]byte("key 2"))
	require.Nil(t, value2)
}
