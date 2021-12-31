package memtable

import (
	"testing"

	"github.com/flowercorp/lotusdb/logfile"
	"github.com/stretchr/testify/require"
)

func TestOpenMenTable(t *testing.T) {
	mem, err := OpenMemTable("/tmp", 0, 10*1204, SkipListRep, logfile.MMap)
	require.NoError(t, err)
	require.NotNil(t, mem)
}

func TestMemtable_Get(t *testing.T) {
	mem, err := OpenMemTable("/tmp", 0, 10*1204, SkipListRep, logfile.MMap)
	require.NoError(t, err)
	require.NotNil(t, mem)

	require.Equal(t, []byte(nil), mem.Get([]byte("lotusdb")))

	err = mem.Put([]byte("lotusdb"), []byte("123"))
	require.NoError(t, err)

	require.Equal(t, []byte("123"), mem.Get([]byte("lotusdb")))
}

func TestMemtable_Put(t *testing.T) {
	mem, err := OpenMemTable("/tmp", 1, 10*1204, SkipListRep, logfile.MMap)
	require.NoError(t, err)
	require.NotNil(t, mem)

	err = mem.Put([]byte("test1"), []byte("1"))
	require.NoError(t, err)

	err = mem.Put([]byte("test2"), []byte("2"))
	require.NoError(t, err)

	err = mem.Put([]byte("test3"), []byte("3"))
	require.NoError(t, err)

	err = mem.Put([]byte("test4"), []byte("4"))
	require.NoError(t, err)
}
