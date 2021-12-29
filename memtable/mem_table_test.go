package memtable

import (
	"github.com/flowercorp/lotusdb/logfile"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOpenMenTable(t *testing.T) {
	var opt = &options{
		fsize:     10 * 1204,
		tableType: SkipListRep,
		ioType:    logfile.MMap,
	}
	mem, err := openMemTable("/tmp", 0, opt)
	require.NoError(t, err)
	require.NotNil(t, mem)
}

func TestMemtable_Get(t *testing.T) {
	var opt = &options{
		fsize:     10 * 1204,
		tableType: SkipListRep,
		ioType:    logfile.MMap,
	}
	mem, err := openMemTable("/tmp", 0, opt)
	require.NoError(t, err)
	require.NotNil(t, mem)

	require.Equal(t, []byte(nil), mem.Get([]byte("lotusdb")))

	err = mem.Put([]byte("lotusdb"), []byte("123"))
	require.NoError(t, err)

	require.Equal(t, []byte("123"), mem.Get([]byte("lotusdb")))
}

func TestMemtable_Put(t *testing.T) {
	var opt = &options{
		fsize:     10 * 1204,
		tableType: SkipListRep,
		ioType:    logfile.MMap,
	}
	mem, err := openMemTable("/tmp", 1, opt)
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

func TestOpenMenTables(t *testing.T) {
	mem, immuMems, err := OpenMenTables("/tmp")
	require.NoError(t, err)
	require.NotNil(t, mem)
	require.NotNil(t, immuMems)

	require.Equal(t, []byte("1"), mem.Get([]byte("test1")))

	require.Equal(t, []byte("123"), immuMems[0].Get([]byte("lotusdb")))

}
