package memtable

import (
	"testing"

	"github.com/flowercorp/lotusdb/logfile"
	"github.com/stretchr/testify/require"
)

func TestOpenMenTable(t *testing.T) {
	memOpts := Options{
		Path:     "/tmp",
		Fid:      0,
		Fsize:    10 * 1024,
		TableTyp: SkipListRep,
		IoType:   logfile.MMap,
	}

	mem, err := OpenMemTable(memOpts)
	require.NoError(t, err)
	require.NotNil(t, mem)
}

func TestMemtable_Get(t *testing.T) {
	memOpts := Options{
		Path:     "/tmp",
		Fid:      0,
		Fsize:    10 * 1024,
		TableTyp: SkipListRep,
		IoType:   logfile.MMap,
	}
	mem, err := OpenMemTable(memOpts)
	require.NoError(t, err)
	require.NotNil(t, mem)

	require.Equal(t, []byte(nil), mem.Get([]byte("lotusdb")))

	err = mem.Put([]byte("lotusdb"), []byte("123"), Options{})
	require.NoError(t, err)

	require.Equal(t, []byte("123"), mem.Get([]byte("lotusdb")))
}

func TestMemtable_Put(t *testing.T) {
	memOpts := Options{
		Path:     "/tmp",
		Fid:      1,
		Fsize:    10 * 1024,
		TableTyp: SkipListRep,
		IoType:   logfile.MMap,
	}

	mem, err := OpenMemTable(memOpts)
	require.NoError(t, err)
	require.NotNil(t, mem)

	err = mem.Put([]byte("test1"), []byte("1"), Options{})
	require.NoError(t, err)

	err = mem.Put([]byte("test2"), []byte("2"), Options{})
	require.NoError(t, err)

	err = mem.Put([]byte("test3"), []byte("3"), Options{})
	require.NoError(t, err)

	err = mem.Put([]byte("test4"), []byte("4"), Options{})
	require.NoError(t, err)
}
