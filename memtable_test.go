package lotusdb

import (
	"fmt"
	"io/ioutil"
	"math"
	"strings"
	"testing"

	arenaskl "github.com/dgraph-io/badger/v4/skl"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	var opts = memOptions{
		path:         "/tmp/lotusdb",
		walId:        0,
		memSize:      64 << 20,
		walByteFlush: 100000,
		WALSync:      false,
		maxBatchSize: int64(50 * arenaskl.MaxNodeSize),
	}
	memtable, err := openMemtable(&opts)
	defer memtable.wal.Delete()
	require.Nil(t, err)

	entryOpts := EntryOptions{
		Sync:       false,
		DisableWal: false,
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

// test whether memtable can generate multiple segment files normally
func TestWriteLarge(t *testing.T) {
	var opts = memOptions{
		path:         "/tmp/lotusdb",
		walId:        0,
		memSize:      64 << 20,
		walByteFlush: 100000,
		WALSync:      false,
		maxBatchSize: int64(50 * arenaskl.MaxNodeSize),
	}
	wal.DefaultOptions.SegmentSize = 5 * wal.MB
	memtable, err := openMemtable(&opts)

	defer memtable.wal.Delete()
	require.Nil(t, err)

	entryOpts := EntryOptions{
		Sync:       false,
		DisableWal: false,
	}

	val := strings.Repeat("v", 512)
	for i := 0; i < 100000; i++ {
		memtable.put([]byte("key"), []byte(val), false, entryOpts)
	}

	fileNum, fileSizes, err := walkDir(memtable.opts.path)
	require.Nil(t, err)
	numSegment := int(math.Ceil(float64(fileSizes) / float64(wal.DefaultOptions.SegmentSize)))

	require.Equal(t, numSegment, fileNum)
}

func TestOptions(t *testing.T) {
	var opts = memOptions{
		path:         "/tmp/lotusdb",
		walId:        1,
		memSize:      64 << 20,
		walByteFlush: 100000,
		WALSync:      false,
		maxBatchSize: int64(50 * arenaskl.MaxNodeSize),
	}
	memtable, err := openMemtable(&opts)
	defer memtable.wal.Delete()
	require.Nil(t, err)

	entryOptions := []EntryOptions{
		{Sync: true, DisableWal: false},
		{Sync: false, DisableWal: true},
	}

	err = memtable.put([]byte("key 1"), []byte("value 1"), false, entryOptions[0])
	require.Nil(t, err)

	err = memtable.put([]byte("key 2"), []byte("value 2"), false, entryOptions[1])
	require.Nil(t, err)
	memtable.wal.Sync()
	memtable.wal.Close()
	memtable, err = openMemtable(&opts)
	_, value2 := memtable.get([]byte("key 2"))
	fmt.Printf("%v\n", string(value2))
	require.Nil(t, value2)
}

func walkDir(dir string) (count int, fileSizes int64, err error) {
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return 0, 0, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			count++
			fileSizes += entry.Size()
		}
	}
	return count, fileSizes, nil
}
