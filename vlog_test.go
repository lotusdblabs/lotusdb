package lotusdb

import (
	"strings"
	"testing"

	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/require"
)

func TestVlogBasic(t *testing.T) {
	opts := valueLogOptions{
		DirPath:     "/tmp/lotusdb",
		SegmentSize: wal.GB,
		BlockCache:  32 * wal.KB * 10,
	}

	// test opening vlog
	vlog, err := openValueLog(opts)
	require.Nil(t, err)

	defer vlog.wal.Delete()

	data := [3][]byte{[]byte("data 0"), []byte("data 1"), []byte("data 2")}
	pos := []*wal.ChunkPosition{}

	// test writing
	t.Run("test writing", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			p, err := vlog.write(data[i])
			require.Nil(t, err)
			pos = append(pos, p)
		}
	})
	vlog.sync()

	// test reading
	t.Run("test reading", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			d, err := vlog.read(pos[i])
			require.Nil(t, err)
			require.Equal(t, data[i], d)
		}
	})

}

func TestRWLarge(t *testing.T) {
	opts := valueLogOptions{
		DirPath:     "/tmp/lotusdb",
		SegmentSize: wal.GB,
		BlockCache:  32 * wal.KB * 10,
	}
	numRW := 100000

	// test opening vlog
	vlog, err := openValueLog(opts)
	require.Nil(t, err)

	defer vlog.wal.Delete()

	pos := []*wal.ChunkPosition{}
	val := strings.Repeat("v", 512)
	for i := 0; i < numRW; i++ {
		p, err := vlog.write([]byte(val))
		require.Nil(t, err)
		pos = append(pos, p)
	}
	vlog.sync()

	for i := 0; i < numRW; i++ {
		_, err := vlog.read(pos[i])
		require.Nil(t, err)
	}

}
