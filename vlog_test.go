package lotusdb

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/require"
)

func TestVlogBasic(t *testing.T) {
	opts := valueLogOptions{
		dirPath:     "/tmp/lotusdb",
		segmentSize: wal.GB,
		blockCache:  32 * wal.KB * 10,
		numPartions: 5,
	}

	// test opening vlog
	vlog, err := openValueLog(opts)
	entries, err := ioutil.ReadDir(opts.dirPath)
	for _, ent := range entries {
		fmt.Printf("%v\n", ent)
	}

	require.Nil(t, err)

	defer func() {
		fmt.Printf("delete!\n")
		for i := 0; i < int(vlog.numPartions); i++ {
			vlog.wals[i].Delete()
		}
	}()

	logs := [5]*LogRecord{
		{Key: []byte("key 0"), Value: []byte("value 0")},
		{Key: []byte("key 1"), Value: []byte("value 1")},
		{Key: []byte("key 2"), Value: []byte("value 2")},
		{Key: []byte("key 3"), Value: []byte("value 3")},
		{Key: []byte("key 4"), Value: []byte("value 4")},
	}
	pos := []*VlogPosition{}

	// test writing
	t.Run("test writing", func(t *testing.T) {
		pos, err = vlog.writeBatch(logs[:])
		require.Nil(t, err)
	})
	vlog.sync()

	// test reading
	t.Run("test reading", func(t *testing.T) {
		readLogs, err := vlog.readBatch(pos)
		require.Nil(t, err)
		for i := 0; i < len(logs); i++ {
			fmt.Printf("%v\n", string(readLogs[i].Value))
			require.Equal(t, logs[i], readLogs[i])
		}
	})

}

// numRW:500000
// numPart:1 write:1.134225253s read:495.503395ms
// numPart:3 write:623.370618ms read:429.974872ms
// numPart:10 write:605.139844ms read:408.844017ms
// numRW:1000000
// numPart:1 write:2.253321174s read:840.806123ms
// numPart:3 write:1.391289828s read:742.954468ms
// numPart:10 write:1.27199545s read:752.774177ms
// numRW:2000000
// numPart:1 write:4.554865317s read:1.834398737s
// numPart:3 write:3.521361612s read:1.667203604s
// numPart:10 write:2.654927796s read:1.481536838s

// Please set the go-test timeout long enough before run this test!
func TestRWLarge(t *testing.T) {
	opts := valueLogOptions{
		dirPath:     "/tmp/lotusdb",
		segmentSize: wal.GB,
		blockCache:  32 * wal.KB * 10,
		numPartions: 1,
	}
	numRWList := []int{500000, 1000000, 2000000}
	numPartList := []int{1, 3, 10}

	for _, numRW := range numRWList {
		fmt.Printf("numRW:%d\n", numRW)
		for _, numPart := range numPartList {
			fmt.Printf("numPart:%d ", numPart)
			err := RWBatch(opts, numRW, numPart)
			require.Nil(t, err)
		}
	}

}

func RWBatch(opts valueLogOptions, numRW int, numPart int) error {
	opts.numPartions = uint32(numPart)
	vlog, err := openValueLog(opts)
	if err != nil {
		return err
	}

	defer func() {
		for i := 0; i < int(vlog.numPartions); i++ {
			vlog.wals[i].Delete()
		}
	}()

	val := strings.Repeat("v", 512)
	logs := []*LogRecord{}
	for i := 0; i < numRW; i++ {
		log := &LogRecord{Key: []byte(fmt.Sprintf("%d", i)), Value: []byte(val)}
		logs = append(logs, log)
	}
	start := time.Now()
	pos, err := vlog.writeBatch(logs)
	if err != nil {
		return err
	}
	end := time.Since(start)
	fmt.Printf("write:%v ", end)

	vlog.sync()

	start = time.Now()
	_, err = vlog.readBatch(pos)
	if err != nil {
		return err
	}
	end = time.Since(start)
	fmt.Printf("read:%v\n", end)

	return nil
}
