package lotusdb

import (
	"fmt"
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
	// entries, err := ioutil.ReadDir(opts.dirPath)
	// for _, ent := range entries {
	// 	fmt.Printf("%v\n", ent)
	// }

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
	kvMap := make(map[string]string)
	for _, log := range logs {
		kvMap[string(log.Key)] = string(log.Value)
	}
	keyPos := []*keyPos{}

	// test writing
	t.Run("test writing", func(t *testing.T) {
		keyPos, err = vlog.writeBatch(logs[:])
		require.Nil(t, err)
	})
	vlog.sync()

	// test reading
	t.Run("test reading", func(t *testing.T) {
		for i := 0; i < len(logs); i++ {
			log, err := vlog.read(keyPos[i].pos)
			require.Nil(t, err)
			fmt.Printf("%v\n", string(log.Value))
			// test whether keyPos[i].pos correspond to the real position of keyPos[i].key
			fmt.Printf("expected value:%v, real value:%v\n", kvMap[string(keyPos[i].key)], string(log.Value))
			require.Equal(t, kvMap[string(keyPos[i].key)], string(log.Value))
		}
	})

}

// numRW:500000
// numPart:1 write:4.6256513s
// numPart:3 write:2.511225s
// numPart:10 write:2.0412199s
// numPart:20 write:2.0883136s
// numRW:1000000
// numPart:1 write:9.4966978s
// numPart:3 write:4.6746816s
// numPart:10 write:4.0549516s
// numPart:20 write:4.1269718s
// numRW:2000000
// numPart:1 write:18.4623601s
// numPart:3 write:9.3436442s
// numPart:10 write:8.3442342s
// numPart:20 write:8.5486256s

// Please set the go-test timeout long enough before run this test!
func TestRWLarge(t *testing.T) {
	opts := valueLogOptions{
		dirPath:     "/tmp/lotusdb",
		segmentSize: wal.GB,
		blockCache:  32 * wal.KB * 10,
		numPartions: 1,
	}
	numRWList := []int{500000, 1000000, 2000000}
	numPartList := []int{1, 3, 10, 20}

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
	_, err = vlog.writeBatch(logs)
	if err != nil {
		return err
	}
	end := time.Since(start)
	fmt.Printf("write:%v\n", end)

	vlog.sync()

	return nil
}
