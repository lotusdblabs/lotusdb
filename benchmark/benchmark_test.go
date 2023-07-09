package util

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	errGroup "golang.org/x/sync/errgroup"

	"github.com/lotusdblabs/lotusdb/v2"
	utils "github.com/lotusdblabs/lotusdb/v2/util"
)

var defaultDB *lotusdb.DB
var defalutPath string = "/tmp/lotusdbv2"

func openDB() (func(), func()) {
	options := lotusdb.DefaultOptions
	options.DirPath = defalutPath

	var err error
	defaultDB, err = lotusdb.Open(options)
	if err != nil {
		panic(err)
	}

	return func() {
			_ = defaultDB.Close()
		}, func() {
			_ = os.RemoveAll(options.DirPath)
		}
}

var defaultTestScale = TestScale{
	TestCaseName: "default-lotus-db",
	KeysNum:      utils.DefaultKeyNum, // use default key numbers
	MinKeySize:   16,
	MaxKeySize:   64,
	MinValueSize: 128,
	MaxValueSize: 512,
	Concurrency:  1,
	Path:         defalutPath,
	Compact:      true,
	Collect:      true,
}

// TestScale testing size
type TestScale struct {
	TestCaseName string // recommend db engine name

	KeysNum int // numbers of key

	// calculate random size with min and max, key and value
	MinKeySize   int
	MaxKeySize   int
	MinValueSize int
	MaxValueSize int

	Concurrency int    // concurrency run numbers, goroutines in go.
	Path        string // database path
	Compact     bool   // TODO is benchmark compaction. write keys twice and run compaction after
	Collect     bool   // TODO is benchmark collection. write some keys and delete keys, then use collection.
}

func (ts TestScale) String() {
	display := fmt.Sprintf("TestDB: %s\n"+
		"Numbers of Key: %d\n"+
		"KeySize(min-max): [ %d-%d ]\n"+
		"ValueSize(min-max): [ %d-%d ]\n"+
		"Concurrency: %d\n"+
		"DB-Path: %s\n"+
		"Is test Compaction: %t\n"+
		"Is test Collection: %t\n",
		ts.TestCaseName,
		ts.KeysNum,
		ts.MinKeySize, ts.MaxKeySize,
		ts.MinValueSize, ts.MaxValueSize,
		ts.Concurrency,
		ts.Path,
		ts.Compact, ts.Collect,
	)
	fmt.Println(display)
}

func TestTestScale(t *testing.T) {
	defaultTestScale.String()
}

// BenchmarkStore interface for kv store
type BenchmarkStore interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Exist(key []byte) (bool, error)
	Delete(key []byte) error
	Close() error
	// TODO declare compaction and collection
}

// forceGC simulate gc process
func forceGC() {
	runtime.GC()
	time.Sleep(time.Millisecond * 500)
}

func endLine(benchStage string) {
	fmt.Printf(" ===================================== current %s end =================================== \n", benchStage)
}

func concurrentBatch(keys [][]byte, concurrency int, cb func(gid int, batch [][]byte) error) error {
	eg := &errGroup.Group{}
	batchSize := len(keys) / concurrency
	for i := 0; i < concurrency; i++ {
		batchStart := i * batchSize
		batchEnd := (i + 1) * batchSize
		if batchEnd > len(keys) {
			batchEnd = len(keys)
		}
		// groutine seq
		gid := i
		// some batch keys
		batch := keys[batchStart:batchEnd]
		eg.Go(func() error {
			return cb(gid, batch)
		})
	}
	return eg.Wait()
}

func _benchmarkPut(rnd *rand.Rand, testScale TestScale, db BenchmarkStore, keys [][]byte) error {
	valSrc := make([]byte, testScale.MaxValueSize)
	if _, err := rand.Read(valSrc); err != nil {
		return err
	}

	err := concurrentBatch(keys, testScale.Concurrency, func(gid int, batch [][]byte) error {
		for _, k := range batch {
			if err := db.Put(k, utils.RandValue(rnd, valSrc, testScale.MinValueSize, testScale.MaxValueSize)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func benchmarkPut(b *testing.B, testScale TestScale, db BenchmarkStore, keys [][]byte) {
	b.ResetTimer()
	b.ReportAllocs()
	rnd := rand.New(rand.NewSource(int64(rand.Uint64())))
	for i := 0; i < b.N; i++ {
		err := _benchmarkPut(rnd, testScale, db, keys)
		assert.Nil(b, err)
	}
	endLine("benchmarkPut")
}

func _benchmarkGet(testScale TestScale, db BenchmarkStore, keys [][]byte) error {

	err := concurrentBatch(keys, testScale.Concurrency, func(gid int, batch [][]byte) error {
		for _, k := range batch {
			v, err := db.Get(k)
			if err != nil {
				return err
			}
			if v == nil {
				return errors.New("key doesn't exist")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func benchmarkGet(b *testing.B, testScale TestScale, db BenchmarkStore, keys [][]byte) {

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := _benchmarkGet(testScale, db, keys)
		assert.Nil(b, err)
	}
	endLine("bencharkGet")
}

func BenchmarkDB(b *testing.B) {
	closeDB1, _ := openDB()

	defaultTestScale.String()

	keys := utils.GenerateKeys(defaultTestScale.KeysNum, defaultTestScale.MinKeySize, defaultTestScale.MaxKeySize)
	endLine("GenerateKeys")

	var totalElapsed float64

	// Put.
	forceGC()
	start := time.Now()
	benchmarkPut(b, defaultTestScale, defaultDB, keys)
	elapsed := time.Since(start).Seconds()
	totalElapsed += elapsed

	fmt.Printf("put: %.3fs\t%d ops/s\n", elapsed, int(float64(defaultTestScale.KeysNum)/elapsed))

	// Reopen DB.
	closeDB1()
	closeDB2, clearFile := openDB()
	defer clearFile()
	defer closeDB2()

	utils.Shuffle(keys)

	// Get.
	forceGC()
	start = time.Now()
	benchmarkGet(b, defaultTestScale, defaultDB, keys)
	elapsed = time.Since(start).Seconds()
	totalElapsed += elapsed
	fmt.Printf("get: %.3fs\t%d ops/s\n", elapsed, int(float64(defaultTestScale.KeysNum)/elapsed))

	// Total stats.
	fmt.Printf("\nput + get: %.3fs\n", totalElapsed)

	sz, err := utils.DirSize(defaultTestScale.Path)
	assert.Nil(b, err)
	fmt.Printf("file size: %s\n", utils.ByteSize(sz))
}
