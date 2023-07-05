package index

import (
	"os"
	"testing"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/stretchr/testify/assert"
)

func Benchmark_Put_1(b *testing.B) {
	benchmark_Put(1, b)
}
func Benchmark_Put_3(b *testing.B) {
	benchmark_Put(3, b)
}
func Benchmark_Put_10(b *testing.B) {
	benchmark_Put(10, b)
}

func benchmark_Put(n int, b *testing.B) {
	path, err := os.MkdirTemp("", "indexer")
	assert.Nil(b, err)
	opts := BPTreeOptions{DirPath: path, IndexType: BptreeBoltDB, ColumnFamilyName: "test", BatchSize: 100000, PartitionNum: n}
	tree, err := NewBPTree(opts)
	assert.Nil(b, err)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := tree.Put(util.RandomValue(10), util.RandomValue(10))
			assert.Nil(b, err)
		}
	})
}

func Benchmark_PutBatch_1(b *testing.B) {
	benchmark_PutBatch(1, b)
}

func Benchmark_PutBatch_3(b *testing.B) {
	benchmark_PutBatch(3, b)
}

func Benchmark_PutBatch_10(b *testing.B) {
	benchmark_PutBatch(10, b)
}

func benchmark_PutBatch(n int, b *testing.B) {
	path, _ := os.MkdirTemp("", "indexer")
	opts := BPTreeOptions{DirPath: path, IndexType: BptreeBoltDB, ColumnFamilyName: "test_p", BatchSize: 100000, PartitionNum: n}
	tree, _ := NewBPTree(opts)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	getkv := func(nums int) []*IndexerNode {
		var nodes []*IndexerNode
		for i := nums; i <= nums*2; i++ {
			node := &IndexerNode{Key: util.RandomValue(10)}
			meta := new(IndexerMeta)
			if i%2 == 0 {
				meta.Value = GetValue16B()
			} else {
				meta.Fid = uint32(i)
				meta.Offset = int64(i * 101)
			}
			node.Meta = meta
			nodes = append(nodes, node)
		}
		return nodes
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			testData := getkv(200000)
			_, err := tree.PutBatch(testData)
			assert.Nil(b, err)
		}
	})
}

func Benchmark_Delete_1(b *testing.B) {
	benchmark_Delete(1, b)
}

func Benchmark_Delete_3(b *testing.B) {
	benchmark_Delete(3, b)
}

func Benchmark_Delete_10(b *testing.B) {
	benchmark_Delete(10, b)
}

func benchmark_Delete(n int, b *testing.B) {
	path, _ := os.MkdirTemp("", "indexer")
	opts := BPTreeOptions{DirPath: path, IndexType: BptreeBoltDB, ColumnFamilyName: "test_p", BatchSize: 100000, PartitionNum: n}
	tree, err := NewBPTree(opts)
	assert.Nil(b, err)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	getkv := func(nums int) ([]*IndexerNode, [][]byte) {
		var nodes []*IndexerNode
		var deletedKeys [][]byte
		for i := nums; i <= nums*2; i++ {
			k := util.RandomValue(10)
			deletedKeys = append(deletedKeys, k)
			node := &IndexerNode{Key: k}
			meta := new(IndexerMeta)
			if i%2 == 0 {
				meta.Value = GetValue16B()
			} else {
				meta.Fid = uint32(i)
				meta.Offset = int64(i * 101)
			}
			node.Meta = meta
			nodes = append(nodes, node)
		}
		return nodes, deletedKeys
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			nodes, deletedKeys := getkv(200000)
			_, err = tree.PutBatch(nodes)
			assert.Nil(b, err)
			for i := range deletedKeys {
				err = tree.Delete(deletedKeys[i])
				assert.Nil(b, err)
			}
		}
	})
}

func Benchmark_DeleteBatch_1(b *testing.B) {
	benchmark_DeleteBatch(1, b)
}

func Benchmark_DeleteBatch_3(b *testing.B) {
	benchmark_DeleteBatch(3, b)
}

func Benchmark_DeleteBatch_10(b *testing.B) {
	benchmark_DeleteBatch(10, b)
}

func benchmark_DeleteBatch(n int, b *testing.B) {
	path, err := os.MkdirTemp("", "indexer")
	assert.Nil(b, err)
	opts := BPTreeOptions{DirPath: path, IndexType: BptreeBoltDB, ColumnFamilyName: "test", PartitionNum: n}
	tree, err := NewBPTree(opts)
	assert.Nil(b, err)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	getkv := func(nums int) ([]*IndexerNode, [][]byte) {
		var nodes []*IndexerNode
		var deletedKeys [][]byte
		for i := nums; i <= nums*2; i++ {
			k := util.RandomValue(10)
			deletedKeys = append(deletedKeys, k)
			node := &IndexerNode{Key: k}
			meta := new(IndexerMeta)
			if i%2 == 0 {
				meta.Value = GetValue16B()
			} else {
				meta.Fid = uint32(i)
				meta.Offset = int64(i * 101)
			}
			node.Meta = meta
			nodes = append(nodes, node)
		}
		return nodes, deletedKeys
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			nodes, deletedKeys := getkv(200000)
			_, err = tree.PutBatch(nodes)
			assert.Nil(b, err)
			err = tree.DeleteBatch(deletedKeys)
			assert.Nil(b, err)
		}
	})
}
