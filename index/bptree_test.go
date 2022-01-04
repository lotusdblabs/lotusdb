package index

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBptreeBolt(t *testing.T) {
	opts := &BPTreeOptions{
		IndexType:        BptreeBoltDB,
		BucketName:       []byte("cf_default"),
		ColumnFamilyName: "cf_default",
		DirPath:          "/tmp/lotusdb/cf_default",
	}
	bpTree, err := BptreeBolt(opts)
	assert.Nil(t, err)
	assert.NotNil(t, bpTree)
}

func TestBPTree_PutBatch(t *testing.T) {
	opts := &BPTreeOptions{
		IndexType:        BptreeBoltDB,
		BucketName:       []byte("cf_default"),
		ColumnFamilyName: "cf_default",
		DirPath:          "/tmp/lotusdb",
	}
	bpTree, err := BptreeBolt(opts)
	assert.Nil(t, err)

	data := []*IndexerNode{
		{
			Key: []byte("11"),
			Meta: &IndexerMeta{
				Value: []byte("aa"),
			},
		},
		{
			Key: []byte("22"),
			Meta: &IndexerMeta{
				Value: []byte("bb"),
			},
		},
	}

	_, err = bpTree.PutBatch(data)
	t.Log(err)
}
