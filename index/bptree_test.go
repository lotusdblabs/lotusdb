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
