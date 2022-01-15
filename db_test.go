package lotusdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	opts := DefaultOptions("/tmp" + separator + "lotusdb")
	t.Run("default", func(t *testing.T) {
		defer os.RemoveAll(opts.DBPath)
		_, err := Open(opts)
		assert.Nil(t, err)
	})

	t.Run("spec-dir", func(t *testing.T) {
		opts.CfOpts.IndexerDir = "/tmp/new-one"
		opts.CfOpts.ValueLogDir = "/tmp/new-one"
		defer os.RemoveAll("/tmp/new-one")
		_, err := Open(opts)
		assert.Nil(t, err)
	})
}

func TestLotusDB_Put(t *testing.T) {
	type fields struct {
		db *LotusDB
	}
	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"normal", fields{db: nil}, args{key: nil, value: nil}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fields.db.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// GetKey length: 32 Bytes
func GetKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
}

func GetValue128() []byte {
	var str bytes.Buffer
	for i := 0; i < 128; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return []byte(str.String())
}

func GetValue() []byte {
	var str bytes.Buffer
	for i := 0; i < 4096; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return []byte(str.String())
}
