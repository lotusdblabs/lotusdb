package lotusdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	options := DefaultOptions("/tmp/lotusdb")
	db, err := Open(options)
	assert.Nil(t, err)
	defer db.Close()

	now := time.Now()
	for i := 0; i < 500000; i++ {
		err := db.Put(GetKey(i), GetValue128())
		assert.Nil(t, err)
	}
	t.Log("writing 50w records, time spent: ", time.Since(now).Milliseconds())
}

func TestLotusDB_Put(t *testing.T) {
	options := DefaultOptions("/tmp/lotusdb")
	db, err := Open(options)
	assert.Nil(t, err)
	defer db.Close()

	err = db.Put([]byte("k"), []byte("lotusdb"))
	assert.Nil(t, err)
}

func TestLotusDB_Get(t *testing.T) {
	options := DefaultOptions("/tmp/lotusdb")
	db, err := Open(options)
	assert.Nil(t, err)
	defer db.Close()

	v, err := db.Get([]byte("k"))
	if err != nil {
		t.Log(err)
	}
	t.Log("val = ", string(v))
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
