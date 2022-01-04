package lotusdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	options := DefaultOptions("/tmp/lotusdb")
	//options.CfOpts.WalMMap = true
	options.CfOpts.ValueThreshold = 100
	db, err := Open(options)
	assert.Nil(t, err)
	defer db.Close()

	now := time.Now()
	for i := 0; i < 600000; i++ {
		err := db.Put(GetKey(i), GetValue())
		assert.Nil(t, err)
	}
	t.Log("writing 50w records, time spent: ", time.Since(now).Milliseconds())
	time.Sleep(time.Minute)
}

func TestLotusDB_Get(t *testing.T) {
	options := DefaultOptions("/tmp/lotusdb")
	db, err := Open(options)
	assert.Nil(t, err)
	defer db.Close()

	v, err := db.Get(GetKey(398721))
	if err != nil {
		t.Log(err)
	}
	t.Log("val = ", string(v))
}

const alphabet = "abcdefghijklmnopqrstuvwxyz"

func init() {
	rand.Seed(time.Now().Unix())
}

func GetKey(n int) []byte {
	return []byte("test_key_" + fmt.Sprintf("%09d", n))
}

func GetValue() []byte {
	var str bytes.Buffer
	for i := 0; i < 12; i++ {
		str.WriteByte(alphabet[rand.Int()%26])
	}
	return []byte("test_val-" + strconv.FormatInt(time.Now().UnixNano(), 10) + str.String())
}
