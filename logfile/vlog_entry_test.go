package logfile

import (
	"testing"
)

func TestEncodeVlogEntry(t *testing.T) {
	ve1 := &VlogEntry{Key: []byte("test-key"), Value: []byte("lotusdb")}
	e, n := EncodeVlogEntry(ve1)
	t.Log(e)
	t.Log(n)

	//b := []byte{2, 2, 97, 50}
	ve := DecodeVlogEntry(e)

	t.Log(string(ve.Key))
	t.Log(string(ve.Value))
}

func TestDecodeVlogEntry(t *testing.T) {
}
