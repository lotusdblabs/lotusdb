package lotusdb

import "testing"

func TestOpenDB(t *testing.T) {
	options := DefaultOptions
	options.DirPath = "/tmp/lotusdb"
	db, err := Open(options)
	t.Log(db, err)

	err = db.Put([]byte("name"), []byte("lotusdb"), nil)
	t.Log(err)

	val, err := db.Get([]byte("name"))
	t.Log(string(val), err)
}
