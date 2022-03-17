package main

import (
	"github.com/flower-corp/lotusdb"
	"io/ioutil"
	"time"
)

// basic operations for LotusDB:
// put
// put with options
// get
// delete
// delete with options
func main() {
	path, _ := ioutil.TempDir("", "lotusdb")
	opts := lotusdb.DefaultOptions(path)
	db, err := lotusdb.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 1.----put----
	key1 := []byte("name")
	err = db.Put(key1, []byte("lotusdb"))
	if err != nil {
		// ...
	}

	key2 := []byte("feature")
	// 2.----put with options----
	writeOpts := &lotusdb.WriteOptions{
		Sync:      true,
		ExpiredAt: time.Now().Add(time.Second * 100).Unix(),
	}
	err = db.PutWithOptions(key2, []byte("store data"), writeOpts)
	if err != nil {
		// ...
	}

	// 3.----get----
	val, err := db.Get(key1)
	if err != nil {
		// ...
	}
	if len(val) > 0 {
		// ...
	}

	// 4.----delete----
	err = db.Delete(key1)
	if err != nil {
		// ...
	}

	// 5.----delete with options----
	deleteOpts := &lotusdb.WriteOptions{
		Sync:       false,
		DisableWal: true,
	}
	err = db.DeleteWithOptions([]byte("dummy key"), deleteOpts)
	if err != nil {
		// ...
	}
}
