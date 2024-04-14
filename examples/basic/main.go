package main

import (
	"github.com/lotusdblabs/lotusdb/v2"
)

// this file shows how to use the basic operations of LotusDB
func main() {
	// specify the options
	options := lotusdb.DefaultOptions
	options.DirPath = "/tmp/lotusdb_basic"

	// open a database
	db, err := lotusdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// put a key
	err = db.Put([]byte("name"), []byte("lotusdb"))
	if err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	println(string(val))

	// delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
