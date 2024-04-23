package main

import (
	"github.com/lotusdblabs/lotusdb/v2"
)

// this file shows how to use the batch operations of LotusDB
func main() {
	// specify the options
	options := lotusdb.DefaultOptions
	options.DirPath = "/tmp/lotusdb_batch"

	// open a database
	db, err := lotusdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// create a batch
	batch := db.NewBatch(lotusdb.DefaultBatchOptions)

	// set a key
	_ = batch.Put([]byte("name"), []byte("lotusdb"))

	// get a key
	val, _ := batch.Get([]byte("name"))
	println(string(val))

	// delete a key
	_ = batch.Delete([]byte("name"))

	// commit the batch
	_ = batch.Commit()

	// _ = batch.Put([]byte("name1"), []byte("lotusdb1")) // don't do this!!!
}
