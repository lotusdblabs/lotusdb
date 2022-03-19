package main

import (
	"github.com/flower-corp/lotusdb"
	"io/ioutil"
)

func main() {
	// open a db with default options.
	path, _ := ioutil.TempDir("", "lotusdb")
	// you must specify a db path.
	opts := lotusdb.DefaultOptions(path)
	db, err := lotusdb.Open(opts)
	defer func() {
		_ = db.Close()
	}()
	if err != nil {
		panic(err)
	}

	cfOpts := lotusdb.DefaultColumnFamilyOptions("a-new-cf")
	cfOpts.DirPath = "/tmp"
	cf, err := db.OpenColumnFamily(cfOpts)
	if err != nil {
		panic(err)
	}

	// the same with db.Put
	err = cf.Put([]byte("name"), []byte("LotusDB"))
	if err != nil {
		// ...
	}
}
