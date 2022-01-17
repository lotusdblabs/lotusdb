package main

import (
	"github.com/flower-corp/lotusdb"
	"io/ioutil"
)

func main() {
	path, _ := ioutil.TempDir("", "lotusdb")
	opts := lotusdb.DefaultOptions(path)
	db, err := lotusdb.Open(opts)
	if err != nil {
		panic(err)
	}

	k := []byte("1")
	err = db.Put(k, []byte("lotusdb"))
	if err != nil {
		panic(err)
	}
}
