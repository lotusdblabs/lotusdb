package main

import (
	"os"

	"github.com/flower-corp/lotusdb"
)

func main() {
	// open a db with default options.
	path, _ := os.MkdirTemp("", "lotusdb")
	// you must specify a db path.
	opts := lotusdb.DefaultOptions(path)
	db, err := lotusdb.Open(opts)
	defer func() {
		_ = db.Close()
		_ = os.RemoveAll(path)
	}()

	if err != nil {
		panic(err)
	}
}
