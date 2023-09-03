## What is LotusDB

LotusDB is the most advanced key-value store written in Go, extremely fast, compatible with LSM tree and B+ tree, and optimization of badger and bbolt.

Key features:

* **Combine the advantages of LSM and B+ tree**
* **Fast read/write performance**
* **Much lower read and space amplification than typical LSM**

## Design Overview

![](https://github.com/lotusdblabs/lotusdb/blob/main/resource/img/design-overview.png)

## Use Example
```
package main

import "github.com/lotusdblabs/lotusdb/v2"

func main() {
	// Set Options
	options := lotusdb.DefaultOptions
	options.DirPath = "/tmp/lotusdb_basic"

	// Open LotusDB
	db, err := lotusdb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// Put Key-Value
	key := []byte("KV store engine")
	value := []byte("LotusDB")
	putOptions := &lotusdb.WriteOptions{
		Sync:       true,
		DisableWal: false,
	}
	err = db.Put(key, value, putOptions)
	if err != nil {
		panic(err)
	}

	// Read Key-Value
	value, err = db.Get(key)
	if err != nil {
		panic(err)
	}
	println(string(value))

	// Delete Key-Value
	err = db.Delete(key, putOptions)
	if err != nil {
		panic(err)
	}

	// Start Compaction of Value Log
	err = db.Compact()
	if err != nil {
		panic(err)
	}
}

```

## Community

Welcome to join the [Slack channel](https://join.slack.com/t/rosedblabs/shared_invite/zt-19oj8ecqb-V02ycMV0BH1~Tn6tfeTz6A) and  [Discussions](https://github.com/lotusdblabs/lotusdb/discussions) to connect with LotusDB team members and other users.

If you are a Chinese user, you are also welcome to join our WeChat group, scan the QR code and you will be invited:

| <img src="https://i.loli.net/2021/05/06/tGTH7SXg8w95slA.jpg" width="200px" align="left"/> |
| ------------------------------------------------------------ |
