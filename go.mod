module github.com/lotusdblabs/lotusdb/v2

go 1.19

require (
	github.com/bwmarrin/snowflake v0.3.0
	github.com/dgraph-io/badger/v4 v4.1.0
	github.com/rosedblabs/wal v1.3.6-0.20230921133940-0753d5ac24c4
	github.com/stretchr/testify v1.8.4
)

require golang.org/x/sys v0.10.0 // indirect

require (
	github.com/kr/text v0.2.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
)

require (
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/dgraph-io/badger/v4 v4.2.0
	github.com/gofrs/flock v0.8.1
	github.com/rosedblabs/diskhash v0.0.0-20230910084041-289755737e2a
	github.com/rosedblabs/wal v1.3.6
	github.com/stretchr/testify v1.8.4
	go.etcd.io/bbolt v1.3.8
	golang.org/x/sync v0.5.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.2.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/klauspost/compress v1.17.4 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rosedblabs/diskhash v0.0.0-20230825144203-2d051ecdb9a5
	go.etcd.io/bbolt v1.3.7
	golang.org/x/net v0.12.0 // indirect
	golang.org/x/sync v0.5.0
)

replace go.etcd.io/bbolt v1.3.7 => github.com/yanxiaoqi932/bbolt v1.3.9-0.20240115123442-fcafe1b1680d
