module github.com/flowercorp/lotusdb

go 1.16

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/peterh/liner v1.2.1
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.7.0
	go.etcd.io/bbolt v1.3.6
	golang.org/x/sys v0.0.0-20211210111614-af8b64212486
)

replace go.etcd.io/bbolt => github.com/flower-corp/bbolt v1.3.6
