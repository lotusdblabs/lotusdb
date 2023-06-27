module github.com/flower-corp/lotusdb

go 1.18

require (
	github.com/hashicorp/golang-lru/v2 v2.0.4
	github.com/peterh/liner v1.2.1
	github.com/stretchr/testify v1.7.0
	go.etcd.io/bbolt v1.3.6
	golang.org/x/sys v0.0.0-20211210111614-af8b64212486
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/mattn/go-runewidth v0.0.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

replace go.etcd.io/bbolt => github.com/flower-corp/bbolt v1.3.7-0.20220315040627-32fed02add8f
