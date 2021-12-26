package util

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestPathExist(t *testing.T) {
	path := "/tmp/path/lotusdb-1"
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll("/tmp/path")

	_, err = os.OpenFile("/tmp/path/lotusdb-file1", os.O_CREATE, 0644)
	assert.Nil(t, err)

	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"path exist", args{path: "/tmp/path/lotusdb-1"}, true,
		},
		{
			"path not exist", args{path: "/tmp/path/lotusdb-2"}, false,
		},
		{
			"file exist", args{path: "/tmp/path/lotusdb-file1"}, true,
		},
		{
			"file not exist", args{path: "/tmp/path/lotusdb-file2"}, false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PathExist(tt.args.path); got != tt.want {
				t.Errorf("PathExist() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCopyDir(t *testing.T) {
	path := "/tmp/path/lotusdb-1"
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll("/tmp/path")

	err = CopyDir(path, "/tmp/path/lotusdb-bak")
	assert.Nil(t, err)
}

func TestCopyFile(t *testing.T) {
	path := "/tmp/path/lotusdb-1"
	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll("/tmp/path")

	file := "/tmp/path/lotusdb-1/001.vlog"
	_, err = os.OpenFile(file, os.O_CREATE, 0644)
	assert.Nil(t, err)

	err = CopyFile(file, path+"/001.vlog-bak")
	assert.Nil(t, err)
}
