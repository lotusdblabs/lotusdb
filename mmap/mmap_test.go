package mmap

import (
	"os"
	"testing"

	"github.com/flower-corp/lotusdb/logger"
	"github.com/stretchr/testify/assert"
)

func TestMmap(t *testing.T) {
	fd, err := os.CreateTemp(t.TempDir(), "mmap.txt")
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, fd.Close())
	})

	type args struct {
		fd       *os.File
		writable bool
		size     int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"normal-size", args{fd: fd, writable: true, size: 100}, false,
		},
		{
			"big-size", args{fd: fd, writable: true, size: 128 << 20}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Mmap(tt.args.fd, tt.args.writable, tt.args.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mmap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Cleanup(func() {
				assert.NoError(t, Munmap(got))
			})
			if int64(len(got)) != tt.args.size {
				t.Errorf("Mmap() want buf size = %d, actual = %d", tt.args.size, len(got))
			}
		})
	}
}

func TestMunmap(t *testing.T) {
	fd, err := os.CreateTemp(t.TempDir(), "mmap.txt")
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, fd.Close())
	})

	buf, err := Mmap(fd, true, 100)
	assert.Nil(t, err)
	err = Munmap(buf)
	assert.Nil(t, err)
}

func TestMsync(t *testing.T) {
	fd, err := os.CreateTemp(t.TempDir(), "mmap.txt")
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, fd.Close())
	})

	buf, err := Mmap(fd, true, 128)
	assert.Nil(t, err)
	t.Cleanup(func() {
		assert.NoError(t, Munmap(buf))
	})

	err = Msync(buf)
	assert.Nil(t, err)
}

func TestMadvise(t *testing.T) {
	fd, err := os.CreateTemp(t.TempDir(), "mmap.txt")
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, fd.Close())
	})

	buf, err := Mmap(fd, true, 128)
	assert.Nil(t, err)
	t.Cleanup(func() {
		assert.NoError(t, Munmap(buf))
	})

	err = Madvise(buf, false)
	assert.Nil(t, err)
}
