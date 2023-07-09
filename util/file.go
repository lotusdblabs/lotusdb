package util

/*
about dir files func
*/

import (
	"os"
	"path/filepath"
)

// DirSize calculate path dir's files size
func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
