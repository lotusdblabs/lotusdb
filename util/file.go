package util

import (
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/rosedblabs/wal"
)

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

func ReadInfoFile(infoFile *wal.WAL) (int, int, error) {
	defer infoFile.Delete()
	reader := infoFile.NewReader()
	allEntries, invalidEntries := 0, 0
	for i := 0; i < 2; i++ {
		chunk, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				return 0, 0, nil
			}
			return 0, 0, err
		}
		if i == 0 {
			allEntries, err = strconv.Atoi(string(chunk))
			if err != nil {
				return 0, 0, err
			}
		} else {
			invalidEntries, err = strconv.Atoi(string(chunk))
			if err != nil {
				return 0, 0, err
			}
		}
	}
	return allEntries, invalidEntries, nil
}
