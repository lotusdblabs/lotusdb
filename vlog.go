package lotusdb

import (
	"fmt"

	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
)

const (
	valueLogFileExt = ".VLOG.%d"
)

// valueLog value log is named after the concept in Wisckey paper
// https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf
type valueLog struct {
	walFiles []*wal.WAL
	options  valueLogOptions
}

type valueLogOptions struct {
	// dirPath specifies the directory path where the WAL segment files will be stored.
	dirPath string

	// segmentSize specifies the maximum size of each segment file in bytes.
	segmentSize int64

	// blockCache specifies the size of the block cache in number of bytes.
	// A block cache is used to store recently accessed data blocks, improving read performance.
	// If BlockCache is set to 0, no block cache will be used.
	blockCache uint32

	// value log are partioned to serveral parts for concurrent writing and reading
	partitionNum uint32

	// hash function for sharding
	hashKeyFunction func([]byte) uint64
}

func openValueLog(options valueLogOptions) (*valueLog, error) {
	var walFiles []*wal.WAL

	for i := 0; i < int(options.partitionNum); i++ {
		vLogWal, err := wal.Open(wal.Options{
			DirPath:        options.dirPath,
			SegmentSize:    options.segmentSize,
			SegmentFileExt: fmt.Sprintf(valueLogFileExt, i),
			BlockCache:     options.blockCache,
			Sync:           false, // we will sync manually
			BytesPerSync:   0,     // the same as Sync
		})
		if err != nil {
			return nil, err
		}
		walFiles = append(walFiles, vLogWal)
	}

	return &valueLog{walFiles: walFiles, options: options}, nil
}

func (vlog *valueLog) read(pos *KeyPosition) (*ValueLogRecord, error) {
	buf, err := vlog.walFiles[pos.partition].Read(pos.position)
	if err != nil {
		return nil, err
	}
	log := decodeValueLogRecord(buf)
	return log, nil
}

func (vlog *valueLog) writeBatch(records []*ValueLogRecord) ([]*KeyPosition, error) {
	partitionRecords := make([][]*ValueLogRecord, vlog.options.partitionNum)
	for _, record := range records {
		p := vlog.getKeyPartition(record.key)
		partitionRecords[p] = append(partitionRecords[p], record)
	}

	posChan := make(chan []*KeyPosition, vlog.options.partitionNum)
	groups := make([]errgroup.Group, vlog.options.partitionNum)
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		if len(partitionRecords[i]) == 0 {
			posChan <- []*KeyPosition{}
			continue
		}

		part := i
		groups[i].Go(func() error {
			var positions []*KeyPosition
			for _, record := range partitionRecords[part] {
				pos, err := vlog.walFiles[part].Write(encodeValueLogRecord(record))
				if err != nil {
					posChan <- nil
					return err
				}
				positions = append(positions, &KeyPosition{
					key:       record.key,
					partition: uint32(part),
					position:  pos},
				)
			}
			posChan <- positions
			return nil
		})
	}

	var keyPositions []*KeyPosition

	for i := 0; i < int(vlog.options.partitionNum); i++ {
		if err := groups[i].Wait(); err != nil {
			return nil, err
		}
		pos := <-posChan
		keyPositions = append(keyPositions, pos...)
	}

	return keyPositions, nil
}

func (vlog *valueLog) sync() error {
	for _, walFile := range vlog.walFiles {
		if err := walFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) close() error {
	for _, walFile := range vlog.walFiles {
		if err := walFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) getKeyPartition(key []byte) int {
	return int(vlog.options.hashKeyFunction(key) % uint64(vlog.options.partitionNum))
}
