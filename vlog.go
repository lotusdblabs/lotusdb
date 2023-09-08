package lotusdb

import (
	"context"
	"fmt"

	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
)

const (
	valueLogFileExt     = ".VLOG.%d"
	tempValueLogFileExt = ".VLOG.%d.temp"
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

	// value log are partitioned to several parts for concurrent writing and reading
	partitionNum uint32

	// hash function for sharding
	hashKeyFunction func([]byte) uint64

	// writing validEntries to disk after reading the specified number of entries.
	compactBatchCount int
}

// open wal files for value log, it will open several wal files for concurrent writing and reading
// the number of wal files is specified by the partitionNum
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

// read the value log record from the specified position
func (vlog *valueLog) read(pos *KeyPosition) (*ValueLogRecord, error) {
	buf, err := vlog.walFiles[pos.partition].Read(pos.position)
	if err != nil {
		return nil, err
	}
	log := decodeValueLogRecord(buf)
	return log, nil
}

// write the value log record to the value log, it will be separated to several partitions
// and write to the corresponding partition concurrently.
func (vlog *valueLog) writeBatch(records []*ValueLogRecord) ([]*KeyPosition, error) {
	// group the records by partition
	partitionRecords := make([][]*ValueLogRecord, vlog.options.partitionNum)
	for _, record := range records {
		p := vlog.getKeyPartition(record.key)
		partitionRecords[p] = append(partitionRecords[p], record)
	}

	// channel for receiving the positions of the records after writing to the value log
	posChan := make(chan []*KeyPosition, vlog.options.partitionNum)
	g, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		if len(partitionRecords[i]) == 0 {
			posChan <- []*KeyPosition{}
			continue
		}

		part := i
		g.Go(func() error {
			var positions []*KeyPosition
			writeIdx := 0
			for idx, record := range partitionRecords[part] {
				select {
				case <-ctx.Done():
					posChan <- nil
					return ctx.Err()
				default:
					err := vlog.walFiles[part].PendingWrites(encodeValueLogRecord(record))
					if err != nil {
						if err != wal.ErrPendingSizeTooLarge {
							posChan <- nil
							return err
						}
						pos, err := vlog.walFiles[part].WriteALL()
						if err != nil {
							posChan <- nil
							return err
						}
						for i, p := range pos {
							positions = append(positions, &KeyPosition{
								key:       partitionRecords[part][writeIdx+i].key,
								partition: uint32(part),
								position:  p,
							})
						}
						writeIdx = idx + 1
					}
				}
			}
			pos, err := vlog.walFiles[part].WriteALL()
			if err != nil {
				posChan <- nil
				return err
			}
			for i, p := range pos {
				positions = append(positions, &KeyPosition{
					key:       partitionRecords[part][writeIdx+i].key,
					partition: uint32(part),
					position:  p,
				})
			}
			posChan <- positions
			return nil
		})
	}

	// nwo we get the positions of the records, we can return them to the caller
	var keyPositions []*KeyPosition
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		pos := <-posChan
		keyPositions = append(keyPositions, pos...)
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return keyPositions, nil
}

// sync the value log to disk
func (vlog *valueLog) sync() error {
	for _, walFile := range vlog.walFiles {
		if err := walFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// close the value log
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
