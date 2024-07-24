package lotusdb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
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
	walFiles         []*wal.WAL
	dpTables         []*deprecatedtable
	deprecatedNumber uint32
	compactChan chan deprecatedState
	options          valueLogOptions
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

	// deprecatedtable capacity, for every wal.
	deprecatedtableCapacity uint32

	// deprecatedtable recommend compaction size
	deprecatedtableLowerThreshold uint32

	// deprecatedtable force compaction size
	deprecatedtableUpperThreshold uint32
}

// open wal files for value log, it will open several wal files for concurrent writing and reading
// the number of wal files is specified by the partitionNum.
// init deprecatedtable for every wal, TODO: we should build dpTable aftering compacting vlog.
func openValueLog(options valueLogOptions) (*valueLog, error) {
	var walFiles []*wal.WAL
	var dpTables []*deprecatedtable
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
		// TODO: add dpTable
		dpTableOption := deprecatedtableOptions{
			options.deprecatedtableCapacity,
			options.deprecatedtableLowerThreshold,
			options.deprecatedtableUpperThreshold,
		}

		dpTable := newDeprecatedTable(i, dpTableOption)
		dpTables = append(dpTables, dpTable)

	}

	return &valueLog{walFiles: walFiles, dpTables: dpTables, options: options}, nil
}

// read the value log record from the specified position.
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
			continue
		}

		part := i
		g.Go(func() error {
			var err error
			defer func() {
				if err != nil {
					vlog.walFiles[part].ClearPendingWrites()
				}
			}()

			var keyPositions []*KeyPosition
			writeIdx := 0
			for _, record := range partitionRecords[part] {
				select {
				case <-ctx.Done():
					err = ctx.Err()
					return err
				default:
					vlog.walFiles[part].PendingWrites(encodeValueLogRecord(record))
				}
			}
			positions, err := vlog.walFiles[part].WriteAll()
			if err != nil {
				return err
			}
			for i, pos := range positions {
				keyPositions = append(keyPositions, &KeyPosition{
					key:       partitionRecords[part][writeIdx+i].key,
					partition: uint32(part),
					// TODO: add uid support
					uid:      partitionRecords[part][writeIdx+i].uid,
					position: pos,
				})
			}
			posChan <- keyPositions
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	close(posChan)

	// nwo we get the positions of the records, we can return them to the caller
	var keyPositions []*KeyPosition
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		pos := <-posChan
		keyPositions = append(keyPositions, pos...)
	}

	return keyPositions, nil
}

// sync the value log to disk.
func (vlog *valueLog) sync() error {
	for _, walFile := range vlog.walFiles {
		if err := walFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// close the value log.
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

//TODO: we add middle layer of DeprecatedTable for interacting with autoCompact func.
func (vlog *valueLog) setDeprecated(partition uint32, id uuid.UUID) {
	vlog.dpTables[partition].addEntry(id)
	vlog.deprecatedNumber++
}

func (vlog *valueLog) isDeprecated(partition int, id uuid.UUID) bool {
	return vlog.dpTables[partition].existEntry(id)
}

func (vlog *valueLog) cleanDeprecatedTable() {
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		vlog.dpTables[i].clean()
	}
	vlog.deprecatedNumber = 0
}

