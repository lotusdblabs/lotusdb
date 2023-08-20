package lotusdb

import (
	"fmt"
	"io"
	"reflect"
	"time"
	"unsafe"

	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
)

const (
	valueLogFileExt = ".VLOG.%v.%d"
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

	// index corresponding to vlog
	index Index

	// The maximum amount of storage (GB) that vlog is allowed to read into memory when compacting
	maxMemoryCompact uint64

	// check size of valieEntries after writing the specified number of entries.
	numEntriesToCheck int

	// the db vlog belongs to
	db *DB
}

func openValueLog(options valueLogOptions) (*valueLog, error) {
	var walFiles []*wal.WAL

	for i := 0; i < int(options.partitionNum); i++ {
		vLogWal, err := wal.Open(wal.Options{
			DirPath:        options.dirPath,
			SegmentSize:    options.segmentSize,
			SegmentFileExt: fmt.Sprintf(valueLogFileExt, time.Now().Format("02-03-04-05-2006"), i),
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

func (vlog *valueLog) compaction() error {
	vlog.options.db.flushLock.Lock()
	var maxMemory uint64 = vlog.options.maxMemoryCompact / uint64(vlog.options.partitionNum)
	groups := make([]errgroup.Group, vlog.options.partitionNum)

	for i := 0; i < int(vlog.options.partitionNum); i++ {
		part := i
		groups[part].Go(func() error {
			newVLogWal, err := wal.Open(wal.Options{
				DirPath:        vlog.options.dirPath,
				SegmentSize:    vlog.options.segmentSize,
				SegmentFileExt: fmt.Sprintf(valueLogFileExt, time.Now().Format("02-03-04-05-2006"), part),
				BlockCache:     vlog.options.blockCache,
				Sync:           false, // we will sync manually
				BytesPerSync:   0,     // the same as Sync
			})
			if err != nil {
				newVLogWal.Delete()
				return err
			}

			validEntries := []*ValueLogRecord{}
			reader := vlog.walFiles[part].NewReader()
			var count = 0
			for {
				count++
				content, chunkPos, err := reader.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					newVLogWal.Delete()
					return err
				}
				record := decodeValueLogRecord(content)
				keyPos, err := vlog.options.index.Get(record.key)
				if err != nil {
					newVLogWal.Delete()
					return err
				}

				if keyPos == nil {
					continue
				}
				if keyPos.partition == uint32(part) && reflect.DeepEqual(keyPos.position, chunkPos) {
					validEntries = append(validEntries, record)
				}

				// if validEntries occupy too much memory, we need to write it to disk and clear memory
				if count%vlog.options.numEntriesToCheck == 0 {
					if unsafe.Sizeof(validEntries) > uintptr(maxMemory) {
						err := vlog.writeCompaction(newVLogWal, validEntries, part)
						if err != nil {
							newVLogWal.Delete()
							return err
						}
						validEntries = validEntries[:0]
					}
				}
			}

			err = vlog.writeCompaction(newVLogWal, validEntries, part)
			if err != nil {
				newVLogWal.Delete()
				return err
			}

			// replace the wal with the new one.
			vlog.walFiles[part].Delete()
			vlog.walFiles[part] = newVLogWal

			return nil
		})
	}

	for i := 0; i < int(vlog.options.partitionNum); i++ {
		if err := groups[i].Wait(); err != nil {
			vlog.options.db.flushLock.Unlock()
			return err
		}
	}

	vlog.options.db.flushLock.Unlock()
	return nil
}

func (vlog *valueLog) writeCompaction(newWal *wal.WAL, validEntries []*ValueLogRecord, part int) error {
	keyPos := []*KeyPosition{}
	for _, record := range validEntries {
		pos, err := newWal.Write(encodeValueLogRecord(record))
		if err != nil {
			return err
		}
		keyPos = append(keyPos, &KeyPosition{
			key:       record.key,
			partition: uint32(part),
			position:  pos},
		)
	}
	vlog.options.index.PutBatch(keyPos)
	return nil
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
