package lotusdb

import (
	"fmt"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
)

const (
	valueLogFileExt = ".VLOG"
)

// valueLog value log is named after the concept in Wisckey paper
// https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf
type valueLog struct {
	wals    []*wal.WAL
	options valueLogOptions
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

	// Value logs are partioned to serveral parts for concurrent writing and reading
	partitionNum uint32
}

type keyPos struct {
	key []byte
	pos *partPosition
}

func openValueLog(options valueLogOptions) (*valueLog, error) {
	vLogWals := []*wal.WAL{}

	for i := 0; i < int(options.partitionNum); i++ {
		vLogWal, err := wal.Open(wal.Options{
			DirPath:        options.dirPath,
			SegmentSize:    options.segmentSize,
			SegmentFileExt: fmt.Sprintf(".%d%s", i, valueLogFileExt),
			BlockCache:     options.blockCache,
			Sync:           false,
			BytesPerSync:   0,
		})
		if err != nil {
			return nil, err
		}
		vLogWals = append(vLogWals, vLogWal)
	}

	return &valueLog{wals: vLogWals, options: options}, nil
}

func (vlog *valueLog) writeLog(data []byte, part int) (*wal.ChunkPosition, error) {
	pos, err := vlog.wals[part].Write(data)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (vlog *valueLog) readLog(position *partPosition) ([]byte, error) {
	value, err := vlog.wals[position.partIndex].Read(position.walPosition)
	if err != nil {
		return nil, err
	}

	return value, nil
}

// write a log
func (vlog *valueLog) write(log *ValueLogRecord) (*keyPos, error) {
	logs := []*ValueLogRecord{}
	logs = append(logs, log)
	positions, err := vlog.writeBatch(logs)
	if err != nil {
		return nil, err
	}
	return positions[0], nil
}

// read a log
func (vlog *valueLog) read(vlogPos *partPosition) (*ValueLogRecord, error) {
	buf, err := vlog.readLog(vlogPos)
	if err != nil {
		return nil, err
	}
	log := decodeValueLogRecord(buf)
	return log, nil
}

// we must maintain correct order with respect to input while returning chunkPositions
func (vlog *valueLog) writeBatch(logs []*ValueLogRecord) ([]*keyPos, error) {
	// storage key and encoded log
	type keyBuf struct {
		key []byte
		buf []byte
	}

	// split logs into parts
	logParts := make([][]keyBuf, vlog.options.partitionNum)
	for _, log := range logs {
		partIdx := vlog.getIndex(log.key)
		buf := encodeValueLogRecord(log)
		logParts[partIdx] = append(logParts[partIdx], keyBuf{key: log.key, buf: buf})
	}

	// write parts concurrently
	posChan := make(chan []*keyPos, vlog.options.partitionNum)
	errG := make([]errgroup.Group, vlog.options.partitionNum)
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		if len(logParts[i]) == 0 {
			posChan <- []*keyPos{}
			continue
		}

		part := i
		errG[part].Go(func() error {
			pos := []*keyPos{}
			for _, s := range logParts[part] {
				p, err := vlog.writeLog(s.buf, part)
				if err != nil {
					posChan <- nil
					return err
				}
				pos = append(pos, &keyPos{key: s.key, pos: &partPosition{partIndex: uint32(part), walPosition: p}})
			}
			posChan <- pos
			return nil
		})
	}

	keyPositions := []*keyPos{}

	for i := 0; i < int(vlog.options.partitionNum); i++ {
		if err := errG[i].Wait(); err != nil {
			return nil, err
		}
		pos := <-posChan
		keyPositions = append(keyPositions, pos...)
	}

	return keyPositions, nil
}

func (vlog *valueLog) sync() error {
	f := func(part int) error {
		return vlog.wals[part].Sync()
	}
	return vlog.doConcurrent(f)
}

func (vlog *valueLog) close() error {
	f := func(part int) error {
		return vlog.wals[part].Close()
	}
	return vlog.doConcurrent(f)
}

func (vlog *valueLog) doConcurrent(f func(int) error) error {
	errG := make([]errgroup.Group, vlog.options.partitionNum)
	for i := 0; i < int(vlog.options.partitionNum); i++ {
		part := i
		errG[part].Go(func() error {
			return f(part)
		})
	}

	for i := 0; i < int(vlog.options.partitionNum); i++ {
		if err := errG[i].Wait(); err != nil {
			return err
		}
	}
	return nil
}

func (vlog *valueLog) getIndex(key []byte) int {
	return int(util.FnvNew32a(string(key)) % vlog.options.partitionNum)
}
