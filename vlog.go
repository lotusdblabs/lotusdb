package lotusdb

import (
	"errors"
	"fmt"

	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/rosedblabs/wal"
)

const (
	valueLogFileExt = ".VLOG"
)

var ErrWriteVlog = errors.New("Write vLog error")

// valueLog value log is named after the concept in Wisckey paper
// https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf
type valueLog struct {
	wals        []*wal.WAL
	numPartions uint32
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
	numPartions uint32
}

// an auxiliary struct for returning chunkPositions in correct order
// while writing or reading batch
type seq struct {
	buf    []byte
	key    []byte
	walpos *wal.ChunkPosition
	part   int
}

type keyPos struct {
	key []byte
	pos *partPosition
}

// // record accurate position in multiple vlogs
// type VlogPosition struct {
// 	part   int
// 	walPos *wal.ChunkPosition
// }

func openValueLog(options valueLogOptions) (*valueLog, error) {
	vLogWals := []*wal.WAL{}

	for i := 0; i < int(options.numPartions); i++ {
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

	return &valueLog{wals: vLogWals, numPartions: options.numPartions}, nil
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
func (vlog *valueLog) write(log *LogRecord) (*keyPos, error) {
	logs := []*LogRecord{}
	logs = append(logs, log)
	positions, err := vlog.writeBatch(logs)
	if err != nil {
		return nil, err
	}
	return positions[0], nil
}

// read a log
func (vlog *valueLog) read(vlogPos *partPosition) (*LogRecord, error) {
	buf, err := vlog.readLog(vlogPos)
	if err != nil {
		return nil, err
	}
	log := decodeLogRecord(buf)
	return log, nil
}

// we must maintain correct order with respect to input while returning chunkPositions
func (vlog *valueLog) writeBatch(logs []*LogRecord) ([]*keyPos, error) {
	// split logs into parts
	logParts := make([][]seq, vlog.numPartions)
	for _, log := range logs {
		partIdx := vlog.getIndex(log.Key)
		buf := encodeLogRecord(log)
		logParts[partIdx] = append(logParts[partIdx], seq{buf: buf, key: log.Key, part: partIdx})
	}

	// write parts concurrently
	errChan := make(chan error, vlog.numPartions)
	posChan := make(chan []*keyPos, vlog.numPartions)
	for i := 0; i < int(vlog.numPartions); i++ {
		if len(logParts[i]) == 0 {
			errChan <- nil
			posChan <- []*keyPos{}
			continue
		}
		go func(part int) {
			pos := []*keyPos{}
			for _, s := range logParts[part] {
				p, err := vlog.writeLog(s.buf, part)
				if err != nil {
					errChan <- err
					posChan <- nil
					return
				}
				pos = append(pos, &keyPos{key: s.key, pos: &partPosition{partIndex: uint32(part), walPosition: p}})
			}
			errChan <- nil
			posChan <- pos
		}(i)
	}

	flag := false
	keyPositions := []*keyPos{}
	for i := 0; i < int(vlog.numPartions); i++ {
		e := <-errChan
		pos := <-posChan
		if e != nil {
			flag = true
		}
		keyPositions = append(keyPositions, pos...)
	}

	if flag {
		return nil, ErrWriteVlog
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
	errChan := make(chan error, vlog.numPartions)
	for i := 0; i < int(vlog.numPartions); i++ {
		go func(part int) {
			err := f(part)
			errChan <- err
		}(i)
	}

	flag := false
	for i := 0; i < int(vlog.numPartions); i++ {
		e := <-errChan
		if e != nil {
			flag = true
		}
	}

	if flag {
		return ErrWriteVlog
	}
	return nil
}

func (vlog *valueLog) getIndex(key []byte) int {
	return int(util.FnvNew32a(string(key)) % vlog.numPartions)
}
