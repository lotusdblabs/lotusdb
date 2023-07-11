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
	// DirPath specifies the directory path where the WAL segment files will be stored.
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	SegmentSize int64

	// BlockCache specifies the size of the block cache in number of bytes.
	// A block cache is used to store recently accessed data blocks, improving read performance.
	// If BlockCache is set to 0, no block cache will be used.
	BlockCache uint32

	// Value logs are partioned to serveral parts for concurrent writing and reading
	NumPartions uint32
}

// an auxiliary struct for returning chunkPositions in correct order
// while writing or reading batch
type seq struct {
	buf    []byte
	id     int
	walpos *wal.ChunkPosition
	part   int
}

// record accurate position in multiple vlogs
type VlogPosition struct {
	part   int
	walPos *wal.ChunkPosition
}

func openValueLog(options valueLogOptions) (*valueLog, error) {
	vLogWals := []*wal.WAL{}

	for i := 0; i < int(options.NumPartions); i++ {
		vLogWal, err := wal.Open(wal.Options{
			DirPath:        options.DirPath,
			SegmentSize:    options.SegmentSize,
			SegmentFileExt: fmt.Sprintf(".%d%s", i, valueLogFileExt),
			BlockCache:     options.BlockCache,
			Sync:           false,
			BytesPerSync:   0,
		})
		if err != nil {
			return nil, err
		}
		vLogWals = append(vLogWals, vLogWal)
	}

	return &valueLog{wals: vLogWals, numPartions: options.NumPartions}, nil
}

func (vlog *valueLog) write(data []byte, part int) (*wal.ChunkPosition, error) {
	pos, err := vlog.wals[part].Write(data)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (vlog *valueLog) read(position *VlogPosition) ([]byte, error) {
	value, err := vlog.wals[position.part].Read(position.walPos)
	if err != nil {
		return nil, err
	}

	return value, nil
}

// we must maintain correct order with respect to input while returning chunkPositions
func (vlog *valueLog) writeBatch(logs []*LogRecord) ([]*VlogPosition, error) {
	// split logs into parts
	logParts := [][]seq{}
	for i := 0; i < int(vlog.numPartions); i++ {
		logPart := []seq{}
		logParts = append(logParts, logPart)
	}
	for i, log := range logs {
		partIdx := vlog.getIndex(log.Key)
		buf := encodeLogRecord(log)
		logParts[partIdx] = append(logParts[partIdx], seq{buf: buf, id: i, part: partIdx})
	}

	// write parts concurrently
	errChan := make(chan error, vlog.numPartions)
	posChan := make(chan []seq, vlog.numPartions)
	for i := 0; i < int(vlog.numPartions); i++ {
		go func(part int) {
			pos := []seq{}
			for _, s := range logParts[part] {
				p, err := vlog.write(s.buf, part)
				if err != nil {
					errChan <- err
					posChan <- nil
					return
				}
				pos = append(pos, seq{walpos: p, id: s.id, part: s.part})
			}
			errChan <- nil
			posChan <- pos
		}(i)
	}

	flag := false
	positions := [][]seq{}
	for i := 0; i < int(vlog.numPartions); i++ {
		e := <-errChan
		pos := <-posChan
		if e != nil {
			flag = true
		}
		positions = append(positions, pos)
	}

	if flag {
		return nil, ErrWriteVlog
	}

	realPositions := vlog.mergePosSeqs(positions, len(logs))
	return realPositions, nil
}

func (vlog *valueLog) readBatch(vlogPos []*VlogPosition) ([]*LogRecord, error) {
	// split positions into parts
	posParts := [][]seq{}
	for i := 0; i < int(vlog.numPartions); i++ {
		posPart := []seq{}
		posParts = append(posParts, posPart)
	}
	for i, pos := range vlogPos {
		partIdx := pos.part
		walPos := pos.walPos
		posParts[partIdx] = append(posParts[partIdx], seq{walpos: walPos, id: i, part: partIdx})
	}

	// read parts concurrently
	errChan := make(chan error, vlog.numPartions)
	bufChan := make(chan []seq, vlog.numPartions)
	for i := 0; i < int(vlog.numPartions); i++ {
		go func(part int) {
			buf := []seq{}
			for _, s := range posParts[part] {
				b, err := vlog.read(&VlogPosition{part: s.part, walPos: s.walpos})
				if err != nil {
					errChan <- err
					bufChan <- nil
					return
				}
				buf = append(buf, seq{buf: b, id: s.id, part: s.part})
			}
			errChan <- nil
			bufChan <- buf
		}(i)
	}

	flag := false
	bufs := [][]seq{}
	for i := 0; i < int(vlog.numPartions); i++ {
		e := <-errChan
		buf := <-bufChan
		if e != nil {
			flag = true
		}
		bufs = append(bufs, buf)
	}

	if flag {
		return nil, ErrWriteVlog
	}

	realBuf := vlog.mergeBufSeqs(bufs, len(vlogPos))
	logRecords := []*LogRecord{}
	for _, buf := range realBuf {
		logRecords = append(logRecords, decodeLogRecord(buf))
	}
	return logRecords, nil
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

// multiplex merge sort for returning in correct order of input logs
func (vlog *valueLog) mergePosSeqs(seqs [][]seq, numSeq int) []*VlogPosition {
	pos := []*VlogPosition{}

	for len(pos) < numSeq {
		minId := numSeq + 1
		minIdSeq := -1
		for i := 0; i < int(vlog.numPartions); i++ {
			if len(seqs[i]) > 0 && seqs[i][0].id < minId {
				minId = seqs[i][0].id
				minIdSeq = i
			}
		}
		pos = append(pos, &VlogPosition{part: seqs[minIdSeq][0].part, walPos: seqs[minIdSeq][0].walpos})
		if len(seqs[minIdSeq]) > 1 {
			seqs[minIdSeq] = seqs[minIdSeq][1:]
		} else {
			seqs[minIdSeq] = []seq{}
		}
	}

	return pos
}

// multiplex merge sort for returning in correct order of input VlogPositions
func (vlog *valueLog) mergeBufSeqs(seqs [][]seq, numSeq int) [][]byte {
	bufs := [][]byte{}

	// multiplex merge sort
	for len(bufs) < numSeq {
		minId := numSeq + 1
		minIdSeq := -1
		for i := 0; i < int(vlog.numPartions); i++ {
			if len(seqs[i]) > 0 && seqs[i][0].id < minId {
				minId = seqs[i][0].id
				minIdSeq = i
			}
		}
		bufs = append(bufs, seqs[minIdSeq][0].buf)
		if len(seqs[minIdSeq]) > 1 {
			seqs[minIdSeq] = seqs[minIdSeq][1:]
		} else {
			seqs[minIdSeq] = []seq{}
		}
	}

	return bufs
}
