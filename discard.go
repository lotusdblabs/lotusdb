package lotusdb

import (
	"encoding/binary"
	"io"
	"path/filepath"
	"sync"

	"github.com/flower-corp/lotusdb/index"
	"github.com/flower-corp/lotusdb/ioselector"
	"github.com/flower-corp/lotusdb/logfile"
	"github.com/flower-corp/lotusdb/logger"
)

const discardRecordSize = 12

type Discard struct {
	sync.Mutex
	valChan chan [][]byte
	file    ioselector.IOSelector
}

func newDiscard(path, name string) (*Discard, error) {
	fname := filepath.Join(path, name)
	file, err := ioselector.NewMMapSelector(fname, 1<<12)
	if err != nil {
		return nil, err
	}

	d := &Discard{
		valChan: make(chan [][]byte, 1024),
		file:    file,
	}
	go d.listenUpdates()
	return d, nil
}

// iterate and find the file with most discarded data,
// there are 256 records at most, no need to worry about the performance.
func (d *Discard) maxDiscardFid() (uint32, float64, error) {
	var maxFid uint32
	var maxRatio float64
	var offset int64
	d.Lock()
	defer d.Unlock()
	for {
		buf := make([]byte, discardRecordSize)
		_, err := d.file.Read(buf, offset)
		if err != nil {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return 0, 0, err
		}
		offset += discardRecordSize

		fid := binary.LittleEndian.Uint32(buf[:4])
		totalCount := binary.LittleEndian.Uint32(buf[4:8])
		discardCount := binary.LittleEndian.Uint32(buf[8:12])
		ratio := float64(discardCount) / float64(totalCount)
		if ratio > maxRatio {
			maxRatio = ratio
			maxFid = fid
		}
	}
	return maxFid, maxRatio, nil
}

func (d *Discard) listenUpdates() {
	for {
		select {
		case oldVal := <-d.valChan:
			for _, buf := range oldVal {
				meta := index.DecodeMeta(buf)
				d.incrDiscard(meta.Fid)
			}
		}
	}
}

func (d *Discard) incrTotal(fid uint32) {
	d.incr(fid, true, 1)
}

func (d *Discard) incrDiscard(fid uint32) {
	d.incr(fid, false, 1)
}

func (d *Discard) clear(fid uint32) {
	d.incr(fid, false, -1)
}

// Discard file`s format:
// +-------+--------------+---------------+  +-------+--------------+---------------+
// |  fid  |  total count | discard count |  |  fid  |  total count | discard count |
// +-------+--------------+---------------+  +-------+--------------+---------------+
// 0-------4--------------8--------------12  +-------16------------20---------------24
func (d *Discard) incr(fid uint32, isTotal bool, delta int) {
	d.Lock()
	defer d.Unlock()

	fileid := make([]byte, 4)
	binary.LittleEndian.PutUint32(fileid, fid)
	if _, err := d.file.Write(fileid, int64(fid*discardRecordSize)); err != nil {
		logger.Errorf("incr value in discard err:%v", err)
		return
	}

	var buf []byte
	var offset int64
	if delta > 0 {
		buf = make([]byte, 4)
		if isTotal {
			offset = int64(fid*discardRecordSize + 4)
		} else {
			offset = int64(fid*discardRecordSize + 8)
		}

		if _, err := d.file.Read(buf, offset); err != nil {
			logger.Errorf("incr value in discard err:%v", err)
			return
		}

		v := binary.LittleEndian.Uint32(buf)
		binary.LittleEndian.PutUint32(buf, v+uint32(delta))
	} else {
		buf = make([]byte, 8)
		offset = int64(fid * discardRecordSize)
	}

	if _, err := d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err:%v", err)
		return
	}
}
