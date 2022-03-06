package lotusdb

import (
	"encoding/binary"
	"path/filepath"
	"sync"

	"github.com/flower-corp/lotusdb/index"
	"github.com/flower-corp/lotusdb/ioselector"
	"github.com/flower-corp/lotusdb/logger"
)

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
	d.incr(fid, true)
}

func (d *Discard) incrDiscard(fid uint32) {
	d.incr(fid, false)
}

// Discard file`s format:
// +-------+--------------+---------------+  +-------+--------------+---------------+
// |  fid  |  total count | discard count |  |  fid  |  total count | discard count |
// +-------+--------------+---------------+  +-------+--------------+---------------+
// 0-------4--------------8--------------12  +-------16------------20---------------24
func (d *Discard) incr(fid uint32, isTotal bool) {
	d.Lock()
	defer d.Unlock()

	buf := make([]byte, 4)
	var offset int64
	if isTotal {
		offset = int64(fid*12 + 4)
	} else {
		offset = int64(fid*12 + 8)
	}

	if _, err := d.file.Read(buf, offset); err != nil {
		logger.Errorf("incr value in discard err:%v", err)
		return
	}

	v := binary.LittleEndian.Uint32(buf)
	binary.LittleEndian.PutUint32(buf, v+1)
	if _, err := d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err:%v", err)
		return
	}
}
