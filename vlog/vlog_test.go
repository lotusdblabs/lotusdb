package vlog

import (
	"bytes"
	"github.com/flower-corp/lotusdb/logfile"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestOpenValueLog(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testOpenValueLog(t, logfile.FileIO)
	})

	t.Run("mmap", func(t *testing.T) {
		testOpenValueLog(t, logfile.MMap)
	})
}

func testOpenValueLog(t *testing.T, ioType logfile.IOType) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	type args struct {
		path      string
		blockSize int64
		ioType    logfile.IOType
	}
	tests := []struct {
		name     string
		args     args
		hasFiles bool
		wantErr  bool
	}{
		{
			"size-zero", args{path: path, blockSize: 0, ioType: ioType}, false, true,
		},
		{
			"no-files", args{path: path, blockSize: 100, ioType: ioType}, false, false,
		},
		{
			"with-files", args{path: path, blockSize: 100, ioType: ioType}, true, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.hasFiles {
				for i := 1; i <= 5; i++ {
					_, err := logfile.OpenLogFile(path, uint32(i), 100, logfile.ValueLog, ioType)
					assert.Nil(t, err)
				}
			}
			got, err := OpenValueLog(tt.args.path, tt.args.blockSize, tt.args.ioType)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenValueLog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.NotNil(t, got)
			}
		})
	}
}

func TestValueLog_Write(t *testing.T) {
	t.Run("fileio", func(t *testing.T) {
		testValueLogWrite(t, logfile.FileIO)
	})

	t.Run("mmap", func(t *testing.T) {
		testValueLogWrite(t, logfile.MMap)
	})
}

func testValueLogWrite(t *testing.T, ioType logfile.IOType) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	vlog, err := OpenValueLog(path, 100, ioType)
	assert.Nil(t, err)

	type fields struct {
		vlog *ValueLog
	}
	type args struct {
		e *logfile.LogEntry
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ValuePos
		wantErr bool
	}{
		// don`t run the sub test alone, because the offset is incremental, run them all at once!!!
		{
			"nil-entry", fields{vlog: vlog}, args{e: nil}, &ValuePos{}, false,
		},
		{
			"no-key", fields{vlog: vlog}, args{e: &logfile.LogEntry{Value: []byte("lotusdb")}}, &ValuePos{Fid: 0, Offset: 0}, false,
		},
		{
			"no-value", fields{vlog: vlog}, args{e: &logfile.LogEntry{Key: []byte("key1")}}, &ValuePos{Fid: 0, Offset: 15}, false,
		},
		{
			"with-key-value", fields{vlog: vlog}, args{e: &logfile.LogEntry{Key: []byte("key2"), Value: []byte("lotusdb-2")}}, &ValuePos{Fid: 0, Offset: 27}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vlog := tt.fields.vlog
			got, err := vlog.Write(tt.args.e)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
			// get from vlog.
			if tt.args.e != nil {
				e, err := vlog.Read(got.Fid, got.Offset)
				assert.Nil(t, err)
				if bytes.Compare(e.Key, tt.args.e.Key) != 0 || bytes.Compare(e.Value, tt.args.e.Value) != 0 {
					t.Errorf("Write() write = %v, but got = %v", tt.args.e, got)
				}
			}
		})
	}
}

func TestValueLog_WriteAfterReopen(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	vlog, err := OpenValueLog(path, 100, logfile.FileIO)
	assert.Nil(t, err)

	tests := []*logfile.LogEntry{
		{
			Key: []byte("key-1"), Value: []byte("val-1")},
		{
			Key: []byte("key-2"), Value: []byte("val-2"),
		},
	}
	var pos []*ValuePos

	pos1, err := vlog.Write(tests[0])
	assert.Nil(t, err)
	pos = append(pos, pos1)

	err = vlog.Close()
	assert.Nil(t, err)

	// reopen it.
	vlog, err = OpenValueLog(path, 100, logfile.MMap)
	pos2, err := vlog.Write(tests[1])
	assert.Nil(t, err)
	pos = append(pos, pos2)

	for i := 0; i < len(pos); i++ {
		res, err := vlog.Read(pos[i].Fid, pos[i].Offset)
		assert.Nil(t, err)
		if bytes.Compare(res.Key, tests[i].Key) != 0 || bytes.Compare(res.Value, tests[i].Value) != 0 {
			t.Errorf("WriteAfterReopen() write = %v, but got = %v", tests[i], res)
		}
	}
}
