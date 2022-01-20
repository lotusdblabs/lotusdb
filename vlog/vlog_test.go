package vlog

import (
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
	path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	assert.Nil(t, err)
	err = os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	defer func() {
		//_ = os.RemoveAll(path)
	}()
	vlog, err := OpenValueLog(path, 100, logfile.FileIO)
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
		{
			"nil-entry", fields{vlog: vlog}, args{e: nil}, &ValuePos{}, false,
		},
		{
			"no-key", fields{vlog: vlog}, args{e: &logfile.LogEntry{Value: []byte("lotusdb")}}, &ValuePos{Fid: 0, Offset: 0}, false,
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
		})
	}
}

func TestValueLog_Close(t *testing.T) {
	//path, err := filepath.Abs(filepath.Join("/tmp", "vlog-test"))
	//assert.Nil(t, err)
	//err = os.MkdirAll(path, os.ModePerm)
	//assert.Nil(t, err)
	//
	//log, err := OpenValueLog(path, 1024 << 20, logfile.FileIO)
	//assert.Nil(t, err)
}
