package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewLotusLogger(t *testing.T) {
	t.Run("log output to Stdout", func(t *testing.T) {
		log := NewLotusLogger(nil)
		assert.NotNil(t, log.logger)
	})

	t.Run("log output to file", func(t *testing.T) {
		cfg := &LotusLoggerConfig{
			OutputType:    OutputTypeFile,
			LogNamePrefix: "test",
			LogDir:        "./",
			Size:          1,
			Backups:       1,
			MaxAge:        1,
			LocalTime:     true,
			Compress:      false,
		}
		log := NewLotusLogger(cfg)
		assert.NotNil(t, log.logger)

		// cleanup
		filepath.Walk(cfg.LogDir, func(path string, info os.FileInfo, err error) error {
			if filepath.Ext(path) == ".log" {
				os.Remove(path)
			}
			return nil
		})
	})
}

func TestLotusLogger_genLogPath(t *testing.T) {
	cfg := &LotusLoggerConfig{
		LogNamePrefix: "test",
		LogDir:        "/data/logs",
	}
	lowPath, highPath := cfg.genLogPath()
	assert.Equal(t, "/data/logs/test_info.log", lowPath)
	assert.Equal(t, "/data/logs/test_error.log", highPath)
}

// BenchmarkLotusLoggerInfoStd-10             47004             38342 ns/op             152 B/op          2 allocs/op
// ok      github.com/lotusdblabs/lotusdb/v2/util  2.218s
func BenchmarkLotusLoggerInfoStd(B *testing.B) {
	log := NewLotusLogger(nil)
	assert.NotNil(B, log.logger)
	for i := 0; i < B.N; i++ {
		log.logger.Info("test", zap.String("test", "test1"), zap.Int("seq", i))
	}
}

// BenchmarkLotusLoggerInfoFile-10           503023              2392 ns/op             152 B/op          2 allocs/op
// ok      github.com/lotusdblabs/lotusdb/v2/util  1.849s
func BenchmarkLotusLoggerInfoFile(B *testing.B) {
	cfg := &LotusLoggerConfig{
		OutputType:    OutputTypeFile,
		LogNamePrefix: "test",
		LogDir:        "./",
		Size:          1,
		Backups:       1,
		MaxAge:        1,
		LocalTime:     true,
		Compress:      false,
	}
	log := NewLotusLogger(cfg)
	for i := 0; i < B.N; i++ {
		log.logger.Info("test", zap.String("test", "test1"), zap.Int("seq", i))
	}
	// cleanup
	filepath.Walk(cfg.LogDir, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) == ".log" {
			os.Remove(path)
		}
		return nil
	})
}
