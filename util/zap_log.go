package util

import (
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2" // split log file tool
)

const (
	GeneralDatetimeFormat = "2006-01-02 15:04:05.000"
)

// OutputType log output type
type OutputType int8

const (
	OutputTypeStd  = iota + 1 // OutputTypeStd log output to Stdout
	OutputTypeFile            // OutputTypeFile log output to file
)

// LotusLogger lotus logger
type LotusLogger struct {
	logger *zap.Logger
}

// write a zap log config struct
type LotusLoggerConfig struct {
	OutputType OutputType // log output type 1 is std 2 is file
	// if OutputType is 2 below configs is useful

	// log filename prefix use with log dir and generate log file name
	LogNamePrefix string
	LogDir        string
	Size          int
	Backups       int
	MaxAge        int
	LocalTime     bool
	Compress      bool
}

// genLogPath generate log path: lowPath and highPath
// eg. if logDir is /data/logs or /data/logs/ and logNamePrefix is lotus
// return /data/logs/lotus_info.log and /data/logs/lotus_error.log
func (lcgf *LotusLoggerConfig) genLogPath() (string, string) {
	logDir := ""
	if strings.HasSuffix(lcgf.LogDir, "/") {
		logDir = lcgf.LogDir
	} else {
		logDir = lcgf.LogDir + "/"
	}
	return logDir + lcgf.LogNamePrefix + "_info.log", logDir + lcgf.LogNamePrefix + "_error.log"
}

// isAllLevel match all level logs
func isAllLevel(lvl zapcore.Level) bool {
	return lvl >= zapcore.DebugLevel
}

// isInfoLevel match info level logs
func isInfoLevel(lvl zapcore.Level) bool {
	return lvl < zapcore.ErrorLevel
}

// isErrLevel match error level logs
func isErrLevel(lvl zapcore.Level) bool {
	return lvl >= zapcore.ErrorLevel
}

// NewLotusLogger create a lotus logger
func NewLotusLogger(cfg *LotusLoggerConfig) *LotusLogger {
	// log output to Stdout
	if cfg == nil || cfg.OutputType == OutputTypeStd {
		log := &LotusLogger{}
		encoder := createEncoderConfig()
		core := zapcore.NewTee(
			zapcore.NewCore(encoder,
				zapcore.AddSync(os.Stdout), zap.LevelEnablerFunc(isAllLevel)),
		)
		log.logger = zap.New(core)
		return log
	}
	// log output to file
	lowPath, highPath := cfg.genLogPath()

	return NewLoggerWithFileConfigs(lowPath, highPath, cfg.Size, cfg.Backups, cfg.MaxAge, cfg.LocalTime, cfg.Compress)
}

// createEncoderConfig create a zap log encoder config
func createEncoderConfig() zapcore.Encoder {
	return zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:       "t",
		LevelKey:      "lv",
		NameKey:       "log",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time,
			enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format(GeneralDatetimeFormat))
		},
		EncodeDuration: func(d time.Duration, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendInt64(int64(d) / 1000000)
		},
		EncodeCaller: zapcore.ShortCallerEncoder,
	})
}

// NewLoggerWithFileConfigs create a logger with file configs
// lowPath is info level log file path
// highPath is error level log file path
func NewLoggerWithFileConfigs(
	lowPath, highPath string,
	size, backups, maxAge int, localTime, compress bool,
) *LotusLogger {

	log := &LotusLogger{}
	field := zap.Fields(zap.String("engine_name", "lotusdb"))
	encoder := createEncoderConfig()
	lowWriter := createHook(lowPath, size,
		backups, maxAge, localTime, compress)
	hightWriter := createHook(highPath, size,
		backups, maxAge, localTime, compress)
	core := zapcore.NewTee(
		zapcore.NewCore(encoder,
			zapcore.AddSync(&lowWriter), zap.LevelEnablerFunc(isInfoLevel)),
		zapcore.NewCore(encoder,
			zapcore.AddSync(&hightWriter), zap.LevelEnablerFunc(isErrLevel)),
	)
	log.logger = zap.New(core, field)
	return log
}

func createHook(
	file string, size, backups, maxAge int,
	localTime, compress bool,
) lumberjack.Logger {

	return lumberjack.Logger{
		Filename:   file,      // log file path
		MaxSize:    size,      // log file max size
		MaxBackups: backups,   // log file max backups
		MaxAge:     maxAge,    // log file save days
		LocalTime:  localTime, // is use local time
		Compress:   compress,  // is compress log file
	}
}
