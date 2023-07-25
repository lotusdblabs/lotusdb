package lotusdb

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	// DefaultLevel the default log level
	DefaultLevel = zapcore.InfoLevel

	// DefaultTimeLayout the default time layout;
	DefaultTimeLayout = time.RFC3339
)

var logger *zap.Logger

// Option custom setup config
type Option func(*option)

type option struct {
	level          zapcore.Level
	fields         map[string]string
	file           io.Writer
	timeLayout     string
	disableConsole bool
}

func WithDebugLevel() Option {
	return func(opt *option) {
		opt.level = zapcore.DebugLevel
	}
}

func WithInfoLevel() Option {
	return func(opt *option) {
		opt.level = zapcore.InfoLevel
	}
}

func WithWarnLevel() Option {
	return func(opt *option) {
		opt.level = zapcore.WarnLevel
	}
}

func WithErrorLevel() Option {
	return func(opt *option) {
		opt.level = zapcore.ErrorLevel
	}
}

// WithField add some customize fields to logger
func WithField(key, value string) Option {
	return func(opt *option) {
		opt.fields[key] = value
	}
}

func WithTimeLayout(timeLayout string) Option {
	return func(opt *option) {
		opt.timeLayout = timeLayout
	}
}

func WithDisableConsole() Option {
	return func(opt *option) {
		opt.disableConsole = true
	}
}

// WithFile set file path
func WithFile(file string) Option {
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0766); err != nil {
		panic(err)
	}

	f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		panic(err)
	}

	return func(opt *option) {
		opt.file = zapcore.Lock(f)
	}
}

// WithFileRotation write log with rotation
func WithFileRotation(file string) Option {
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0766); err != nil {
		panic(err)
	}

	return func(opt *option) {
		opt.file = &lumberjack.Logger{ // concurrent-safed
			Filename:   file, // 文件路径
			MaxSize:    128,  // 单个文件最大尺寸，默认单位 M
			MaxBackups: 300,  // 最多保留 300 个备份
			MaxAge:     30,   // 最大时间，默认单位 day
			LocalTime:  true, // 使用本地时间
			Compress:   true, // 是否压缩 disabled by default
		}
	}
}

// NewJSONLogger return a json-encoder zap logger,
func NewJSONLogger(opts ...Option) (*zap.Logger, error) {
	opt := &option{level: DefaultLevel, fields: make(map[string]string)}
	for _, f := range opts {
		f(opt)
	}

	timeLayout := DefaultTimeLayout
	if opt.timeLayout != "" {
		timeLayout = opt.timeLayout
	}

	// similar to zap.NewProductionEncoderConfig()
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger", // used by logger.Named(key); optional; useless
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace", // use by zap.AddStacktrace; optional; useless
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder, // 小写编码器
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format(timeLayout))
		},
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder, // 全路径编码器
	}

	jsonEncoder := zapcore.NewJSONEncoder(encoderConfig)

	// lowPriority usd by info\debug\warn
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= opt.level && lvl < zapcore.ErrorLevel
	})

	// highPriority usd by error\panic\fatal
	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= opt.level && lvl >= zapcore.ErrorLevel
	})

	stdout := zapcore.Lock(os.Stdout) // lock for concurrent safe
	stderr := zapcore.Lock(os.Stderr) // lock for concurrent safe

	core := zapcore.NewTee()

	if !opt.disableConsole {
		core = zapcore.NewTee(
			zapcore.NewCore(jsonEncoder,
				zapcore.NewMultiWriteSyncer(stdout),
				lowPriority,
			),
			zapcore.NewCore(jsonEncoder,
				zapcore.NewMultiWriteSyncer(stderr),
				highPriority,
			),
		)
	}

	if opt.file != nil {
		core = zapcore.NewTee(core,
			zapcore.NewCore(jsonEncoder,
				zapcore.AddSync(opt.file),
				zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
					return lvl >= opt.level
				}),
			),
		)
	}

	log := zap.New(core,
		zap.AddCaller(),
		zap.ErrorOutput(stderr),
	)

	for key, value := range opt.fields {
		log = log.WithOptions(zap.Fields(zapcore.Field{Key: key, Type: zapcore.StringType, String: value}))
	}

	// 给全局logger对象
	logger = log

	return log, nil
}

var _ Meta = (*meta)(nil)

// Meta key-value
type Meta interface {
	Key() string
	Value() interface{}
}

type meta struct {
	key   string
	value interface{}
}

func (m *meta) Key() string {
	return m.key
}

func (m *meta) Value() interface{} {
	return m.value
}

func NewMeta(key string, value interface{}) Meta {
	return &meta{key: key, value: value}
}

// WrapMeta wrap metas as zap fields
func WrapMeta(err error, metas ...Meta) (fields []zap.Field) {
	capacity := len(metas) + 1 // namespace meta
	if err != nil {
		capacity++
	}

	fields = make([]zap.Field, 0, capacity)
	if err != nil {
		fields = append(fields, zap.Error(err))
	}

	fields = append(fields, zap.Namespace("meta"))
	for _, meta := range metas {
		fields = append(fields, zap.Any(meta.Key(), meta.Value()))
	}

	return
}

func setLogger() {
	if logger == nil {
		logger, _ = NewJSONLogger()
	}
}

func Info(msg string, fields ...zap.Field) {
	setLogger()
	logger.Info(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	setLogger()
	logger.Debug(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	setLogger()
	logger.Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	setLogger()
	logger.Error(msg, fields...)
}

// Sync calls the underlying Core's Sync method, flushing any buffered log
// entries. Applications should take care to call Sync before exiting.
func Sync() {
	if logger != nil {
		_ = logger.Sync()
	}
}
