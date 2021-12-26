package logfile

const entryHeaderSize = 0

type logEntry struct {
	key       []byte
	value     []byte
	expiredAt uint64
}
