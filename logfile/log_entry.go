package logfile

import (
	"encoding/binary"
	"hash/crc32"
)

const entryHeaderSize = 20

type LogEntry struct {
	Key       []byte
	Value     []byte
	ExpiredAt uint64 // time.Unix
}

type entryHeader struct {
	kSize     uint32
	vSize     uint32
	expiredAt uint64 // time.Unix
	crc32     uint32 // check sum
}

func (e *LogEntry) size() int {
	return entryHeaderSize + len(e.Key) + len(e.Value)
}

func encodeEntry(e *LogEntry) []byte {
	buf := make([]byte, e.size())
	// encode header.
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(e.Value)))
	binary.LittleEndian.PutUint64(buf[12:20], e.ExpiredAt)

	// key and value.
	copy(buf[entryHeaderSize:], e.Key)
	copy(buf[entryHeaderSize+len(e.Key):], e.Value)

	// crc32.
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf
}

func decodeHeader(buf []byte) *entryHeader {
	return &entryHeader{
		kSize:     binary.LittleEndian.Uint32(buf[4:8]),
		vSize:     binary.LittleEndian.Uint32(buf[8:12]),
		expiredAt: binary.LittleEndian.Uint64(buf[12:20]),
		crc32:     binary.LittleEndian.Uint32(buf[:4]),
	}
}

func getEntryCrc(e *LogEntry, h []byte) uint32 {
	crc := crc32.ChecksumIEEE(h[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
