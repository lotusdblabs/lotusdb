package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrEndOfEntry = errors.New("logfile: end of entry in log file")
	ErrInvalidCrc = errors.New("logfile: invalid crc")
)

// MaxHeaderSize max entry header size.
// crc32	typ    kSize	vSize	expiredAt
//
//	4    +   1   +   5   +   5    +    10      = 25 (refer to binary.MaxVarintLen32 and binary.MaxVarintLen64)
const MaxHeaderSize = 25

// EntryType type of Entry.
type EntryType byte

const (
	// TypeDelete represents entry type is delete.
	TypeDelete EntryType = iota + 1
)

type WalLogEntry struct {
	Key       []byte
	Value     []byte
	ExpiredAt int64 // time.Unix
	Type      EntryType
}

type entryHeader struct {
	crc32     uint32 // check sum
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiredAt int64 // time.Unix
}

// EncodeEntry will encode entry into a byte slice.
// The encoded Entry looks like:
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//
//	|--------------------------crc check---------------------------|
func EncodeEntry(e *WalLogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}
	header := make([]byte, MaxHeaderSize)
	// encode header.
	header[4] = byte(e.Type)
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(e.Key)))
	index += binary.PutVarint(header[index:], int64(len(e.Value)))
	index += binary.PutVarint(header[index:], e.ExpiredAt)

	var size = index + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	copy(buf[:index], header[:])
	// key and value.
	copy(buf[index:], e.Key)
	copy(buf[index+len(e.Key):], e.Value)

	// crc32.
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

// decode []byte read from wal, return WalEntry, entry size and error
func (wal *WAL) DecodeWAL(buf []byte) (*WalLogEntry, int64, error) {
	var headerBuf []byte
	if len(buf) > MaxHeaderSize {
		headerBuf = buf[:MaxHeaderSize]
	} else {
		headerBuf = buf
	}

	header, size := decodeHeader(headerBuf)

	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	e := &WalLogEntry{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	kSize, vSize := int64(header.kSize), int64(header.vSize)
	var entrySize = size + kSize + vSize

	// read entry key and value.
	if kSize > 0 || vSize > 0 {
		kvBuf := buf[size : size+kSize+vSize]
		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]
	}

	// crc32 check.
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil

}

func decodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	h := &entryHeader{
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   EntryType(buf[4]),
	}
	var index = 5
	ksize, n := binary.Varint(buf[index:])
	h.kSize = uint32(ksize)
	index += n

	vsize, n := binary.Varint(buf[index:])
	h.vSize = uint32(vsize)
	index += n

	expiredAt, n := binary.Varint(buf[index:])
	h.expiredAt = expiredAt
	return h, int64(index + n)
}

func getEntryCrc(e *WalLogEntry, h []byte) uint32 {
	if e == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(h[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
