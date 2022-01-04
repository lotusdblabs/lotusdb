package logfile

import "encoding/binary"

const vlogHeaderSize = 10

type VlogEntry struct {
	Key   []byte
	Value []byte
}

func EncodeVlogEntry(ve *VlogEntry) ([]byte, int) {
	header := make([]byte, vlogHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(len(ve.Key)))
	index += binary.PutVarint(header[index:], int64(len(ve.Value)))

	size := index + len(ve.Key) + len(ve.Value)
	buf := make([]byte, size)
	copy(buf[:index], header[:index])
	copy(buf[index:], ve.Key)
	copy(buf[index+len(ve.Key):], ve.Value)
	return buf, size
}

func DecodeVlogEntry(b []byte) *VlogEntry {
	var index int
	ksize, n := binary.Varint(b[index:])
	index += n
	_, n = binary.Varint(b[index:])
	index += n

	return &VlogEntry{
		Key:   b[index : index+int(ksize)],
		Value: b[index+int(ksize):],
	}
}
