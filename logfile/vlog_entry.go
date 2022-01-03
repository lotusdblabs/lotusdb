package logfile

type VlogEntry struct {
	Key   []byte
	Value []byte
	KSize uint32
	VSize uint32
}

func EncodeVlogEntry(ve *VlogEntry) ([]byte, int) {
	return nil, 0
}
