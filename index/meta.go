package index

type IndexerMeta struct {
	Value  []byte
	Fid    uint32
	Offset int64
	Size   uint32
}

func EncodeMeta() ([]byte, int) {
	return nil, 0
}

func DecodeMeta([]byte) *IndexerMeta {
	return nil
}
