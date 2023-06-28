package index

// Indexer index data are stored in indexer.
type Indexer interface {
	Get(key []byte) error

	Close() error

	Sync() error
}
