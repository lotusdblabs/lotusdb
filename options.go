package lotusdb

// EntryOptions set optional params for PutWithOptions and DeleteWithOptions.
// If use Put and Delete (without options), that means to use the default values.
type EntryOptions struct {
	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.

	// Default value is false.
	Sync bool

	// DisableWal if true, writes will not first go to the write ahead log, and the write may get lost after a crash.
	// Setting true only if don`t care about the data loss.
	// Default value is false.
	DisableWal bool

	// ExpiredAt time to live for the specified key, must be a time.Unix value.
	// It will be ignored if it`s not a positive number.
	// Default value is 0.
	ExpiredAt int64
}
