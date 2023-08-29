package lotusdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
	"github.com/spaolacci/murmur3"
	"golang.org/x/sync/errgroup"
)

// diskhash requires fixed-size value, so we set the slotValueLength to `binary.MaxVarintLen32*3 + binary.MaxVarintLen64`.
// This is the maximum length after wal.chunkPosition encoding.
const slotValueLength = binary.MaxVarintLen32*3 + binary.MaxVarintLen64

// HashTable is the diskhash index implementation.
// see: https://github.com/rosedblabs/diskhash
type HashTable struct {
	options     indexOptions
	tables      []*diskhash.Table
	bytesBuffer *sync.Pool
}

// openHashIndex open a diskhash for each partition.
// The partition number is specified by the index options.
func openHashIndex(options indexOptions) (*HashTable, error) {
	tables := make([]*diskhash.Table, options.partitionNum)
	bytesBuffer := &sync.Pool{
		New: func() any {
			return make([]byte, slotValueLength)
		},
	}
	for i := 0; i < options.partitionNum; i++ {
		dishHashOptions := diskhash.DefaultOptions
		dishHashOptions.DirPath = filepath.Join(options.dirPath, fmt.Sprintf(indexFileExt, i))
		dishHashOptions.SlotValueLength = slotValueLength
		table, err := diskhash.Open(dishHashOptions)
		if err != nil {
			return nil, err
		}
		tables[i] = table
	}

	return &HashTable{
		options:     options,
		tables:      tables,
		bytesBuffer: bytesBuffer,
	}, nil
}

// PutBatch put batch records to index
func (ht *HashTable) PutBatch(positions []*KeyPosition) error {
	if len(positions) == 0 {
		return nil
	}

	partitionRecords := make([][]*KeyPosition, ht.options.partitionNum)
	for _, pos := range positions {
		p := pos.partition
		partitionRecords[p] = append(partitionRecords[p], pos)
	}

	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionRecords {
		partition := i
		if len(partitionRecords[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			// get the hashtable instance for this partition
			table := ht.tables[partition]
			for _, record := range partitionRecords[partition] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if record.key == nil || len(record.key) == 0 {
						return errors.New("key required")
					}
					matchKey := func(slot diskhash.Slot) (bool, error) {
						if murmur3.Sum32(record.key) == slot.Hash {
							return true, nil
						}
						return false, nil
					}
					encPos := ht.bytesBuffer.Get().([]byte)
					copy(encPos, record.position.Encode())
					if err := table.Put(record.key, encPos, matchKey); err != nil {
						return err
					}
					ht.bytesBuffer.Put(encPos)
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// Get chunk position by key
func (ht *HashTable) Get(key []byte) (*KeyPosition, error) {
	p := ht.options.getKeyPartition(key)
	table := ht.tables[p]
	var value []byte
	var keyPos *KeyPosition

	getFunc := func(slot diskhash.Slot) (bool, error) {
		if murmur3.Sum32(key) == slot.Hash {
			value = make([]byte, len(slot.Value))
			copy(value, slot.Value)
			return true, nil
		}
		return false, nil
	}

	err := table.Get(key, getFunc)
	if err != nil {
		return nil, err
	}
	if len(value) != 0 {
		keyPos = new(KeyPosition)
		keyPos.key, keyPos.partition = key, uint32(p)
		keyPos.position = wal.DecodeChunkPosition(value)
	}
	return keyPos, nil
}

// DeleteBatch delete batch records from index
func (ht *HashTable) DeleteBatch(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	partitionKeys := make([][][]byte, ht.options.partitionNum)
	for _, key := range keys {
		p := ht.options.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
	}
	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionKeys {
		partition := i
		if len(partitionKeys[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			table := ht.tables[partition]
			for _, key := range partitionKeys[partition] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					matchKey := func(slot diskhash.Slot) (bool, error) {
						if murmur3.Sum32(key) == slot.Hash {
							return true, nil
						}
						return false, nil
					}
					if err := table.Delete(key, matchKey); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// Sync sync index data to disk
func (ht *HashTable) Sync() error {
	for _, table := range ht.tables {
		err := table.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close index
func (ht *HashTable) Close() error {
	for _, table := range ht.tables {
		err := table.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
