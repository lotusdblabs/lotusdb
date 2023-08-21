package lotusdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
	"github.com/spaolacci/murmur3"
	"golang.org/x/sync/errgroup"
)

type HashTable struct {
	options indexOptions
	tables  []*diskhash.Table
}

func OpenHashTable(options indexOptions) (*HashTable, error) {
	tables := make([]*diskhash.Table, options.partitionNum)
	for i := 0; i < options.partitionNum; i++ {
		dishHashOptions := diskhash.DefaultOptions
		dishHashOptions.DirPath = filepath.Join(options.dirPath, fmt.Sprintf(indexFileExt, i))
		dishHashOptions.SlotValueLength = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
		table, err := diskhash.Open(dishHashOptions)
		if err != nil {
			return nil, err
		}
		tables[i] = table
	}

	return &HashTable{
		options: options,
		tables:  tables,
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
			// get the bolt db instance for this partition
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
					encPos := encodePosition(record.position)
					if err := table.Put(record.key, encPos, matchKey); err != nil {
						return err
					}
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

func encodePosition(cp *wal.ChunkPosition) []byte {
	maxLen := binary.MaxVarintLen32*3 + binary.MaxVarintLen64
	buf := make([]byte, maxLen)

	var index = 0
	// SegmentId
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	// BlockNumber
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	// ChunkOffset
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	// ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))

	return buf
}
