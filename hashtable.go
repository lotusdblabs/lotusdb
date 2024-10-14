package lotusdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
)

// diskhash requires fixed-size value
// so we set the slotValueLength to `binary.MaxVarintLen32*3 + binary.MaxVarintLen64`.
// This is the maximum length after wal.chunkPosition encoding.
const slotValueLength = binary.MaxVarintLen32*3 + binary.MaxVarintLen64

// HashTable is the diskhash index implementation.
// see: https://github.com/rosedblabs/diskhash
type HashTable struct {
	options indexOptions
	tables  []*diskhash.Table
}

// openHashIndex open a diskhash for each partition.
// The partition number is specified by the index options.
func openHashIndex(options indexOptions) (*HashTable, error) {
	tables := make([]*diskhash.Table, options.partitionNum)

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
		options: options,
		tables:  tables,
	}, nil
}

// PutBatch put batch records to index.
func (ht *HashTable) PutBatch(positions []*KeyPosition, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	partitionRecords := make([][]*KeyPosition, ht.options.partitionNum)
	matchKeys := make([][]diskhash.MatchKeyFunc, ht.options.partitionNum)
	for i, pos := range positions {
		p := pos.partition
		partitionRecords[p] = append(partitionRecords[p], pos)
		matchKeys[p] = append(matchKeys[p], matchKeyFunc[i])
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
			matchKey := matchKeys[partition]
			for i, record := range partitionRecords[partition] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if len(record.key) == 0 {
						return ErrKeyIsEmpty
					}
					encPos := record.position.EncodeFixedSize()
					if err := table.Put(record.key, encPos, matchKey[i]); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return nil, g.Wait()
}

// Get chunk position by key.
func (ht *HashTable) Get(key []byte, matchKeyFunc ...diskhash.MatchKeyFunc) (*KeyPosition, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	p := ht.options.getKeyPartition(key)
	table := ht.tables[p]
	err := table.Get(key, matchKeyFunc[0])
	if err != nil {
		return nil, err
	}
	//nolint:nilnil // hashTable will not use keyPosition, so return nil, nil
	return nil, nil
}

// DeleteBatch delete batch records from index.
func (ht *HashTable) DeleteBatch(keys [][]byte, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	partitionKeys := make([][][]byte, ht.options.partitionNum)
	matchKeys := make([][]*diskhash.MatchKeyFunc, ht.options.partitionNum)
	for i, key := range keys {
		p := ht.options.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
		matchKeys[p] = append(matchKeys[p], &matchKeyFunc[i])
	}
	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionKeys {
		partition := i
		if len(partitionKeys[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			table := ht.tables[partition]
			matchKey := matchKeys[partition]
			for i, key := range partitionKeys[partition] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if len(key) == 0 {
						return ErrKeyIsEmpty
					}
					if err := table.Delete(key, *matchKey[i]); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return nil, g.Wait()
}

// Sync sync index data to disk.
func (ht *HashTable) Sync() error {
	for _, table := range ht.tables {
		err := table.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close close index.
func (ht *HashTable) Close() error {
	for _, table := range ht.tables {
		err := table.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// MatchKeyFunc Set nil if do not need keyPos or value.
func MatchKeyFunc(db *DB, key []byte, keyPos **KeyPosition, value *[]byte) func(slot diskhash.Slot) (bool, error) {
	return func(slot diskhash.Slot) (bool, error) {
		chunkPosition := wal.DecodeChunkPosition(slot.Value)
		checkKeyPos := &KeyPosition{
			key:       key,
			partition: uint32(db.vlog.getKeyPartition(key)),
			position:  chunkPosition,
		}
		valueLogRecord, err := db.vlog.read(checkKeyPos)
		if err != nil {
			return false, err
		}
		if valueLogRecord == nil {
			return false, nil
		}
		if !bytes.Equal(valueLogRecord.key, key) {
			return false, nil
		}
		if keyPos != nil {
			*keyPos = checkKeyPos
		}
		if value != nil {
			*value = valueLogRecord.value
		}
		return true, nil
	}
}
