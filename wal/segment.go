package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	lru "github.com/hashicorp/golang-lru/v2"
)

type ChunkType = byte
type SegmentID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	// 7 Bytes
	// Checksum Length Type
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * KB

	fileModePerm = 0644
)

// Segment represents a single segment file in WAL.
// The segment file is append-only, and the data is written in blocks.
// Each block is 32KB, and the data is written in chunks.
type segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	cache              *lru.Cache[uint64, []byte]
}

// segmentReader is used to iterate all the data from the segment file.
// You can call Next to get the next chunk data,
// and io.EOF will be returned when there is no data.
type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

// ChunkPosition represents the position of a chunk in a segment file.
// Used to read the data from the segment file.
type ChunkPosition struct {
	SegmentId SegmentID
	// BlockNumber The block number of the chunk in the segment file.
	BlockNumber uint32
	// ChunkOffset The start offset of the chunk in the segment file.
	ChunkOffset int64
	// ChunkSize How many bytes the chunk data takes up in the segment file.
	ChunkSize uint32
}

// openSegmentFile a new segment file.
func openSegmentFile(dirPath, extName string, id uint32, cache *lru.Cache[uint64, []byte]) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	// set the current block number and block size.
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err))
	}

	return &segment{
		id:                 id,
		fd:                 fd,
		cache:              cache,
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
	}, nil
}

func (seg *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.fd.Close()
	}

	return os.Remove(seg.fd.Name())
}

func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.fd.Close()
}

func (seg *segment) Size() int64 {
	return int64(seg.currentBlockNumber*blockSize + seg.currentBlockSize)
}

func (seg *segment) Write(data []byte) (*ChunkPosition, error) {
	if seg.closed {
		return nil, ErrClosed
	}

	// The left block space is not enough for a chunk header
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		// padding if necessary
		if seg.currentBlockSize < blockSize {
			padding := make([]byte, blockSize-seg.currentBlockSize)
			if _, err := seg.fd.Write(padding); err != nil {
				return nil, err
			}
		}

		// A new block, clear the current block size.
		seg.currentBlockNumber += 1
		seg.currentBlockSize = 0
	}

	// the start position(for read operation)
	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}
	dataSize := uint32(len(data))
	// The entire chunk can fit into the block.
	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		err := seg.writeInternal(data, ChunkTypeFull)
		if err != nil {
			return nil, err
		}
		position.ChunkSize = dataSize + chunkHeaderSize
		return position, nil
	}

	// If the size of the data exceeds the size of the block,
	// the data should be written to the block in batches.
	var leftSize = dataSize
	var blockCount uint32 = 0
	for leftSize > 0 {
		chunkSize := blockSize - seg.currentBlockSize - chunkHeaderSize
		if chunkSize > leftSize {
			chunkSize = leftSize
		}
		chunk := make([]byte, chunkSize)

		var end = dataSize - leftSize + chunkSize
		if end > dataSize {
			end = dataSize
		}

		copy(chunk[:], data[dataSize-leftSize:end])

		// write the chunks
		var err error
		if leftSize == dataSize {
			// First Chunk
			err = seg.writeInternal(chunk, ChunkTypeFirst)
		} else if leftSize == chunkSize {
			// Last Chunk
			err = seg.writeInternal(chunk, ChunkTypeLast)
		} else {
			// Middle Chunk
			err = seg.writeInternal(chunk, ChunkTypeMiddle)
		}
		if err != nil {
			return nil, err
		}
		leftSize -= chunkSize
		blockCount += 1
	}

	position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	return position, nil
}

func (seg *segment) writeInternal(data []byte, chunkType ChunkType) error {
	dataSize := uint32(len(data))
	buf := make([]byte, dataSize+chunkHeaderSize)

	// Length	2 Bytes	index:4-5
	binary.LittleEndian.PutUint16(buf[4:6], uint16(dataSize))
	// Type	1 Byte	index:6
	buf[6] = chunkType
	// data N Bytes index:7-end
	copy(buf[7:], data)
	// Checksum	4 Bytes index:0-3
	sum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], sum)

	// append to the file
	if _, err := seg.fd.Write(buf); err != nil {
		return err
	}

	if seg.currentBlockSize > blockSize {
		panic("wrong! can not exceed the block size")
	}

	// update the corresponding fields
	seg.currentBlockSize += dataSize + chunkHeaderSize
	// A new block
	if seg.currentBlockSize == blockSize {
		seg.currentBlockNumber += 1
		seg.currentBlockSize = 0
	}
	return nil
}

func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := seg.readInternal(blockNumber, chunkOffset)
	return value, err
}

func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		segSize   = seg.Size()
		nextChunk = &ChunkPosition{SegmentId: seg.id}
	)
	for {
		size := int64(blockSize)
		offset := int64(blockNumber * blockSize)
		if size+offset > segSize {
			size = segSize - offset
		}

		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		// read an entire block
		var block []byte
		var ok bool
		if seg.cache != nil {
			block, ok = seg.cache.Get(seg.getCacheKey(blockNumber))
		}
		// cache miss, read from the segment file
		if !ok || len(block) == 0 {
			block = make([]byte, size)
			_, err := seg.fd.ReadAt(block, offset)
			if err != nil {
				return nil, nil, err
			}
			// cache the block, so that the next time it can be read from the cache.
			// if the block size is smaller than blockSize, it means that the block is not full,
			// so we will not cache it.
			if seg.cache != nil && size == blockSize {
				seg.cache.Add(seg.getCacheKey(blockNumber), block)
			}
		}

		// header
		header := make([]byte, chunkHeaderSize)
		copy(header, block[chunkOffset:chunkOffset+chunkHeaderSize])

		// length
		length := binary.LittleEndian.Uint16(header[4:6])

		// copy data
		start := chunkOffset + chunkHeaderSize
		result = append(result, block[start:start+int64(length)]...)

		// check sum
		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		checksum := crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		// type
		chunkType := header[6]

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			// If this is the last chunk in the block, and the left block
			// space are paddings, the next chunk should be in the next block.
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

func (seg *segment) getCacheKey(blockNumber uint32) uint64 {
	return uint64(seg.id)<<32 | uint64(blockNumber)
}

func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	// The segment file is closed
	if segReader.segment.closed {
		return nil, nil, ErrClosed
	}

	// this position describes the current chunk info
	chunkPosition := &ChunkPosition{
		SegmentId:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}

	value, nextChunk, err := segReader.segment.readInternal(
		segReader.blockNumber,
		segReader.chunkOffset,
	)
	if err != nil {
		return nil, nil, err
	}

	// Calculate the chunk size.
	// Remember that the chunk size is just an estimated value,
	// not accurate, so don't use it for any important logic.
	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(segReader.blockNumber*blockSize + uint32(segReader.chunkOffset))

	// update the position
	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chunkOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}
