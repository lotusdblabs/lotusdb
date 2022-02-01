/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arenaskl

import (
	"math"
	"sync/atomic"
	"unsafe"
)

// Align requested alignment.
type Align uint8

const (
	// Align1 align 0.
	Align1 = 0
	// Align8 align 7
	Align8 = 7
)

// Arena should be lock-free.
type Arena struct {
	n   uint64
	buf []byte
}

// NewArena allocates a new arena of the specified size and returns it.
func NewArena(size uint32) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	out := &Arena{
		n:   1,
		buf: make([]byte, size),
	}
	return out
}

// Size size have been allocated.
func (a *Arena) Size() uint32 {
	s := atomic.LoadUint64(&a.n)
	if s > math.MaxUint32 {
		// Saturate at MaxUint32.
		return math.MaxUint32
	}
	return uint32(s)
}

// Cap capacity of arena buffer.
func (a *Arena) Cap() uint32 {
	return uint32(len(a.buf))
}

// Reset reset size of arena.
func (a *Arena) Reset() {
	atomic.StoreUint64(&a.n, 1)
}

// Alloc allocate memory buffer by the given size.
func (a *Arena) Alloc(size, overflow uint32, align Align) (uint32, error) {
	// Verify that the arena isn't already full.
	origSize := atomic.LoadUint64(&a.n)
	if int(origSize) > len(a.buf) {
		a.growBufSize(origSize - uint64(len(a.buf)))
	}

	// Pad the allocation with enough bytes to ensure the requested alignment.
	padded := size + uint32(align)

	// Use 64-bit arithmetic to protect against overflow.
	newSize := atomic.AddUint64(&a.n, uint64(padded))
	if int(newSize)+int(overflow) > len(a.buf) {
		a.growBufSize(uint64(padded + overflow))
	}

	// Return the aligned offset.
	offset := (uint32(newSize) - padded + uint32(align)) & ^uint32(align)
	return offset, nil
}

// GetBytes returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta bytes.
func (a *Arena) GetBytes(offset uint32, size uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+size]
}

// GetPointer get pointer at offset.
func (a *Arena) GetPointer(offset uint32) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	return unsafe.Pointer(&a.buf[offset])
}

// GetPointerOffset get the pointer offset in buffer.
func (a *Arena) GetPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}
	return uint32(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}

func (a *Arena) growBufSize(growBy uint64) {
	newBuf := make([]byte, uint64(len(a.buf))+growBy)
	copy(newBuf, a.buf)
	a.buf = newBuf
}
