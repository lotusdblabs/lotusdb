package lotusdb

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
)

func TestEncodeDecodeValueLogRecord(t *testing.T) {
	// Example data
	key := []byte("mykey")
	value := []byte("myvalue")
	uuidVal := uuid.New()

	record := &ValueLogRecord{
		key:   key,
		value: value,
		uid:   uuidVal,
	}

	// Encode the record
	encoded := encodeValueLogRecord(record)

	// Decode the encoded record
	decoded := decodeValueLogRecord(encoded)

	// Compare original and decoded records
	if !bytes.Equal(record.key, decoded.key) {
		t.Errorf("Expected key %v, got %v", record.key, decoded.key)
	}

	if !bytes.Equal(record.value, decoded.value) {
		t.Errorf("Expected value %v, got %v", record.value, decoded.value)
	}

	if record.uid != decoded.uid {
		t.Errorf("Expected UUID %v, got %v", record.uid, decoded.uid)
	}
}
