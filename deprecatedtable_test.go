package lotusdb

import (
	"testing"

	"github.com/google/uuid"
)

func TestAddEntry(t *testing.T) {
	options := deprecatedtableOptions{
		capacity: 32,
	}
	dt := newDeprecatedTable(0, options)
	uidNumber := 3
	count := ((int)(options.capacity) - 4) / uidNumber
	
	for i := 0; i < count; i++ {
		for j := 0; j < uidNumber; j++ {
			uid := uuid.New()
			dt.addEntry(string(rune(i)), uid)
		}
    }
	if (int)(dt.size) != count * uidNumber {
		t.Errorf("expected dt.size to be %d, got %d",count, dt.size)
	}
}

func TestIsFull(t *testing.T) {
	options := deprecatedtableOptions{
		capacity: 32,
	}
	dt := newDeprecatedTable(0, options)
	count := 31
	for i := 0; i < count; i++ {
        uid := uuid.New()
		dt.addEntry(string(rune(i)),uid)
    }
	if dt.isFull() {
		t.Errorf("expected not full dt.size:%d, capacity:%d",dt.size, dt.options.capacity)
	}
	uid := uuid.New()
	dt.addEntry(string(rune(0)),uid)
	if !dt.isFull() {
		t.Errorf("expected full dt.size:%d, capacity:%d",dt.size, dt.options.capacity)
	}
}

func TestUuidExist(t *testing.T) {
	options := deprecatedtableOptions{
		capacity: 32,
	}
	dt := newDeprecatedTable(0, options)
	uidNumber := 3
	count := ((int)(options.capacity) - 4) / uidNumber
	
	for i := 0; i < count; i++ {
		for j := 0; j < uidNumber; j++ {
			uid := uuid.New()
			dt.addEntry(string(rune(i)),uid)
			if !dt.existEntry(string(rune(i)),uid) {
				t.Errorf("expected entry not exist! dt.size:%d, capacity:%d",dt.size, dt.options.capacity)
			}
		}
    }
	if (int)(dt.size) != count * uidNumber {
		t.Errorf("expected dt.size to be %d, got %d",count, dt.size)
	}
}


