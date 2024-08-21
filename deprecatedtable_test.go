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
			dt.addEntry(uid)
		}
	}
	if (int)(dt.size) != count*uidNumber {
		t.Errorf("expected dt.size to be %d, got %d", count, dt.size)
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
			dt.addEntry(uid)
			if !dt.existEntry(uid) {
				t.Errorf("expected entry not exist! dt.size:%d, capacity:%d", dt.size, dt.options.capacity)
			}
		}
	}
	if (int)(dt.size) != count*uidNumber {
		t.Errorf("expected dt.size to be %d, got %d", count, dt.size)
	}
}
