package lotusdb

import (
	"testing"

	"github.com/google/uuid"
)

func TestAddEntry(t *testing.T) {
	dt := newDeprecatedTable(0)
	uidNumber := 3
	count := 4

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
	dt := newDeprecatedTable(0)
	uidNumber := 3
	count := 4

	for i := 0; i < count; i++ {
		for j := 0; j < uidNumber; j++ {
			uid := uuid.New()
			dt.addEntry(uid)
			if !dt.existEntry(uid) {
				t.Errorf("expected entry not exist!")
			}
		}
	}
	if (int)(dt.size) != count*uidNumber {
		t.Errorf("expected dt.size to be %d, got %d", count, dt.size)
	}
}
