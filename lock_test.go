package lotusdb

import (
	"testing"
	"time"

	"github.com/flower-corp/lotusdb/util"
	"github.com/stretchr/testify/assert"
)

func TestNewLockManager(t *testing.T) {
	lockMgr := NewLockManager(16)
	t.Log(lockMgr)

	key := []byte("test-key")
	h := util.MemHash(key)
	err := lockMgr.TryLockKey(1, 1, h, time.Millisecond*100, true)
	assert.Nil(t, err)

	go func() {
		time.Sleep(time.Millisecond * 900)
		t.Log("unlock...")
		lockMgr.UnlockKey(1, 1, h)
	}()

	err = lockMgr.TryLockKey(1, 1, h, time.Second*1, false)
	t.Log(err)
}
