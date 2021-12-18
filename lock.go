package lotusdb

import "time"

type (
	LockMgr struct {
	}

	LockMap struct {
	}

	LockMapStripe struct {
		keys map[uint64]*LockInfo
	}

	LockInfo struct {
		exclusive bool
		expiredAt time.Time
	}
)

func (lm *LockMgr) TryLock(cfId uint64, key uint64) {

}
