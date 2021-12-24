package lotusdb

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrLockWaitTimeout  = errors.New("lock wait timeout")
	ErrInvalidStripeIdx = errors.New("stripe index is invalid")
)

const (
	maxStripeNum = 1 << 16
)

type (
	LockMgr struct {
		lockMaps     map[int]*LockMap
		mapStripeNum int
		sync.Mutex
	}

	LockMap struct {
		stripes   []*LockMapStripe
		stripeNum int
	}

	LockMapStripe struct {
		chn     chan struct{}
		keys    map[uint64]*LockInfo
		waiters waitersMap
	}

	LockInfo struct {
		exclusive bool
		txnIds    map[uint64]struct{}
	}

	waitersMap map[uint64]map[uint64]chan struct{}
)

func NewLockManager(stripeNum int) *LockMgr {
	num := stripeNumFor(stripeNum)
	return &LockMgr{
		lockMaps:     make(map[int]*LockMap),
		mapStripeNum: num,
	}
}

func newLockMap(stripeNum int) *LockMap {
	m := &LockMap{
		stripes:   make([]*LockMapStripe, stripeNum),
		stripeNum: stripeNum,
	}
	for i := range m.stripes {
		m.stripes[i] = newLockMapStripe()
	}
	return m
}

func newLockMapStripe() *LockMapStripe {
	chn := make(chan struct{}, 1)
	chn <- struct{}{}
	return &LockMapStripe{
		keys: make(map[uint64]*LockInfo),
		chn:  chn,
	}
}

// TryLockKey 解锁位置要再看看
func (lm *LockMgr) TryLockKey(txnId uint64, cfId int, key uint64, timeout time.Duration, exclusive bool) error {
	lockMap := lm.getLockMap(cfId)
	stripe := lm.getMapStripe(lockMap, key)

	// must hold stripe`s mutex.
	now := time.Now()
	if locked := stripe.lockTimeout(timeout); !locked {
		return ErrLockWaitTimeout
	}

	// fast path, acquire lock immediately.
	acquied := lm.acquireLock(stripe, txnId, key, exclusive)
	if acquied {
		return nil
	}

	spent := time.Now().Sub(now)
	timeout -= spent

	// 拿不到，并且还有超时时间
	if timeout != 0 {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()

		var acquired bool
		for !acquired {
			select {
			case <-ctx.Done():
				return ErrLockWaitTimeout
			default:
				waiter := make(chan struct{})
				stripe.waiters[key][txnId] = waiter
				stripe.unLock()

				// wait until timeout.
				select {
				case <-time.After(timeout):
					return ErrLockWaitTimeout
				case <-waiter:
				}

				// try to acquire lock again, must hold stripe`s mutex.
				lock := stripe.lockTimeout(timeout)
				if lock {
					acquied = lm.acquireLock(stripe, txnId, key, exclusive)
				}
			}
		}
		return nil
	}
	return ErrLockWaitTimeout
}

func (lm *LockMgr) UnlockKey(txnId uint64, cfId int, key uint64) {
	lockMap := lm.getLockMap(cfId)
	stripe := lm.getMapStripe(lockMap, key)

	stripe.lock()
	lockInfo := stripe.keys[key]
	if lockInfo == nil {
		stripe.unLock()
		return
	}
	delete(lockInfo.txnIds, txnId)
	var waiters []chan struct{}
	if len(lockInfo.txnIds) == 0 || lockInfo.exclusive {
		for _, ch := range stripe.waiters[key] {
			waiters = append(waiters, ch)
		}
	} else {
		waiters = append(waiters, stripe.waiters[key][txnId])
	}
	stripe.unLock()

	// notify
	for _, w := range waiters {
		close(w)
	}
}

func (lm *LockMgr) getLockMap(cfId int) *LockMap {
	lm.Lock()
	defer lm.Unlock()
	if lm.lockMaps[cfId] == nil {
		lm.lockMaps[cfId] = newLockMap(lm.mapStripeNum)
	}
	return lm.lockMaps[cfId]
}

func (lm *LockMgr) getMapStripe(lockMap *LockMap, key uint64) *LockMapStripe {
	sn := uint64(lockMap.stripeNum)
	stripeIdx := key & (sn - 1)
	if stripeIdx < 0 || stripeIdx >= uint64(lockMap.stripeNum) {
		panic(ErrInvalidStripeIdx.Error())
	}

	return lockMap.stripes[stripeIdx]
}

func (lm *LockMgr) acquireLock(stripe *LockMapStripe, txnId, key uint64, exclusive bool) bool {
	lkInfo := stripe.keys[key]
	if lkInfo == nil {
		// 拿到锁，直接返回
		txnIds := make(map[uint64]struct{})
		txnIds[txnId] = struct{}{}
		lkInfo := &LockInfo{
			exclusive: exclusive,
			txnIds:    txnIds,
		}
		stripe.keys[key] = lkInfo
		return true
	}
	if !exclusive && !lkInfo.exclusive {
		// 共享锁能拿到
		lkInfo.txnIds[txnId] = struct{}{}
		stripe.keys[key] = lkInfo
		return true
	}
	return false
}

func (sp *LockMapStripe) lockTimeout(timeout time.Duration) bool {
	select {
	case <-sp.chn:
		// get lock success.
		return true
	case <-time.After(timeout):
		return false
	}
}

func (sp *LockMapStripe) lock() {
	<-sp.chn
}

func (sp *LockMapStripe) unLock() {
	select {
	case sp.chn <- struct{}{}:
	default:
		panic("unlock of unlocked mutex")
	}
}

func stripeNumFor(stripeNum int) int {
	n := stripeNum - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return 1
	} else if n >= maxStripeNum {
		return maxStripeNum
	} else {
		return n + 1
	}
}
