package lotusdb

import "sync/atomic"

type ioState struct {
	trigger     bool  // if true, monitoring io state
	wtState     int32 // current write status, used to assist compaction
	rdState     int32 // current read status, used to assist compaction
	wtThreshold int32 // write IO threshold, exceeding it indicates current busy
	rdThreshold int32 // read IO threshold, exceeding it indicates current busy
}

func newioState(trigger bool, wtThreshold int32, rdThreshold int32) (ioState,error) {
	return ioState{
		trigger: trigger,
		wtThreshold: wtThreshold,
		rdThreshold: rdThreshold,
	},nil
}

func (io ioState) LogWrite(count int) {
	if io.trigger {
		atomic.AddInt32(&io.wtState, int32(count))
	}
}

func (io ioState) DelogWrite(count int) {
	if io.trigger {
		atomic.AddInt32(&io.wtState, -1*int32(count))
	}
}

func (io ioState) LogRead(count int) {
	if io.trigger {
		atomic.AddInt32(&io.rdState, int32(count))
	}
}

func (io ioState) DelogRead(count int32) {
	if io.trigger {
		atomic.AddInt32(&io.rdState, -1*int32(count))
	}
}

func (io ioState) checkFree() bool {
	if io.trigger {
		println("wtState:",atomic.LoadInt32(&io.wtState),"rdState:",atomic.LoadInt32(&io.rdState))
		if atomic.LoadInt32(&io.wtState) > io.wtThreshold || atomic.LoadInt32(&io.rdState) > io.rdThreshold {
			return false
		}
	}
	return true
}
