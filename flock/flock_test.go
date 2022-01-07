package flock

import (
	"testing"
	"time"
)

func TestAcquireFileLock(t *testing.T) {
	acquire := func() {
		path := "/tmp/FLOCK"
		_, err := AcquireFileLock(path, false)
		if err != nil {
			t.Log("acquire flock err.", err)
		} else {
			t.Log("acquire flock success")
		}
	}

	go acquire()
	go acquire()
	go acquire()

	time.Sleep(time.Minute)
}
