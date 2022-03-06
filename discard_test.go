package lotusdb

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestDiscard_listenUpdates(t *testing.T) {
}

func TestDiscard_incrTotalAndDiscard(t *testing.T) {
	dir, _ := ioutil.TempDir("", "lotusdb-discard-test")
	discard, err := newDiscard(dir, vlogDiscardName)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	type args struct {
		fid uint32
	}
	tests := []struct {
		name string
		d    *Discard
		args args
	}{
		{
			"total-fid-0", discard, args{fid: 0},
		},
		{
			"discard-fid-0", discard, args{fid: 0},
		},
		{
			"total-fid-1", discard, args{fid: 1},
		},
		{
			"discard-fid-1", discard, args{fid: 1},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if i%2 == 0 {
				tt.d.incrTotal(tt.args.fid)
			} else {
				tt.d.incrDiscard(tt.args.fid)
			}
		})
	}
}

func BenchmarkDiscard_incrTotalAndDiscard(b *testing.B) {
	dir, _ := ioutil.TempDir("", "lotusdb-discard-test")
	discard, err := newDiscard(dir, vlogDiscardName)
	assert.Nil(b, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i % 2 == 0 {
			discard.incr(uint32(i%255), true)
		} else {
			discard.incr(uint32(i%255), false)
		}
	}
}
