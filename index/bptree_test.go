package index

import (
	"reflect"
	"testing"
)

func TestNewBPTree(t *testing.T) {
	type args struct {
		opt *BPTreeOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *BPTree
		wantErr bool
	}{
		{},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewBPTree(tt.args.opt)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBPTree() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBPTree() got = %v, want %v", got, tt.want)
			}
		})
	}
}
