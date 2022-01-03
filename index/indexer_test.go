package index

import (
	"reflect"
	"testing"
)

func TestEncodeMeta(t *testing.T) {
	type args struct {
		m *IndexerMeta
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			"nil", args{m: &IndexerMeta{Value: nil, Fid: 0, Size: 3932, Offset: 98}}, []byte{0, 184, 61, 196, 1},
		},
		{
			"0", args{m: &IndexerMeta{Value: []byte(""), Fid: 0, Size: 0, Offset: 0}}, []byte{0, 0, 0},
		},
		{
			"1", args{m: &IndexerMeta{Value: []byte("1"), Fid: 0, Size: 0, Offset: 0}}, []byte{0, 0, 0, 49},
		},
		{
			"many", args{m: &IndexerMeta{Value: []byte("lotusdb"), Fid: 0, Size: 0, Offset: 0}}, []byte{0, 0, 0, 108, 111, 116, 117, 115, 100, 98},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EncodeMeta(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeMeta() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeMeta(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name string
		args args
		want *IndexerMeta
	}{
		{
			"nil", args{buf: []byte{0, 184, 61, 196, 1}}, &IndexerMeta{Value: []byte(""), Fid: 0, Size: 3932, Offset: 98},
		},
		{
			"0", args{buf: []byte{0, 0, 0}}, &IndexerMeta{Value: []byte(""), Fid: 0, Size: 0, Offset: 0},
		},
		{
			"1", args{buf: []byte{0, 0, 0, 48}}, &IndexerMeta{Value: []byte("0"), Fid: 0, Size: 0, Offset: 0},
		},
		{
			"many", args{buf: []byte{0, 0, 0, 108, 111, 116, 117, 115, 100, 98}}, &IndexerMeta{Value: []byte("lotusdb"), Fid: 0, Size: 0, Offset: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DecodeMeta(tt.args.buf); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeMeta() = %v, want %v", got, tt.want)
			}
		})
	}
}
