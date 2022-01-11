package logfile

import (
	"reflect"
	"testing"
)

func TestEncodeVlogEntry(t *testing.T) {
	type args struct {
		ve *VlogEntry
	}
	tests := []struct {
		name  string
		args  args
		want  []byte
		want1 int
	}{
		{
			"nil", args{ve: nil}, nil, 0,
		},
		{
			"no-key-value", args{ve: &VlogEntry{}}, []byte{0, 0}, 2,
		},
		{
			"no-key", args{ve: &VlogEntry{Value: []byte("1")}}, []byte{0, 2, 49}, 3,
		},
		{
			"no-value", args{ve: &VlogEntry{Key: []byte("1")}}, []byte{2, 0, 49}, 3,
		},
		{
			"normal", args{ve: &VlogEntry{Key: []byte("1"), Value: []byte("lotusdb")}}, []byte{2, 14, 49, 108, 111, 116, 117, 115, 100, 98}, 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := EncodeVlogEntry(tt.args.ve)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeVlogEntry() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("EncodeVlogEntry() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestDecodeVlogEntry(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name string
		args args
		want *VlogEntry
	}{
		{
			"nil", args{b: nil}, &VlogEntry{},
		},
		{
			"no-fields", args{b: []byte{}}, &VlogEntry{Key: []byte{}, Value: []byte{}},
		},
		{
			"no-key", args{b: []byte{0, 2, 49}}, &VlogEntry{Key: []byte{}, Value: []byte("1")},
		},
		{
			"no-value", args{b: []byte{2, 0, 49}}, &VlogEntry{Key: []byte("1"), Value: []byte{}},
		},
		{
			"normal", args{b: []byte{2, 14, 49, 108, 111, 116, 117, 115, 100, 98}}, &VlogEntry{Key: []byte("1"), Value: []byte("lotusdb")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DecodeVlogEntry(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeVlogEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}
