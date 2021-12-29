package boltdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBboltdb(t *testing.T) {
	db, err := NewBboltdb(&BboltdbConfig{
		ReadIndexComponentType: "bolt",
		BatchSize:              10,
		MaxDataSize:            100000,
		DBName:                 "test_main",
		FilePath:               "./test_db",
		BucketName:             []byte("test"),
	})

	require.NoError(t, err)
	require.NotNil(t, db)
}
