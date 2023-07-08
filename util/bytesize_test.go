/*
 * @Author: 27
 * @LastEditors: 27
 * @Date: 2023-07-09 01:05:27
 * @LastEditTime: 2023-07-09 01:33:27
 * @FilePath: /lotusdb-learn/util/bytesize_test.go
 * @description: type some description
 */

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var unitsTestCase = []struct {
	name                string
	unit                byteSize
	expectDisplayString string
	errMsg              string
}{
	{"KB-Test: ", KB, "1.00KB", "1KB not display 1.00KB"},
	{"MB-Test: ", MB, "1.00MB", "1MB not display 1.00MB"},
	{"GB-Test: ", GB, "1.00GB", "1GB not display 1.00GB"},
	{"TB-Test: ", TB, "1.00TB", "1TB not display 1.00TB"},
	{"PB-Test: ", PB, "1.00PB", "1PB not display 1.00PB"},
	{"EB-Test: ", EB, "1.00EB", "1EB not display 1.00EB"},
	{"ZB-Test: ", ZB, "1.00ZB", "1ZB not display 1.00ZB"},
	{"YB-Test: ", YB, "1.00YB", "1YB not display 1.00YB"},
}

func TestString(t *testing.T) {
	for _, tc := range unitsTestCase {
		require.Equal(t, tc.expectDisplayString, tc.unit.String(), tc.errMsg)
	}

}
