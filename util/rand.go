package util

import _ "unsafe" // required by go:linkname

// Fastrand returns a lock free uint32 value.
//go:linkname Fastrand runtime.fastrand
func Fastrand() uint32
