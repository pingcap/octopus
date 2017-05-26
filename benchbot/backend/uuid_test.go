package backend

import (
	"testing"
)

func TestIncreasingUUID(t *testing.T) {
	c := NewUUIDAllocator()

	var uuid, i, n int64 = 0, 0, 10
	for ; i < n; i++ {
		uuid = c.Gen("utest")
	}

	if uuid != n {
		t.Error("not match : ", n, " vs ", uuid)
	}
}
