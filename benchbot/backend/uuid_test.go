package backend

import (
	"testing"
)

func TestIncreasingUUID(t *testing.T) {
	c := NewUUIDCenter()

	var uuid, i, n int64 = 0, 0, 10
	for ; i < n; i++ {
		uuid = c.ApplyNext("utest")
	}

	if uuid != n {
		t.Error("not match : ", n, " vs ", uuid)
	}
}
