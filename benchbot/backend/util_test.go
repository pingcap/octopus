package backend

import (
	//"fmt"
	"testing"
)

func TestUUID(t *testing.T) {
	cnt := 10
	rets := make(map[string]int)
	for i := 0; i < cnt; i++ {
		rets[string(i)] = i
	}
	if len(rets) != 10 {
		t.Errorf("failed !")
	}
}
