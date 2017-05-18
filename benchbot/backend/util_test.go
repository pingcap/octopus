package backend

import (
	"fmt"
	"testing"
)

func TestUUID(t *testing.T) {
	fmt.Printf("%s\n", GenUUID())
}
