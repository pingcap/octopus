package backend

import (
	"sync"
)

type UUIDCenter struct {
	uuids map[string]int64
	mux   sync.Mutex
}

func NewUUIDCenter() *UUIDCenter {
	return &UUIDCenter{
		uuids: make(map[string]int64),
	}
}

func (c *UUIDCenter) ApplyNext(name string) int64 {
	c.mux.Lock()
	defer c.mux.Unlock()

	// TODO ... replace by another smart way

	var uuid int64 = -1
	if v, exists := c.uuids[name]; !exists {
		uuid = 1
	} else {
		uuid = v + 1
	}

	c.uuids[name] = uuid

	return uuid
}
