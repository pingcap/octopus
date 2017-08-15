package suite

import (
	"log"
	"testing"

	. "github.com/pingcap/octopus/benchbot/common"
)

func TestBlockWrite(t *testing.T) {
	db := MustConnectTestDB()

	cfg := &BlockWriteConfig{
		NumThreads:   3,
		NumRequests:  10,
		MinBlockSize: 8,
		MaxBlockSize: 10,
	}

	s := NewBlockWriteSuite(cfg)
	if _, err := s.run(db); err != nil {
		log.Fatal(err)
	}
}
