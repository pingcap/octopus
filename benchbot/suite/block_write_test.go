package suite

import (
	"log"
	"testing"
	"time"

	. "github.com/pingcap/octopus/benchbot/common"
)

func TestBlockWrite(t *testing.T) {
	db := MustConnectTestDB()

	cfg := &BlockWriteConfig{
		Duration:     Duration{Duration: time.Second * 10},
		NumThreads:   3,
		MinBlockSize: 8,
		MaxBlockSize: 10,
	}

	s := NewBlockWriteSuite(cfg)
	if _, err := s.run(db); err != nil {
		log.Fatal(err)
	}
}
