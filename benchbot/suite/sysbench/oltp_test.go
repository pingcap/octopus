package sysbench

import (
	"log"
	"testing"

	. "github.com/pingcap/octopus/benchbot/common"
)

func TestSysbenchOLTP(t *testing.T) {
	db := MustConnectTestDB()

	cfg := &SysbenchOLTPConfig{
		TableSize:   10,
		NumTables:   3,
		NumThreads:  3,
		NumRequests: 20,
	}

	s := NewSysbenchOLTPSuite(cfg)
	if _, err := s.run(db); err != nil {
		log.Fatal(err)
	}
}
