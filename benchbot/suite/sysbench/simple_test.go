package sysbench

import (
	"log"
	"testing"

	. "github.com/pingcap/octopus/benchbot/common"
)

func TestSysbenchSimple(t *testing.T) {
	db := MustConnectTestDB()

	cfg := &SysbenchSimpleConfig{
		TableSize:  10,
		NumTables:  4,
		NumThreads: 3,
		NumSelects: 20,
		NumInserts: 20,
		NumDeletes: 20,
	}

	s := NewSysbenchSimpleSuite(cfg)
	if _, err := s.run(db); err != nil {
		log.Fatal(err)
	}
}
