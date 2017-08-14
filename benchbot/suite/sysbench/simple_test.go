package sysbench

import (
	"testing"

	"github.com/ngaut/log"

	. "github.com/pingcap/octopus/benchbot/common"
)

func TestSysbenchSimple(t *testing.T) {
	db := MustConnectTestDB()

	cfg := &SysbenchSimpleConfig{
		TableSize:   10,
		NumTables:   4,
		NumThreads:  3,
		NumRequests: 40,
	}

	s := NewSysbenchSimpleSuite(cfg)
	if _, err := s.run(db); err != nil {
		log.Fatal(err)
	}
}
