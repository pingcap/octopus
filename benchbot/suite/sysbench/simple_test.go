package sysbench

import (
	"fmt"
	"testing"

	"github.com/ngaut/log"

	. "github.com/pingcap/octopus/benchbot/common"
	. "github.com/pingcap/octopus/benchbot/suite"
)

func TestSysbenchSimple(t *testing.T) {
	db := MustConnectTestDB()

	cfg := &SysbenchSimpleConfig{
		TableSize:   10,
		NumTables:   4,
		NumThreads:  3,
		NumRequests: 40,
	}

	for i := 0; i < cfg.TableSize; i++ {
		DropTable(db, fmt.Sprintf("%s_%d", sysbenchTableName, i))
	}

	s := NewSysbenchSimpleSuite(cfg)
	if _, err := s.run(db); err != nil {
		log.Fatal(err)
	}
}
