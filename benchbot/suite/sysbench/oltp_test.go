package sysbench

import (
	"fmt"
	"testing"

	"github.com/ngaut/log"

	. "github.com/pingcap/octopus/benchbot/common"
	. "github.com/pingcap/octopus/benchbot/suite"
)

func TestSysbenchOLTP(t *testing.T) {
	db := MustConnectTestDB()

	cfg := &SysbenchOLTPConfig{
		TableSize:   10,
		NumTables:   3,
		NumThreads:  3,
		NumRequests: 20,
	}

	for i := 0; i < cfg.TableSize; i++ {
		DropTable(db, fmt.Sprintf("%s_%d", sysbenchTableName, i))
	}

	s := NewSysbenchOLTPSuite(cfg)
	if _, err := s.run(db); err != nil {
		log.Fatal(err)
	}
}
