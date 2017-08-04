package suite

import (
	"testing"
	_ "time"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/pkg"
	"golang.org/x/net/context"
)

func TestSimpleOLTP(t *testing.T) {
	var user, passwd, host, port, dbname = "root", "", "localhost", 4000, "test"

	db, err := ConnectDB(user, passwd, host, port, dbname)
	if err != nil {
		log.Fatalf("failed to connect db: %s", err)
	}

	c := &SimpleOLTPConfig{
		TableSize:   4,
		NumTables:   4,
		NumThreads:  4,
		NumRequests: 32,
	}

	s := NewSimpleOLTPSuite(c)
	if _, err := s.Run(context.Background(), db); err != nil {
		log.Fatalf("failed to run suite: %s", err)
	}
}
