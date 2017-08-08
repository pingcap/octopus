package suite

import (
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/pkg"
	"golang.org/x/net/context"
)

func TestBlockWrite(t *testing.T) {
	var user, passwd, host, port, dbname = "root", "", "localhost", 4000, "test"

	db, err := ConnectDB(user, passwd, host, port, dbname)
	if err != nil {
		log.Fatalf("failed to connect db: %s", err)
	}

	c := &BlockWriteConfig{
		NumThreads:   4,
		NumRequests:  8,
		MinBlockSize: 16,
		MaxBlockSize: 32,
	}

	s := NewBlockWriteSuite(c)
	if _, err := s.Run(context.Background(), db); err != nil {
		log.Fatalf("failed to run suite: %s", err)
	}
}
