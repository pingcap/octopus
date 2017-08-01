package suite

import (
	"fmt"
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/pkg"
)

func TestPrepare(t *testing.T) {
	var user, passwd, host, port, dbname = "root", "", "localhost", 4000, "test"

	db, err := ConnectDB(user, passwd, host, port, dbname)
	if err != nil {
		log.Fatalf("failed to connect db: %s", err)
	}

	tableSize := 100
	numTables := 4
	numThreads := 4

	for i := 0; i < numTables; i++ {
		table := fmt.Sprintf("%s_%d", defaultTableName, i)
		dropTable(db, table)
	}

	tables, err := prepare(db, tableSize, numTables, numThreads)
	if err != nil {
		log.Fatalf("failed to prepare: %s", err)
	}

	fmt.Println(tables)
}
