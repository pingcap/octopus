package suite

import (
	"fmt"
	"testing"

	. "github.com/pingcap/octopus/benchbot/backend"
)

func testPrepare(t *testing.T) {
	var user, psw, host, dbName string = "root", "", "localhost", "test"
	var port uint16 = 3306

	db, err := ConnectDB(user, psw, host, port, dbName)
	if err != nil {
		fmt.Println("DB connect failed !")
		return
	}

	tableCount := 35
	tableRows := 10

	for i := 0; i < tableCount; i++ {
		table := fmt.Sprintf("%s_%d", defaultTableName, i)
		dropTable(db, table)
	}
	prepare(db, tableCount, tableRows)
}
