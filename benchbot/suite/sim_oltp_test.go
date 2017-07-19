package suite

import (
	"fmt"
	"testing"
	_ "time"

	. "github.com/pingcap/octopus/benchbot/backend"
	"golang.org/x/net/context"
)

func TestSimpleOLTP(t *testing.T) {
	var user, psw, host, dbName string = "root", "", "localhost", "test"
	var port uint16 = 3306

	ctx, _ := context.WithCancel(context.Background())
	db, err := ConnectDB(user, psw, host, port, dbName)
	if err != nil {
		fmt.Println("DB connect failed !")
		return
	}

	// refresh
	for i := 0; i < simOLTPTables; i++ {
		table := fmt.Sprintf("%s_%d", defaultTableName, i)
		dropTable(db, table)
	}

	// run
	s := NewSimpleOLTPSuite()
	rets, err := s.Run(ctx, db)
	if err != nil {
		fmt.Println(err.Error())
	}

	for _, r := range rets {
		fmt.Println(r.Dump())
	}
}
