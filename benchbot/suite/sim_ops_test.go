package suite

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/backend"
	"golang.org/x/net/context"
)

func TestSimulateCases(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var user, psw, host, dbName string = "root", "", "172.16.10.2", "test"
	var port uint16 = 4000

	db, err := ConnectDB(user, psw, host, port, dbName)
	if err != nil {
		fmt.Println("DB connect failed !")
		return
	}

	db.SetMaxIdleConns(1001)
	db.SetMaxOpenConns(1001)
	db.SetConnMaxLifetime(-1)

	cases := []BenchCase{
		NewSimpleGetCase(),
	}

	for i, c := range cases {
		log.Infof("[%s] initialize ...", c.name)
		c.Initialize(nil, db)

		s = time.Now()
		log.Infof("[%s] running ...", c.name)
		c.Run(nil, db)
		e = time.Now()

		fmt.Println(c.Result().Summary.FormatJSON())
		fmt.Printf("run cost : %.2f sec\n", e.Sub(s).Seconds())
	}
}

func TestSimpleOperation(t *testing.T) {
	var user, psw, host, dbName string = "root", "", "localhost", "test"
	var port uint16 = 3306

	ctx, _ := context.WithCancel(context.Background())
	db, err := ConnectDB(user, psw, host, port, dbName)
	if err != nil {
		fmt.Println("DB connect failed !")
		return
	}

	// refresh
	for i := 0; i < simOpsTables; i++ {
		table := fmt.Sprintf("%s_%d", defaultTableName, i)
		dropTable(db, table)
	}

	// run
	s := NewSimpleOperationSuite()
	rets, err := s.Run(ctx, db)
	if err != nil {
		fmt.Println(err.Error())
	}

	for _, r := range rets {
		fmt.Println(r.Dump())
	}
}
