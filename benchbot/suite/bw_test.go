package suite

import (
	"fmt"
	"golang.org/x/net/context"
	"testing"

	. "github.com/pingcap/octopus/benchbot/backend"
)

func TestBlockWrite(t *testing.T) {
	var user, psw, host, dbName string = "root", "", "localhost", "test"
	var port uint16 = 3306

	ctx, _ := context.WithCancel(context.Background())
	db, err := ConnectDB(user, psw, host, port, dbName)
	if err != nil {
		fmt.Println("DB connect failed !")
		return
	}

	s := NewBlockWriteSuite()
	results, _ := s.Run(ctx, db)
	fmt.Println(len(results))
	for _, r := range results {
		fmt.Println(r.Dump())
	}
}
