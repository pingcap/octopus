package suite

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/pkg"
)

const (
	createTestTable = `
    CREATE TABLE IF NOT EXISTS statdb_test (
        id INTEGER UNSIGNED NOT NULL,
        name VARCHAR(32) DEFAULT '' NOT NULL,
        age INTEGER DEFAULT 0
    ) ENGINE=innodb;`
)

func TestStatDB(t *testing.T) {
	var user, passwd, host, port, dbname = "root", "", "localhost", 4000, "test"

	rawDB, err := ConnectDB(user, passwd, host, port, dbname)
	if err != nil {
		log.Fatalf("failed to connect db: %s", err)
	}

	rawDB.Exec("TRUNCATE TABLE `statdb_test`")
	rawDB.Exec(createTestTable)

	db := NewStatDB(rawDB)
	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			var name string
			var err error

			// write x 10
			for j := 0; j < 10; j++ {
				name = fmt.Sprintf("name.%d_%d", i, j)
				_, err = db.Exec("Insert into `statdb_test`(`id`, `name`, `age`) VALUES (?,?,?)", i, name, j)
				if err != nil {
					fmt.Println(err.Error())
				}
			}

			// read x 1
			rows, _ := db.Query("Select * from `statdb_test` where id = ?", i)

			var id, age int
			for rows.Next() {
				if err = rows.Scan(&id, &name, &age); err != nil {
					fmt.Println(err.Error())
				}
			}
			rows.Close()

			wg.Done()
		}()
	}

	wg.Wait()
	db.Close()
	rawDB.Close()
	fmt.Printf("(cost = %.2f sec)\n", time.Now().Sub(start).Seconds())

	res := db.Stat()
	fmt.Println(res.summary.formatHuman())
	fmt.Println(res.summary.FormatJSON())
}
