package suite

import (
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/octopus/benchbot/backend"
)

const (
	createTestTable = `
CREATE TABLE IF NOT EXISTS statdb_test (
id INTEGER UNSIGNED NOT NULL,
name VARCHAR(32) DEFAULT '' NOT NULL,
age INTEGER DEFAULT 0
) ENGINE=innodb;
	`
)

func TestStatDB(t *testing.T) {
	var user, psw, host, dbName string = "root", "", "localhost", "test"
	var port uint16 = 3306

	rawDB, err := ConnectDB(user, psw, host, port, dbName)
	if err != nil {
		fmt.Println("DB connect failed !")
		return
	}

	rawDB.Exec("truncate table `statdb_test`")
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
