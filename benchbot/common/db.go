package pkg

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func ConnectDB(user, passwd, host string, port int, dbname string) (*sql.DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s:%d)/%s", user, passwd, host, port, dbname))
	if err != nil {
		return nil, err
	}
	if err := CheckConnection(db); err != nil {
		db.Close()
		return nil, err
	}
	db.SetMaxIdleConns(1024)
	db.SetMaxOpenConns(1024)
	db.SetConnMaxLifetime(0)
	return db, nil
}

func CheckConnection(db *sql.DB) error {
	var res int
	if err := db.QueryRow("select 1").Scan(&res); err != nil {
		return err
	}
	if res != 1 {
		return errors.New("invalid db connection with incorrect data response")
	}
	return nil
}

func MustConnectTestDB() *sql.DB {
	db, err := ConnectDB("root", "", "127.0.0.1", 4000, "test")
	if err != nil {
		log.Fatal(err)
	}
	return db
}
