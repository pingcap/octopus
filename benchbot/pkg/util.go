package pkg

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func DumpJSON(obj interface{}, format bool) (string, error) {
	if data, err := json.Marshal(obj); err != nil {
		return "", err
	} else {
		if format {
			var out bytes.Buffer
			json.Indent(&out, data, "", "\t")
			return string(out.Bytes()), nil
		} else {
			return string(data), nil
		}
	}
}

func DeepClone(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func ConnectDB(user, passwd, host string, port int, dbname string) (db *sql.DB, err error) {
	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@(%s:%d)/%s", user, passwd, host, port, dbname))
	if err == nil {
		err = CheckConnection(db)
	}

	if err != nil && db != nil {
		db.Close()
		db = nil
	}

	return
}

func CheckConnection(db *sql.DB) error {
	if db == nil {
		return errors.New("no db connection")
	}

	var res int
	err := db.QueryRow("select 1").Scan(&res)
	if err == nil && res != 1 {
		err = errors.New("invalid db connection with incorrect data response")
	}

	return err
}
