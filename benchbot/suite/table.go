package suite

import (
	"database/sql"
	"fmt"
	"math/rand"
)

const (
	asciiStart = int('a')
	asciiLimit = int('z')
)

func RandomAsciiBytes(r *rand.Rand, size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(r.Intn(asciiLimit-asciiStart) + asciiStart)
	}
	return data
}

func DropTable(db *sql.DB, tableName string) error {
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
	_, err := db.Exec(stmt)
	return err
}

func CreateTable(db *sql.DB, tableName, tableSchema string) error {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` %s", tableName, tableSchema)
	_, err := db.Exec(stmt)
	return err
}

func RecreateTable(db *sql.DB, tableName, tableSchema string) error {
	if err := DropTable(db, tableName); err != nil {
		return err
	}
	return CreateTable(db, tableName, tableSchema)
}
