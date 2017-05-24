package backend

import (
	"errors"
	"strconv"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

type DataStore struct {
	db  *leveldb.DB
	mux sync.Mutex
}

func NewDataStore(path string) (*DataStore, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	store := &DataStore{db: db}
	return store, nil
}

func (d *DataStore) AutoIncr(field string) (int64, error) {
	// var t *leveldb.Transaction = db.OpenTransaction()
	// defer t.Commit()
	d.mux.Lock()
	defer d.mux.Unlock()

	var num int64
	var key = []byte(field)

	val, _ := d.db.Get(key, nil)
	if val == nil {
		val = []byte("0")
	}

	if n, err := strconv.ParseInt(string(val), 10, 64); err != nil {
		return -1, errors.New("not digital value !")
	} else {
		num = n + 1
		d.db.Put(key, []byte(strconv.FormatInt(num, 10)), nil)
	}

	return num, nil
}

func (d *DataStore) Close() error {
	return d.db.Close()
}
