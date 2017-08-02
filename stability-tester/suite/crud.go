// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package suite

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
)

var errNotExist = errors.New("the request row does not exist")

// CRUDCase simulates a typical CMS system that contains Users and Posts.
type CRUDCase struct {
	sync.Mutex
	cfg     *config.CRUDCaseConfig
	userIDs *idList
	postIDs *idList
	rnd     *rand.Rand
}

// NewCRUDCase creates a CRUDCase.
func NewCRUDCase(cfg *config.Config) Case {
	return &CRUDCase{
		cfg:     &cfg.Suite.CRUD,
		userIDs: newIDList(),
		postIDs: newIDList(),
		rnd:     rand.New(rand.NewSource(time.Now().Unix())),
	}
}

func (c *CRUDCase) Initialize(ctx context.Context, db *sql.DB) error {
	mustExec(db, "DROP TABLE IF EXISTS crud_users, crud_posts")
	mustExec(db, "CREATE TABLE crud_users (id BIGINT PRIMARY KEY, name VARCHAR(16), posts BIGINT)")
	mustExec(db, "CREATE TABLE crud_posts (id BIGINT PRIMARY KEY, author BIGINT, title VARCHAR(128))")
	return nil
}

func (c *CRUDCase) String() string {
	return "crud"
}

func (c *CRUDCase) Execute(db *sql.DB, index int) error {
	// CRUDCase does not support multithreading.
	if index != 0 {
		return nil
	}

	var newUsers, deleteUsers, newPosts, newAuthors, deletePosts []int64
	defer func() {
		log.Infof("[crud] newUsers %v, deleteUsers %v, newPosts %v, newAuthors %v, deletePosts %v", newUsers, deleteUsers, newPosts, newAuthors, deletePosts)
	}()

	// Add new users.
	for i := 0; i < c.cfg.UpdateUsers && c.userIDs.len() < c.cfg.UserCount+c.cfg.UpdateUsers; i++ {
		id := c.userIDs.allocID()
		if err := c.createUser(db, id); err != nil {
			return errors.Trace(err)
		}
		newUsers = append(newUsers, id)
	}

	// Delete random users.
	for i := 0; i < c.cfg.UpdateUsers && c.userIDs.len() > c.cfg.UserCount; i++ {
		id, ok := c.userIDs.randomID()
		if !ok {
			break
		}
		if err := c.deleteUser(db, id); err != nil {
			return errors.Trace(err)
		}
		deleteUsers = append(deleteUsers, id)
	}

	// Add new posts.
	for i := 0; i < c.cfg.UpdatePosts && c.postIDs.len() < c.cfg.PostCount+c.cfg.UpdatePosts; i++ {
		id := c.postIDs.allocID()
		author, ok := c.userIDs.randomID()
		if !ok {
			break
		}
		if err := c.addPost(db, id, author); err != nil {
			return errors.Trace(err)
		}
		newPosts = append(newPosts, id)
		newAuthors = append(newAuthors, author)
	}

	// Delete random posts.
	for i := 0; i < c.cfg.UpdatePosts && c.postIDs.len() > c.cfg.PostCount; i++ {
		id, ok := c.postIDs.randomID()
		if !ok {
			break
		}
		if err := c.deletePost(db, id); err != nil {
			return errors.Trace(err)
		}
		deletePosts = append(deletePosts, id)
	}

	// Check all.
	if err := c.checkAllPostCount(db); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *CRUDCase) createUser(db *sql.DB, id int64) error {
	c.userIDs.pushID(id)

	name := make([]byte, 10)
	randString(name, c.rnd)
	_, err := ExecWithRollback(db, []queryEntry{{query: fmt.Sprintf(`INSERT INTO crud_users VALUES (%v, "%s", %v)`, id, name, 0), expectAffectedRows: 1}})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *CRUDCase) checkUserPostCount(db *sql.DB, id int64) error {
	var count1, count2 int64
	checkF := func() error {
		var err error
		count1, err = c.QueryInt64(db, fmt.Sprintf("SELECT posts FROM crud_users WHERE id=%v", id))
		if err != nil {
			return errors.Trace(err)
		}
		count2, err = c.QueryInt64(db, fmt.Sprintf("SELECT COUNT(*) FROM crud_posts WHERE author=%v", id))
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	if err := runWithRetry(context.Background(), 3, 10*time.Second, checkF); err != nil {
		return errors.Trace(err)
	}

	if count1 != count2 {
		log.Fatalf("posts count not match %v != %v for user %v", count1, count2, id)
	}
	return nil
}

func (c *CRUDCase) deleteUser(db *sql.DB, id int64) error {
	if err := c.checkUserPostCount(db, id); err != nil {
		if errors.Cause(err) == errNotExist {
			c.userIDs.popID(id)
			return nil
		}
		return errors.Trace(err)
	}
	posts, err := c.QueryInt64s(db, fmt.Sprintf("SELECT id FROM crud_posts WHERE author=%v", id))
	if err != nil {
		return errors.Trace(err)
	}
	q := []queryEntry{
		{query: fmt.Sprintf("DELETE FROM crud_users WHERE id=%v", id), expectAffectedRows: 1},
		{query: fmt.Sprintf("DELETE FROM crud_posts WHERE author=%v", id), expectAffectedRows: int64(len(posts))},
	}
	if _, err = ExecWithRollback(db, q); err != nil {
		return errors.Trace(err)
	}
	c.userIDs.popID(id)
	for _, id := range posts {
		c.postIDs.popID(id)
	}
	return nil
}

func (c *CRUDCase) addPost(db *sql.DB, id, author int64) error {
	if err := c.checkUserPostCount(db, author); err != nil {
		return errors.Trace(err)
	}
	c.postIDs.pushID(id)
	title := make([]byte, 64)
	randString(title, c.rnd)
	q := []queryEntry{
		{query: fmt.Sprintf(`INSERT INTO crud_posts VALUES (%v, %v, "%s")`, id, author, title), expectAffectedRows: 1},
		{query: fmt.Sprintf("UPDATE crud_users SET posts=posts+1 WHERE id=%v", author), expectAffectedRows: 1},
	}
	if _, err := ExecWithRollback(db, q); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *CRUDCase) deletePost(db *sql.DB, id int64) error {
	author, err := c.QueryInt64(db, fmt.Sprintf("SELECT author from crud_posts WHERE id=%v", id))
	if err != nil {
		if errors.Cause(err) == errNotExist {
			c.postIDs.popID(id)
			return nil
		}
		return errors.Trace(err)
	}
	if err := c.checkUserPostCount(db, author); err != nil {
		return errors.Trace(err)
	}
	q := []queryEntry{
		{query: fmt.Sprintf("DELETE FROM crud_posts WHERE id=%v", id), expectAffectedRows: 1},
		{query: fmt.Sprintf("UPDATE crud_users SET posts=posts-1 WHERE id=%v", author), expectAffectedRows: 1},
	}
	if _, err := ExecWithRollback(db, q); err != nil {
		return errors.Trace(err)
	}
	c.postIDs.popID(id)
	return nil
}

func (c *CRUDCase) checkAllPostCount(db *sql.DB) error {
	var count1, count2 int64
	checkF := func() error {
		var err error
		count1, err = c.QueryInt64(db, "SELECT SUM(posts) FROM crud_users")
		if err != nil {
			return errors.Trace(err)
		}
		count2, err = c.QueryInt64(db, "SELECT COUNT(*) FROM crud_posts")
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	if err := runWithRetry(context.Background(), 3, 10*time.Second, checkF); err != nil {
		return errors.Trace(err)
	}
	if count1 != count2 {
		log.Fatalf("total posts count not match %v != %v", count1, count2)
	}

	return nil
}

func (c *CRUDCase) QueryInt64s(db *sql.DB, query string, args ...interface{}) ([]int64, error) {
	var vals []int64

	rows, err := db.Query(query, args...)
	if err != nil {
		return []int64{}, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var val int64
		if err := rows.Scan(&val); err != nil {
			log.Fatalf("failed to scan int64 result: %v", err)
		}
		vals = append(vals, val)
	}
	if err := rows.Err(); err != nil {
		return []int64{}, errors.Trace(err)
	}
	return vals, errors.Trace(err)
}

func (c *CRUDCase) QueryInt64(db *sql.DB, query string, args ...interface{}) (int64, error) {
	vals, err := c.QueryInt64s(db, query, args...)
	if err != nil {
		return 0, err
	}
	if len(vals) == 0 {
		return 0, errNotExist
	}
	if len(vals) != 1 {
		return 0, errors.Errorf("expect 1 row for query %v, but got %v rows", query, len(vals))
	}
	return vals[0], nil
}

type idList struct {
	sync.RWMutex
	ids       []int64
	positions map[int64]int
	maxID     int64
}

func newIDList() *idList {
	return &idList{
		positions: make(map[int64]int),
	}
}

func (l *idList) allocID() int64 {
	l.Lock()
	defer l.Unlock()

	l.maxID++
	return l.maxID
}

func (l *idList) len() int {
	l.RLock()
	defer l.RUnlock()

	return len(l.ids)
}

func (l *idList) pushID(id int64) {
	l.Lock()
	defer l.Unlock()

	l.positions[id] = len(l.ids)
	l.ids = append(l.ids, id)
}

func (l *idList) popID(id int64) {
	l.Lock()
	defer l.Unlock()

	pos, ok := l.positions[id]
	if !ok {
		return
	}
	lastID := l.ids[len(l.ids)-1]
	l.ids[pos] = lastID
	l.positions[lastID] = pos
	l.ids = l.ids[:len(l.ids)-1]
}

func (l *idList) randomID() (int64, bool) {
	l.RLock()
	defer l.RUnlock()

	if len(l.ids) == 0 {
		return 0, false
	}

	return l.ids[rand.Intn(len(l.ids))], true
}

func init() {
	RegisterSuite("crud", NewCRUDCase)
}
