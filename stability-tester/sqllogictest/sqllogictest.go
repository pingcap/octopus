package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/util"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"golang.org/x/net/context"
)

// SqllogicTestCaseConfig is for sqllogic_test test case
type SqllogicTestCaseConfig struct {
	TestPath  string `toml:"test_path"`
	SkipError bool   `toml:"skipError"`
	Parallel  int    `toml:"parallel"`
	DBName    string `toml:"database"`
	Host      string `toml:"host"`
	User      string `toml:"user"`
	Password  string `toml:"password"`
}

// SqllogictestCase is configuration for sqllogic.
type SqllogictestCase struct {
	*SqllogicTestCaseConfig
}

// NewSqllogictest return the SqllogictestCase.
func NewSqllogictest(cfg *SqllogicTestCaseConfig) *SqllogictestCase {
	c := &SqllogictestCase{
		SqllogicTestCaseConfig: cfg,
	}
	return c
}

// Initialize implements Case Initialize interface.
func (s *SqllogictestCase) Initialize(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to init...", s.String())
	defer func() {
		log.Infof("[%s] init end...", s.String())
	}()
	for index := 0; index < s.Parallel; index++ {
		dropsql := fmt.Sprintf("drop database if exists sqllogic_test_%d;", index)
		err := util.RunWithRetry(ctx, dbTryNumber, 3, func() error {
			_, err := db.Exec(dropsql)
			return err
		})
		if err != nil {
			return errors.Errorf("executing %s err %v", dropsql, err)
		}

		createsql := fmt.Sprintf("create database sqllogic_test_%d;", index)
		err = util.RunWithRetry(ctx, dbTryNumber, 3*time.Second, func() error {
			_, err := db.Exec(createsql)
			return err
		})
		if err != nil {
			return errors.Errorf("executing %s err %v", createsql, err)
		}
	}
	return nil
}

// String implements fmt.Stringer interface.
func (s *SqllogictestCase) String() string {
	return "sqllogic_test"
}

// Execute implements Case Execute interface.
func (s *SqllogictestCase) Execute(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to test...", s)
	defer func() {
		log.Infof("[%s] test end...", s)
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err := s.ExecuteSqllogic(ctx, db); err != nil {
			log.Errorf("[%s] execute failed %v", s.String(), err)
		}
	}
}

// ExecuteSqllogic run case
func (s *SqllogictestCase) ExecuteSqllogic(ctx context.Context, db *sql.DB) error {
	startTime := time.Now()
	var fileNames []string
	filepath.Walk(s.TestPath, func(testPath string, info os.FileInfo, err error) error {
		if info == nil || info.IsDir() {
			return nil
		}

		if !strings.HasSuffix(testPath, ".test") {
			return nil
		}

		fileNames = append(fileNames, testPath)
		return nil
	})

	taskCount := s.Parallel
	if taskCount > len(fileNames) {
		taskCount = len(fileNames)
	}
	if taskCount <= 0 {
		return nil
	}
	taskChan := make(chan string, taskCount)
	doneChan := make(chan struct{}, taskCount)
	resultChan := make(chan *result, taskCount)
	mdbs := createDatabases(taskCount, s.Host, s.User, s.Password, s.DBName)
	defer closeDatabases(mdbs)

	go addTasks(ctx, fileNames, taskChan)

	for i := 0; i < taskCount; i++ {
		go doProcess(ctx, doneChan, taskChan, resultChan, mdbs[i], i, s.SkipError)
	}

	go doWait(ctx, doneChan, resultChan, taskCount)
	if errCnt := doResult(ctx, resultChan, startTime); errCnt > 0 {
		log.Fatalf("Test failed, error count:%d", errCnt)
	}
	return nil
}

const (
	intType     = 'I'
	floatType   = 'R'
	stringType  = 'T'
	dbTryNumber = 100
)

type msgType byte

const (
	infoType msgType = iota
	errorType
	fatalType
)

type result struct {
	data string
	tp   msgType
}

type tester struct {
	labelHashes map[string]string
	mdb         *sql.DB
	ctx         context.Context
}

type lineScanner struct {
	*bufio.Scanner
	line int
}

type statement struct {
	pos       string
	sql       string
	expectErr bool
}

type query struct {
	pos             string
	sql             string
	colTypes        string
	sortMode        string
	label           string
	expectedValues  int
	expectedHash    string
	expectedResults []string
}

type value struct {
	Value string
	Type  byte
}

var (
	// Regexp for query result hash result like "15 values hashing to f7f59b0d893d8b24a77e45c84e33a4dc"
	resultHashRE = regexp.MustCompile(`^(\d+)\s+values?\s+hashing\s+to\s+([0-9A-Fa-f]+)$`)
)

func createDatabases(num int, host string, user string, password string, dbname string) []*sql.DB {
	mdbs := make([]*sql.DB, 0, num)
	for i := 0; i < num; i++ {
		dbstring := fmt.Sprintf("%s:%s@tcp(%s)/sqllogic_test_%d?strict=true", user, password, host, i)
		mdb, err := sql.Open("mysql", dbstring)
		if err != nil {
			log.Fatal(err)
		}

		mdbs = append(mdbs, mdb)
	}

	return mdbs
}

func closeDatabases(mdbs []*sql.DB) {
	for _, mdb := range mdbs {
		mdb.Close()
	}
	os.RemoveAll("var")
}

func addTasks(ctx context.Context, tasks []string, taskChan chan string) {
	for _, task := range tasks {
		log.Infof("add task %s", task)
		taskChan <- task
	}
	close(taskChan)
}

func doProcess(ctx context.Context, doneChan chan struct{}, taskChan chan string, resultChan chan *result,
	mdb *sql.DB, runid int, skipError bool) {
	for task := range taskChan {
		t := &tester{
			labelHashes: make(map[string]string),
			mdb:         mdb,
			ctx:         ctx,
		}

		log.Infof("run %s", task)
		t.run(ctx, task, resultChan, runid, skipError)
		select {
		case <-ctx.Done():
			return
		default:
		}
	}

	doneChan <- struct{}{}
}

func (t *tester) prepare(ctx context.Context, runid int) {
	dropsql := fmt.Sprintf("drop database if exists sqllogic_test_%d;", runid)
	err := util.RunWithRetry(ctx, dbTryNumber, 3, func() error {
		_, err := t.mdb.Exec(dropsql)
		return err
	})
	if err != nil {
		log.Fatalf("executing %s err %v", dropsql, err)
	}

	createsql := fmt.Sprintf("create database sqllogic_test_%d;", runid)
	err = util.RunWithRetry(ctx, dbTryNumber, 3, func() error {
		_, err := t.mdb.Exec(createsql)
		return err
	})
	if err != nil {
		log.Fatalf("executing %s err %v", createsql, err)
	}

	usesql := fmt.Sprintf("USE sqllogic_test_%d;", runid)
	err = util.RunWithRetry(ctx, dbTryNumber, 3, func() error {
		_, err := t.mdb.Exec(usesql)
		return err
	})
	if err != nil {
		log.Fatalf("executing %s err %v", usesql, err)
	}

}

func (t *tester) run(ctx context.Context, path string, resultChan chan *result, runid int, skipError bool) {
	var err error

	file, err := os.Open(path)
	if err != nil {
		sendFatalResult(resultChan, err.Error())
		return
	}

	t.prepare(ctx, runid)

	s := newLineScanner(file)

LOOP:
	for s.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fields := strings.Fields(s.Text())
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		switch cmd {
		case "statement":
			stmt := statement{pos: fmt.Sprintf("%s:%d", path, s.line)}

			// format is: statement ok | statement error
			if len(fields) != 2 {
				data := fmt.Sprintf("%s: invalid test statement: %s", stmt.pos, s.Text())
				sendFatalResult(resultChan, data)
				return
			}

			stmt.expectErr = (fields[1] == "error")

			var buf bytes.Buffer
			for s.Scan() {
				line := s.Text()
				if line == "" {
					break
				}
				fmt.Fprintln(&buf, line)
			}
			stmt.sql = strings.TrimSpace(buf.String())
			if err := t.execStatement(ctx, stmt); err != nil {
				sendFatalResult(resultChan, err.Error())
				return
			}

		case "query":
			q := query{pos: fmt.Sprintf("%s:%d", path, s.line), sortMode: "nosort"}

			// format is query <type-string> <sort-mode> <label>
			if len(fields) < 2 {
				log.Fatalf("%s: invalid test statement: %s", q.pos, s.Text())
			} else {
				q.colTypes = fields[1]
				for _, v := range q.colTypes {
					if v != intType && v != floatType && v != stringType {
						data := fmt.Sprintf("%s: invalid type string in query: %s, must be 'I', 'R', or 'T'", q.pos, s.Text())
						sendFatalResult(resultChan, data)
						return
					}
				}

				if len(fields) >= 3 {
					switch fields[2] {
					case "nosort", "rowsort", "valuesort":
						q.sortMode = fields[2]
					default:
						data := fmt.Sprintf("%s: invalid sort mode in query: %s", q.pos, s.Text())
						sendFatalResult(resultChan, data)
						return
					}
				}
				if len(fields) == 4 {
					q.label = fields[3]
				}
			}
			var buf bytes.Buffer
			for s.Scan() {
				line := s.Text()
				if line == "----" {
					break
				}
				fmt.Fprintln(&buf, line)
			}
			q.sql = strings.TrimSpace(buf.String())

			// query has two result format
			// 1 a hash result like "15 values hashing to f7f59b0d893d8b24a77e45c84e33a4dc"
			// 2 a two-dimension result set for individual value
			if s.Scan() {
				if m := resultHashRE.FindStringSubmatch(s.Text()); m != nil {
					q.expectedValues, err = strconv.Atoi(m[1])
					if err != nil {
						data := fmt.Sprintf("%s: invalid result value in query: %s", q.pos, s.Text())
						sendFatalResult(resultChan, data)
						return
					}
					q.expectedHash = m[2]
				} else {
					for {
						results := strings.Fields(s.Text())
						if len(results) == 0 {
							break
						}
						q.expectedResults = append(q.expectedResults, results...)
						if !s.Scan() {
							break
						}
					}
					q.expectedValues = len(q.expectedResults)
				}
			}

			if err := t.execQuery(q); err != nil {
				if skipError {
					sendErrorResult(resultChan, err.Error())
				} else {
					sendFatalResult(resultChan, err.Error())
					return
				}
			}
		case "halt":
			// for debug only, ignore the rest of the cases.
			break LOOP

		case "hash-threshold":
			// we just run the origin test, no need to re-generate.
			// so no need to handle this

		case "onlyif", "skipif":
			// we only care mysql now
			if len(fields) < 2 {
				data := fmt.Sprintf("invalid %s: %s", cmd, s.Text())
				sendFatalResult(resultChan, data)
				return
			}

			needSkip := false
			name := fields[1]
			if (cmd == "onlyif" && name != "mysql") ||
				(cmd == "skipif" && name == "mysql") {
				needSkip = true
			}
			if needSkip {
				// skip this case
				for s.Scan() {
					line := s.Text()
					if line == "" {
						break
					}
				}
			}
		}
		sendInfoResult(resultChan, "")
	}

	if err := s.Err(); err != nil {
		sendFatalResult(resultChan, err.Error())
		return
	}
}

func sendInfoResult(resultChan chan *result, data string) {
	msg := &result{data, infoType}
	resultChan <- msg
}

func sendErrorResult(resultChan chan *result, data string) {
	msg := &result{data, errorType}
	resultChan <- msg
}
func sendFatalResult(resultChan chan *result, data string) {
	msg := &result{data, fatalType}
	resultChan <- msg
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{
		Scanner: bufio.NewScanner(r),
		line:    0,
	}
}

func (l *lineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.line++
	}
	return ok
}

func (t *tester) execStatement(ctx context.Context, stmt statement) error {
	defer func() {
		var err error
		if e := recover(); e != nil {
			switch x := e.(type) {
			case error:
				err = x
			default:
				err = fmt.Errorf("%v", e)
			}
		}
		if err != nil {
			log.Errorf("PANIC for %s:[%s] %v\n%s", stmt.pos, stmt.sql, err, debug.Stack())
		}
	}()

	err := util.RunWithRetry(ctx, dbTryNumber, 3, func() error {
		_, err := t.mdb.Exec(stmt.sql)
		return err
	})
	if stmt.expectErr {
		if err == nil {
			return fmt.Errorf("%s: expected error, but return ok", stmt.pos)
		}
	} else if err != nil {
		if ignoreUnknownError(err) {
			return nil
		}
		return fmt.Errorf("%s: expected success, but found %v", stmt.pos, err)
	}
	return nil
}

func (t *tester) execQuery(q query) error {
	defer func() {
		var err error
		if e := recover(); e != nil {
			switch x := e.(type) {
			case error:
				err = x
			default:
				err = fmt.Errorf("%v", e)
			}
		}
		if err != nil {
			log.Errorf("PANIC for %s:[%s] %v\n%s", q.pos, q.sql, err, debug.Stack())
		}
	}()

	var rows *sql.Rows
	var err error
	for i := 0; i < dbTryNumber; i++ {
		rows, err = t.mdb.Query(q.sql)
		if err == nil {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("%s: query err %v - sql[ %s ]", q.pos, err, q.sql)
	}

	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("%s: get result columns err %v - sql[ %s ]", q.pos, err, q.sql)
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = &value{Type: q.colTypes[i]}
	}

	var results []string

	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return fmt.Errorf("%s: scan rows err %v - sql[ %s ]", q.pos, err, q.sql)
		}
		for _, v := range vals {
			vv := string(v.(*value).Value)
			results = append(results, vv)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: scan rows err %v  - sql[ %s ]", q.pos, err, q.sql)
	}

	switch q.sortMode {
	case "rowsort":
		colNum := len(q.colTypes)
		rowNum := len(results) / colNum

		r := make(rowSlice, rowNum)
		for i := 0; i < len(r); i++ {
			start := i * colNum
			stop := (i + 1) * colNum
			r[i] = results[start:stop]
		}
		sort.Sort(r)
		results = r.flatten()
	case "valuesort":
		sort.Strings(results)
	}

	h := md5.New()

	for _, vv := range results {
		_, _ = io.WriteString(h, vv)
		_, _ = io.WriteString(h, "\n")
	}

	hash := fmt.Sprintf("%x", h.Sum(nil))

	if q.expectedHash != "" {
		n := len(results)
		if q.expectedValues != n {
			return fmt.Errorf("%s: expected %d results, but found %d - sql[ %s ]", q.pos, q.expectedValues, n, q.sql)
		}
		// Hash the values using MD5. This hashing precisely matches the hashing in
		// sqllogictest.c.

		if q.expectedHash != hash {
			return fmt.Errorf("%s: expected %s, but found %s - sql[ %s ]", q.pos, q.expectedHash, hash, q.sql)
		}
	} else {
		// some origin expected results contain space, we split this result into multi sub results using Fields above,
		// so we will meet error for directly DeepEqual here.
		if strings.Join(q.expectedResults, " ") != strings.Join(results, " ") {
			return fmt.Errorf("%s: expected %q, but found %q - sql[ %s ]", q.pos, q.expectedResults, results, q.sql)
		}
	}

	// TODO, if we have a label, we will check hash with other tests for same label
	if q.label != "" {
		if lastHash, ok := t.labelHashes[q.label]; ok {
			if hash != lastHash {
				return fmt.Errorf("%s: hash %s not equal last query %s - sql[ %s ]", q.pos, lastHash, hash, q.sql)
			}
		} else {
			t.labelHashes[q.label] = hash
		}
	}
	return nil
}

type rowSlice [][]string

func (r rowSlice) Len() int {
	return len(r)
}

func (r rowSlice) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r rowSlice) Less(i, j int) bool {
	ri := r[i]
	rj := r[j]
	for k := 0; k < len(ri); k++ {
		if ri[k] < rj[k] {
			return true
		} else if ri[k] > rj[k] {
			return false
		}
	}
	return false
}

func (r rowSlice) flatten() []string {
	var s []string
	if len(r) == 0 {
		return s
	}

	for _, v := range r {
		s = append(s, v...)
	}

	return s
}

func doWait(ctx context.Context, doneChan chan struct{}, resultChan chan *result, taskCount int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		for i := 0; i < taskCount; i++ {
			<-doneChan
		}
		close(resultChan)
	}

}

// Return error count
func doResult(ctx context.Context, resultChan chan *result, startTime time.Time) int64 {
	var totalCount, errCount int64
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return errCount
		case <-ticker.C:
			printResultInfo("run", totalCount, errCount, startTime)
		case msg, ok := <-resultChan:
			if !ok {
				log.Infof("\nsqllogictest finished!")
				printResultInfo("final", totalCount, errCount, startTime)
				return errCount
			}

			switch msg.tp {
			case infoType:
				totalCount++
			case errorType:
				errCount++
				log.Error(msg.data)
			case fatalType:
				log.Fatal(msg.data)
			}
		}
	}
}

func printResultInfo(tag string, totalCount, errCount int64, startTime time.Time) {
	now := time.Now()
	seconds := now.Unix() - startTime.Unix()

	qps := int64(-1)
	if seconds > 0 {
		qps = totalCount / seconds
	}

	log.Infof("[%s]total %d cases, failed %d, cost %d seconds, qps %d, start %s, now %s\n", tag, totalCount, errCount, seconds, qps, startTime, now)
}

func (v *value) Scan(src interface{}) error {
	switch t := src.(type) {
	case nil:
		v.Value = "NULL"
	case bool:
		if t {
			v.Value = "1"
		} else {
			v.Value = "0"
		}
	case int64:
		v.Value = strconv.FormatInt(t, 10)
	case uint64:
		v.Value = strconv.FormatUint(t, 10)
	case float32:
		v.handleFloat(float64(t))
	case float64:
		v.handleFloat(float64(t))
	case []byte:
		v.handleString(string(t))
	case string:
		// Empty strings are rendered as "(empty)".
		// TODO: all control characters and unprintable characters are rendered as "@".
		v.handleString(t)
	default:
		return fmt.Errorf("unexpected type: %T", src)
	}
	return nil
}

func (v *value) handleFloat(f float64) {
	if v.Type == intType {
		// if result type is int, must convert to int then format
		v.Value = strconv.FormatInt(int64(f), 10)
	} else {
		// Floating point values are rendered as if by printf("%.3f")
		v.Value = fmt.Sprintf("%.3f", f)
	}
}

func (v *value) handleString(str string) {
	if v.Type == stringType {
		if len(str) == 0 {
			v.Value = "(empty)"
		} else {
			v.Value = renderString(str)
		}
	} else if v.Type == intType {
		// no need to handle error, if parse failed, we will use 0
		// use ParseFloat because we may get float string like "123.123"
		f, _ := strconv.ParseFloat(str, 64)
		v.Value = strconv.FormatInt(int64(f), 10)
	} else if v.Type == floatType {
		// no need to handle error, if parse failed, we will use 0
		f, _ := strconv.ParseFloat(str, 64)
		v.Value = fmt.Sprintf("%.3f", f)
	}
}

func renderString(str string) string {
	// all control characters and unprintable characters are rendered as "@"
	dest := make([]byte, 0, len(str))
	for _, v := range str {
		if v < ' ' || v == '~' {
			dest = append(dest, '@')
		} else {
			dest = append(dest, byte(v))
		}
	}
	return string(dest)
}

func ignoreUnknownError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := terror.ErrCode(mysqlErr.Number)
	if errCode == tmysql.ErrUnknown {
		return true
	}
	return false
}