package backend

import (
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/context"

	. "github.com/pingcap/octopus/benchbot/common"
)

const (
	jobsTableName   = "bench_jobs"
	jobsTableSchema = `(
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
	    CreateTime TEXT NOT NULL DEFAULT '',
	    Status TEXT NOT NULL DEFAULT '',
	    Meta TEXT NOT NULL DEFAULT '',
	    Result TEXT NOT NULL DEFAULT ''
    )`
)

type JobSet struct {
	mux    sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	db       *sql.DB
	jobs     map[int64]*BenchmarkJob
	jobsList []*BenchmarkJob
}

func NewJobSet(dbFile string) (*JobSet, error) {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, err
	}

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` %s", jobsTableName, jobsTableSchema)
	if _, err := db.Exec(stmt); err != nil {
		return nil, err
	}

	js := &JobSet{db: db}
	if err := js.loadAll(); err != nil {
		db.Close()
		return nil, err
	}
	js.ctx, js.cancel = context.WithCancel(context.Background())

	js.wg.Add(1)
	go js.sync()
	return js, nil
}

func (js *JobSet) sync() {
	defer js.wg.Done()
	for {
		select {
		case <-time.After(time.Second * 60):
			js.saveAll()
		case <-js.ctx.Done():
			return
		}
	}
}

func (js *JobSet) Close() {
	js.cancel()
	js.wg.Wait()
	js.saveAll()
	js.db.Close()
}

func (js *JobSet) Size() int {
	js.mux.RLock()
	defer js.mux.RUnlock()

	return len(js.jobsList)
}

func (js *JobSet) AddJob(job *BenchmarkJob) error {
	js.mux.Lock()
	defer js.mux.Unlock()

	if err := js.insert(job); err != nil {
		return nil
	}

	js.jobs[job.ID] = job
	js.jobsList = append(js.jobsList, job)
	return nil
}

func (js *JobSet) GetJob(jobID int64) *BenchmarkJob {
	js.mux.RLock()
	defer js.mux.RUnlock()

	job, ok := js.jobs[jobID]
	if !ok {
		return nil
	} else {
		return job
	}
}

func (js *JobSet) ListJobs() []*BenchmarkJob {
	return js.jobsList
}

func (js *JobSet) insert(job *BenchmarkJob) error {
	meta, err := DumpJSON(&job.Meta, false)
	if err != nil {
		return err
	}

	result, err := DumpJSON(&job.Result, false)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf(
		"INSERT INTO `%s` (`CreateTime`, `Status`, `Meta`, `Result`) VALUES(?, ?, ?, ?)", jobsTableName)
	res, err := js.db.Exec(stmt, job.CreateTime, job.Status, meta, result)
	if err != nil {
		return err
	}

	jobID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	job.ID = jobID
	return nil
}

func (js *JobSet) update(job *BenchmarkJob) error {
	meta, err := DumpJSON(&job.Meta, false)
	if err != nil {
		return err
	}

	result, err := DumpJSON(&job.Result, false)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf(
		"UPDATE `%s` SET `CreateTime` = ?, `Status` = ?, `Meta` = ?, `Result` = ? WHERE `Id` = ?", jobsTableName)
	_, err = js.db.Exec(stmt, job.CreateTime, job.Status, meta, result, job.ID)
	return err
}

func (js *JobSet) loadAll() error {
	stmt := fmt.Sprintf("SELECT * FROM `%s`", jobsTableName)
	rows, err := js.db.Query(stmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	js.jobs = make(map[int64]*BenchmarkJob)
	for rows.Next() {
		job, err := formatJob(rows)
		if err != nil {
			return err
		}
		js.jobs[job.ID] = job
		js.jobsList = append(js.jobsList, job)
	}
	return nil
}

func (js *JobSet) saveAll() error {
	js.mux.Lock()
	defer js.mux.Unlock()

	for _, job := range js.jobs {
		if err := js.update(job); err != nil {
			return err
		}
	}

	return nil
}

func formatJob(r *sql.Rows) (*BenchmarkJob, error) {
	job := new(BenchmarkJob)
	meta, result := "", ""
	if err := r.Scan(&job.ID, &job.CreateTime, &job.Status, &meta, &result); err != nil {
		return nil, err
	}

	rdr := ioutil.NopCloser(strings.NewReader(meta))
	if err := ReadJSON(rdr, &job.Meta); err != nil {
		return nil, err
	}

	rdr = ioutil.NopCloser(strings.NewReader(result))
	if err := ReadJSON(rdr, &job.Result); err != nil {
		return nil, err
	}

	return job, nil
}
