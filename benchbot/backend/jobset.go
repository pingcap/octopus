package backend

import (
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/ngaut/log"
	"golang.org/x/net/context"

	. "github.com/pingcap/octopus/benchbot/pkg"
)

const (
	jobsTableName   = "bench_jobs"
	jobsTableSchema = `(
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
	CreateTime TEXT NOT NULL DEFAULT '',
	Status TEXT NOT NULL DEFAULT '',
	Meta TEXT NOT NULL DEFAULT '',
	Result TEXT NOT NULL DEFAULT '')`
)

type JobSet struct {
	mux    sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	db       *sql.DB
	jobs     []*BenchmarkJob
	jobsByID map[int64]*BenchmarkJob
}

const (
	defaultJobsStorePath = "./jobs.db"
)

func NewJobSet(localFile string) (*JobSet, error) {
	if len(localFile) == 0 {
		localFile = defaultJobsStorePath
	}

	db, err := sql.Open("sqlite3", localFile)
	if err != nil {
		return nil, err
	}

	jobSet := &JobSet{db: db}
	if err := jobSet.init(); err != nil {
		return nil, err
	}

	jobSet.ctx, jobSet.cancel = context.WithCancel(context.Background())
	jobSet.autoSync()

	return jobSet, nil
}

func (js *JobSet) init() error {
	if _, err := js.db.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`  %s", jobsTableName, jobsTableSchema)); err != nil {
		return err
	}

	js.loadFromDB()
	return nil
}

func (js *JobSet) loadFromDB() error {
	jobs, err := js.listAll(js.db)
	if err != nil {
		return err
	}

	js.jobs = jobs
	js.jobsByID = make(map[int64]*BenchmarkJob)
	for _, job := range js.jobs {
		js.jobsByID[job.ID] = job
	}
	return nil
}

func (js *JobSet) syncToDB() error {
	js.mux.Lock()
	defer js.mux.Unlock()

	for _, job := range js.jobs {
		if err := js.update(js.db, job); err != nil {
			return err
		}
	}

	return nil
}

func (js *JobSet) autoSync() {
	js.wg.Add(1)
	go func() {
		defer js.wg.Done()
		for {
			select {
			case <-time.After(time.Second * 60):
				js.syncToDB()
			case <-js.ctx.Done():
				return
			}
		}
	}()
}

func (js *JobSet) Close() {
	if js.db != nil {
		js.cancel()
		js.wg.Wait()

		js.syncToDB()
		js.db.Close()
	}
}

func (js *JobSet) Size() int {
	js.mux.RLock()
	defer js.mux.RUnlock()

	return len(js.jobsByID)
}

func (js *JobSet) AddBenchJob(job *BenchmarkJob) error {
	js.mux.Lock()
	defer js.mux.Unlock()

	if err := js.add(js.db, job); err != nil {
		return nil
	}

	js.jobs = append(js.jobs, job)
	js.jobsByID[job.ID] = job
	// TODO ... size different

	return nil
}

func (js *JobSet) GetByID(jobID int64) *BenchmarkJob {
	js.mux.RLock()
	defer js.mux.RUnlock()

	job, ok := js.jobsByID[jobID]
	if !ok {
		return nil
	} else {
		return job
	}
}

func (js *JobSet) List() []*BenchmarkJob {
	return js.jobs // TODO .. safe
}

func (js *JobSet) add(db *sql.DB, job *BenchmarkJob) error {
	meta, err := DumpJSON(&job.Meta, false)
	if err != nil {
		return err
	}

	result, err := DumpJSON(&job.Result, false)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s`(`CreateTime`, `Status`, `Meta`, `Result`) VALUES(?,?,?,?)", jobsTableName)

	res, err := db.Exec(query, job.CreateTime, job.Status, meta, result)
	if err != nil {
		return err
	}

	if jobID, err := res.LastInsertId(); err != nil {
		return err
	} else {
		job.ID = jobID
	}

	return nil
}

func (js *JobSet) update(db *sql.DB, job *BenchmarkJob) error {
	meta, err := DumpJSON(&job.Meta, false)
	if err != nil {
		return err
	}

	result, err := DumpJSON(&job.Result, false)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		"UPDATE `%s` SET `CreateTime` = ?, `Status` = ?, `Meta` = ?, `Result` = ? WHERE `Id` = ?",
		jobsTableName)

	_, err = db.Exec(query, job.CreateTime, job.Status, meta, result, job.ID)
	if err != nil {
		return err
	}

	// TODO ... if not exits before !

	return nil
}

func (js *JobSet) size(db *sql.DB) int {
	query := fmt.Sprintf("SELECT COUNT(`id`) FROM `%s`", jobsTableName)
	r := js.db.QueryRow(query)

	var size int = -1
	if err := r.Scan(&size); err != nil {
		return -1
	}

	return size
}

func (js *JobSet) query(db *sql.DB, jobID int64) *BenchmarkJob {
	query := fmt.Sprintf("SELECT * FROM `%s` WHERE `Id` = ?", jobsTableName)
	rows, err := db.Query(query)
	defer rows.Close()

	if err != nil {
		return nil
	}

	var job *BenchmarkJob
	if rows.Next() {
		job, err = js.formatRows(rows)
		if err != nil {
			job = nil
		}
	}

	return job
}

func (js *JobSet) listAll(db *sql.DB) ([]*BenchmarkJob, error) {
	query := fmt.Sprintf("SELECT * FROM `%s`", jobsTableName)
	rows, err := db.Query(query)
	defer rows.Close()

	if err != nil {
		return make([]*BenchmarkJob, 0), err
	}

	size := js.Size()
	list := make([]*BenchmarkJob, 0, size)

	for rows.Next() {
		job, err := js.formatRows(rows)
		if err == nil {
			list = append(list, job)
		} else {
			return nil, err
		}
	}

	return list, nil
}

func (js *JobSet) formatRows(r *sql.Rows) (*BenchmarkJob, error) {
	job := new(BenchmarkJob)
	meta, result := "", ""
	if err := r.Scan(&job.ID, &job.CreateTime, &job.Status, &meta, &result); err != nil {
		return nil, err
	}

	rdr := ioutil.NopCloser(strings.NewReader(meta))
	if err := ReadJson(rdr, &job.Meta); err != nil {
		return nil, err
	}

	rdr = ioutil.NopCloser(strings.NewReader(result))
	if err := ReadJson(rdr, &job.Result); err != nil {
		return nil, err
	}

	return job, nil
}
