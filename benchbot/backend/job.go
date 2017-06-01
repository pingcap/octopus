package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	TimeFormat       string = "2006-01-02 15:04:05"
	BenchmarkJobUUID string = "benchmark_job_uuid"
)

type BinPackage struct {
	Repo     string `json:"repo"`
	Branch   string `json:"branch"`
	Tag      string `json:"tag"`
	GitHash  string `json:"git_hash"`
	Platform string `json:"platform"`
	BinUrl   string `json:"binary_url"`
}

type BenchmarkMeta struct {
	Creator      string       `json:"user"`
	Note         string       `json:"note"`
	Upstream     string       `json:"from"`
	HttpCallback string       `json:"callback"`
	Packages     []BinPackage `json:"packages"`
	TiDB         BinPackage   `json:"-"`
	TiKV         BinPackage   `json:"-"`
	Pd           BinPackage   `json:"-"`
}

type BenchmarkResult struct {
	Cases    int64  `json:"cases"`
	TimeCost int64  `json:"time_cost"`
	Success  int    `json:"success"`
	Fail     int    `json:"failure"`
	Message  string `json:"msg"`
}

type BenchmarkJob struct {
	mux sync.RWMutex

	ID         int64           `json:"id"`
	CreateTime string          `json:"created_time"`
	Status     string          `json:"status"`
	Meta       BenchmarkMeta   `json:"meta"`
	Result     BenchmarkResult `json:"result"`
}

func NewBenchmarkJob(svr *Server) *BenchmarkJob {
	return &BenchmarkJob{
		ID:         svr.uuidAllocator.Gen(BenchmarkJobUUID),
		Status:     Pending,
		CreateTime: time.Now().Format(TimeFormat),
	}
}

func (pkg BinPackage) valid() bool {
	return (len(pkg.Branch) > 0 || len(pkg.Tag) > 0) && len(pkg.GitHash) > 0
}

func (job *BenchmarkJob) ParseFromRequstJSON(data []byte) error {
	job.mux.Lock()
	defer job.mux.Unlock()

	if err := json.Unmarshal(data, &job.Meta); err != nil {
		return err
	}

	for _, item := range job.Meta.Packages {
		switch item.Repo {
		case "tidb":
			job.Meta.TiDB = item
		case "tikv":
			job.Meta.TiKV = item
		case "pd":
			job.Meta.Pd = item
		default:
			return errors.New(fmt.Sprint("unsupported item : ", item.Repo))
		}
	}

	return nil
}

func (job *BenchmarkJob) DumpToJson() string {
	job.mux.RLock()
	defer job.mux.RUnlock()

	data, err := json.Marshal(job)
	if err != nil {
		data = []byte{}
	}

	return string(data)
}

func (job *BenchmarkJob) abortedError() error {
	return fmt.Errorf("job [%s] aborted", job.ID)
}

func (job *BenchmarkJob) Valid() bool {
	job.mux.RLock()
	defer job.mux.RUnlock()

	basic := len(job.Meta.Creator) > 0
	return basic && job.Meta.TiDB.valid() && job.Meta.TiKV.valid() && job.Meta.Pd.valid()
}

func (job *BenchmarkJob) Run() error {
	defer func() {
		job.mux.Lock()
		job.Status = Finished
		job.mux.Unlock()
	}()

	cluster, err := job.initCluster()
	if err != nil {
		return err
	}

	if err := job.run(cluster); err != nil {
		return err
	}

	return nil
}

func (job *BenchmarkJob) initCluster() (Cluster, error) {
	if err := job.updateStatus(Deploying); err != nil {
		return nil, err
	}

	time.Sleep(time.Millisecond * 100) // ps ; just pretend to do something

	// TODO ... seperate whole progress into bunch of operations

	return nil, errors.New("cluster init failed.")
}

func (job *BenchmarkJob) run(cluster Cluster) error {
	if cluster == nil || cluster.Valid() {
		return errors.New("cluster init failed.")
	}
	defer cluster.Destory()

	if err := job.updateStatus(Running); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 100) // ps ; just pretend to do something

	// TODO ... seperate whole progress into bunch of operations

	return nil
}

func (job *BenchmarkJob) updateStatus(stat string) error {
	job.mux.Lock()
	defer job.mux.Unlock()

	if job.Status == Aborted {
		return job.abortedError()
	}

	job.Status = stat
	return nil
}

func (job *BenchmarkJob) Abort(note string) bool {
	job.mux.Lock()
	defer job.mux.Unlock()

	if !(job.Status == Pending || job.Status == Deploying || job.Status == Running) {
		return false
	}

	// TODO :
	// status setting is not safe, need to imporve it in smart way !!!
	job.Status = Aborted
	job.Result.Message = fmt.Sprintf("[abort] : %s\n", note)
	return true
}

func (job *BenchmarkJob) Stat() string {
	job.mux.RLock()
	defer job.mux.RUnlock()

	return job.Status
}
