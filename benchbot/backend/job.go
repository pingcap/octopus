package backend

import (
	"encoding/json"
	"errors"
	"fmt"
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
	ID         int64           `json:"id"`
	CreateTime string          `json:"createtime"`
	Status     string          `json:"status"`
	Meta       BenchmarkMeta   `json:"meta"`
	Result     BenchmarkResult `json:"result"`
	server     *Server
}

func NewBenchmarkJob(svr *Server) *BenchmarkJob {
	return &BenchmarkJob{
		ID:         svr.uuidCenter.ApplyNext(BenchmarkJobUUID),
		Status:     Pending,
		CreateTime: time.Now().Format(TimeFormat),
		server:     svr,
	}
}

func (pkg BinPackage) valid() bool {
	return (len(pkg.Branch) > 0 || len(pkg.Tag) > 0) && len(pkg.GitHash) > 0
}

func (job *BenchmarkJob) ParseFromRequstJSON(data []byte) error {
	if err := json.Unmarshal(data, &job.Meta); err != nil {
		return err
	}

	for _, item := range job.Meta.Packages {
		switch item.Repo {
		case "tidb":
			job.Meta.TiDB = item
			break
		case "tikv":
			job.Meta.TiKV = item
			break
		case "pd":
			job.Meta.Pd = item
			break
		default:
			break
		}
	}

	return nil
}

func (job *BenchmarkJob) abortedError() error {
	return fmt.Errorf("job [%s] aborted", job.ID)
}

func (job *BenchmarkJob) Valid() bool {
	basic := len(job.Meta.Creator) > 0
	return basic && job.Meta.TiDB.valid() && job.Meta.TiKV.valid() && job.Meta.Pd.valid()
}

func (job *BenchmarkJob) Run() error {
	defer func() {
		job.Status = Finished
	}()

	cluster, err := job.initCluster()
	if err != nil {
		return err
	}

	if err := job.runBenchmark(cluster); err != nil {
		return err
	}

	return nil
}

func (job *BenchmarkJob) initCluster() (Cluster, error) {
	if job.Status == Aborted {
		return nil, job.abortedError()
	}
	job.Status = Deploying

	// TODO
	time.Sleep(time.Millisecond * 100) // ps ; just pretend to do something

	return nil, errors.New("cluster init failed.")
}

func (job *BenchmarkJob) runBenchmark(cluster Cluster) error {
	if cluster == nil || cluster.Valid() {
		return errors.New("cluster init failed.")
	}
	defer cluster.Destory()

	if job.Status == Aborted {
		return job.abortedError()
	}
	job.Status = Running

	// TODO
	time.Sleep(time.Millisecond * 100) // ps ; just pretend to do something

	return nil
}

func (job *BenchmarkJob) Abort(note string) {
	// TODO :
	// status setting is not safe, need to imporve it in smart way !!!
	job.Status = Aborted
	job.Result.Message = fmt.Sprintf("[abort] : %s\n", note)
}

func (job *BenchmarkJob) close() {
	job.Status = Finished
}

func (job *BenchmarkJob) ExportBasicInfo() map[string]interface{} {
	data := make(map[string]interface{})
	data["job_id"] = job.ID
	data["status"] = job.Status
	data["createtime"] = job.CreateTime
	data["meta"] = job.Meta
	return data
}

func (job *BenchmarkJob) ExportResultInfo() map[string]interface{} {
	data := make(map[string]interface{})
	data["job_id"] = job.ID
	data["res"] = job.Result
	return data
}
