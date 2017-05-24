package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
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
	Id         string          `json:"jobid"`
	CreateTime string          `json:"createtime"`
	Status     string          `json:"status"`
	Meta       BenchmarkMeta   `json:"meta"`
	Result     BenchmarkResult `json:"result"`
}

func NewBenchmarkJob() *BenchmarkJob {
	job := BenchmarkJob{
		Id:         GenUUID(),
		Status:     Pending,
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	return &job
}

func (pkg BinPackage) valid() bool {
	return (len(pkg.Branch) > 0 || len(pkg.Tag) > 0) &&
		len(pkg.GitHash) > 0
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
	return errors.New(fmt.Sprintf("job-%s abort", job.Id))
}

func (job *BenchmarkJob) Valid() bool {
	basic := len(job.Meta.Creator) > 0
	return basic && job.Meta.TiDB.valid() && job.Meta.TiKV.valid() && job.Meta.Pd.valid()
}

func (job *BenchmarkJob) Run() error {
	defer func() {
		job.Status = Finished
	}()

	tiCluster, err := job.initCluster()
	if err != nil {
		return err
	}

	if err := job.runBenchmark(tiCluster); err != nil {
		return err
	}

	return nil
}

func (job *BenchmarkJob) initCluster() (TiCulster, error) {
	if job.Status == Aborted {
		return nil, job.abortedError()
	}
	job.Status = Deploying

	// init cluster
	tiCfg := &TiClusterConfig{
		tidb: &job.Meta.TiDB,
		tikv: &job.Meta.TiKV,
		pd:   &job.Meta.Pd,
	}
	tiCluster := newAnsibleCluster(tiCfg)

	ops := [](func() error){tiCluster.Prepare, tiCluster.Deploy, tiCluster.Run}
	// TODO ... merge pre operation into one
	for _, op := range ops {
		if job.Status == Aborted {
			return nil, job.abortedError()
		}

		if err := op(); err != nil {
			tiCluster.Destory()
			return nil, err
		}
	}

	return tiCluster, nil
}

func (job *BenchmarkJob) runBenchmark(cluster TiCulster) error {
	defer cluster.Destory()

	job.Status = Running
	// TODO ...

	if job.Status == Aborted {
		return job.abortedError()
	}

	return nil
}

// func (job *BenchmarkJob) cheating() {
// 	const simulateTime = 10
//
// 	fmt.Printf("[%s] job is running !\n", job.Id)
// 	for i := 0; i < simulateTime && job.Status == Testing; i++ {
// 		fmt.Printf("[%s] case %d ...\n", job.Id, i+1)
// 		time.Sleep(time.Second * 1)
// 	}
// }

func (job *BenchmarkJob) Abort(note string) {
	// TODO :
	// status setting is not safe, need to imporve it in smart way !!!
	job.Status = Aborted
	job.Result.Message = fmt.Sprintf("[abort] : %s\n", note)
}

func (job *BenchmarkJob) close() {
	job.Status = Finished
	// fmt.Printf("[%s] job end. !\n", job.Id)
}

func (job *BenchmarkJob) ExportBasicInfo() map[string]interface{} {
	data := make(map[string]interface{})
	data["job_id"] = job.Id
	data["status"] = job.Status
	data["createtime"] = job.CreateTime
	data["meta"] = job.Meta
	return data
}

func (job *BenchmarkJob) ExportResultInfo() map[string]interface{} {
	data := make(map[string]interface{})
	data["job_id"] = job.Id
	data["res"] = job.Result
	return data
}
