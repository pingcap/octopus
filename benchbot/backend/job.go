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
	ID         string          `json:"jobid"`
	CreateTime string          `json:"createtime"`
	Status     string          `json:"status"`
	Meta       BenchmarkMeta   `json:"meta"`
	Result     BenchmarkResult `json:"result"`
	server     *Server
}

func NewBenchmarkJob(svr *Server) *BenchmarkJob {
	job := BenchmarkJob{
		ID:         GenUUID(),
		Status:     Pending,
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
		server:     svr,
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
	return errors.New(fmt.Sprintf("job-%s abort", job.ID))
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

	// init cluster
	cfg := &ClusterConfig{
		tidb: &job.Meta.TiDB,
		tikv: &job.Meta.TiKV,
		pd:   &job.Meta.Pd,
	}

	clusterMgr := job.server.clusterMgr
	cluster := clusterMgr.newAnsibleCluster(cfg)

	ops := [](func() error){cluster.Prepare, cluster.Deploy, cluster.Run}
	// TODO ... merge pre operation into one
	for _, op := range ops {
		if job.Status == Aborted {
			return nil, job.abortedError()
		}

		if err := op(); err != nil {
			cluster.Destory()
			return nil, err
		}
	}

	return cluster, nil
}

func (job *BenchmarkJob) runBenchmark(cluster Cluster) error {
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
// 	fmt.Printf("[%s] job is running !\n", job.ID)
// 	for i := 0; i < simulateTime && job.Status == Testing; i++ {
// 		fmt.Printf("[%s] case %d ...\n", job.ID, i+1)
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
	// fmt.Printf("[%s] job end. !\n", job.ID)
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
