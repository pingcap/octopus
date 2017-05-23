package backend

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type BinPackage struct {
	Repo       string `json:"repo"`
	Branch     string `json:"branch"`
	Tag        string `json:"tag"`
	CommitHash string `json:"githash"`
	Platform   string `json:"platform"`
	Url        string `json:"binaryurl"`
}

type TestingMeta struct {
	Creator      string       `json:"user"`
	Note         string       `json:"note"`
	Upstream     string       `json:"from"`
	HttpCallback string       `json:"callback"`
	Packages     []BinPackage `json:"packages"`
	TiDB         BinPackage   `json:"-"`
	TiKV         BinPackage   `json:"-"`
	Pd           BinPackage   `json:"-"`
}

type TestingResult struct {
	TestCases int64  `json:"test_cases"`
	TimeCost  int64  `json:"time_cost"`
	Success   int    `json:"success"`
	Fail      int    `json:"failure"`
	Message   string `json:"msg"`
}

type TestingJob struct {
	Id         string        `json:"jobid"`
	CreateTime string        `json:"createtime"`
	Status     string        `json:"status"`
	Meta       TestingMeta   `json:"meta"`
	Result     TestingResult `json:"result"`
}

func NewTestingJob() *TestingJob {
	job := TestingJob{
		Id:         GenUUID(),
		Status:     Pending,
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	return &job
}

func (pkg BinPackage) valid() bool {
	return (len(pkg.Branch) > 0 || len(pkg.Tag) > 0) &&
		len(pkg.CommitHash) > 0
}

func (job *TestingJob) ParseFromRequstJSON(data []byte) error {
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

func (job *TestingJob) abortedError() error {
	return errors.New(fmt.Sprintf("job-%s abort", job.Id))
}

func (job *TestingJob) Valid() bool {
	basic := len(job.Meta.Creator) > 0
	return basic && job.Meta.TiDB.valid() && job.Meta.TiKV.valid() && job.Meta.Pd.valid()
}

func (job *TestingJob) Run() error {
	defer func() {
		job.Status = Finish
	}()

	tiCluster, err := job.initCluster()
	if err != nil {
		return err
	}

	if err := job.runTesting(tiCluster); err != nil {
		return err
	}

	return nil
}

func (job *TestingJob) initCluster() (TiCulster, error) {
	if job.Status == Abort {
		return nil, job.abortedError()
	}
	job.Status = Deploying

	// init cluster
	tiCfg := &TiClusterConfig{
		tidb: &job.Meta.TiDB,
		tikv: &job.Meta.TiKV,
		pd:   &job.Meta.Pd,
	}
	tiCluster := newTiClusterByAnsible(tiCfg)

	ops := [](func() error){tiCluster.Prepare, tiCluster.Deploy, tiCluster.Run}
	// TODO ... merge pre operation into one
	for _, op := range ops {
		if job.Status == Abort {
			return nil, job.abortedError()
		}

		if err := op(); err != nil {
			tiCluster.Destory()
			return nil, err
		}
	}

	return tiCluster, nil
}

func (job *TestingJob) runTesting(cluster TiCulster) error {
	defer cluster.Destory()

	job.Status = Running
	// TODO ...

	if job.Status == Abort {
		return job.abortedError()
	}

	return nil
}

// func (job *TestingJob) cheating() {
// 	const simulateTime = 10
//
// 	fmt.Printf("[%s] job is running !\n", job.Id)
// 	for i := 0; i < simulateTime && job.Status == Testing; i++ {
// 		fmt.Printf("[%s] case %d ...\n", job.Id, i+1)
// 		time.Sleep(time.Second * 1)
// 	}
// }

func (job *TestingJob) Abort(note string) {
	/*
		TODO ... status setting is not safe
				 need to imporve it in smart way !!!
	*/
	job.Status = Abort
	job.Result.Message = fmt.Sprintf("[abort] : %s\n", note)
}

func (job *TestingJob) close() {
	job.Status = Finish
	// fmt.Printf("[%s] job end. !\n", job.Id)
}

func (job *TestingJob) ExportBasicInfo() map[string]interface{} {
	data := make(map[string]interface{})
	data["job_id"] = job.Id
	data["status"] = job.Status
	data["createtime"] = job.CreateTime
	data["meta"] = job.Meta
	return data
}

func (job *TestingJob) ExportResultInfo() map[string]interface{} {
	data := make(map[string]interface{})
	data["job_id"] = job.Id
	data["res"] = job.Result
	return data
}
