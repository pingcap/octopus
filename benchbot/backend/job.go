package backend

import (
	"encoding/json"
	"fmt"
	"time"
)

type BinPackage struct {
	Name       string `json:"-"`
	Branch     string `json:"branch"`
	Tag        string `json:"tag"`
	CommitHash string `json:"commit_hash"`
	Url        string `json:"bin_url"`
}

type TestingMeta struct {
	Creator      string     `json:"user"`
	Note         string     `json:"note"`
	Upstream     string     `json:"from"`
	HttpCallback string     `json:"callback"`
	TiDB         BinPackage `json:"tidb"`
	TiKV         BinPackage `json:"tikv"`
	Pd           BinPackage `json:"pd"`
}

type TestingResult struct {
	TestCases int64
	TimeCost  int64
	Success   int
	Fail      int
	Log       string
}

type TestingJob struct {
	Id         string        `json:"job_id"`
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

	job.Meta.TiDB.Name = "tidb"
	job.Meta.TiKV.Name = "tikv"
	job.Meta.Pd.Name = "pd"

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

	return nil
}

func (job *TestingJob) Valid() bool {
	basic := len(job.Meta.Creator) > 0
	return basic && job.Meta.TiDB.valid() && job.Meta.TiKV.valid() && job.Meta.Pd.valid()
}

func (job *TestingJob) Run() error {
	job.Status = Testing
	job.cheating()
	job.close()
	return nil
}

const simulateTime = 10

func (job *TestingJob) cheating() {
	fmt.Printf("[%s] job is running !\n", job.Id)
	for i := 0; i < simulateTime && job.Status == Testing; i++ {
		fmt.Printf("[%s] case %d ...\n", job.Id, i+1)
		time.Sleep(time.Second * 1)
	}
}

func (job *TestingJob) Abort(note string) {
	job.Status = Abort
	job.Result.Log = note
}

func (job *TestingJob) close() {
	job.Status = Finish
	fmt.Printf("[%s] job end. !\n", job.Id)
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
