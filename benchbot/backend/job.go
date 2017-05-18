package backend

import (
	"encoding/json"
	"fmt"
	"time"
)

type BinPackage struct {
	Branch     string `json:"branch"`
	CommitHash string `json:"commit_hash"`
	Url        string `json:"bin_url"`
}

type TestJob struct {
	Id           string     `json:"job_id"`
	CreateTime   string     `json:"createtime"`
	Status       string     `json:"status"`
	Creator      string     `json:"user"`
	Note         string     `json:"note"`
	Upstream     string     `json:"from"`
	HttpCallback string     `json:"callback"`
	TiDB         BinPackage `json:"tidb"`
	TiKV         BinPackage `json:"tikv"`
	Pd           BinPackage `json:"pd"`
	log          string
}

func NewTestJob() *TestJob {
	now := time.Now()

	job := &TestJob{
		Id:         GenUUID(),
		Status:     Pending,
		CreateTime: now.Format("2000-01-01 23:59:59"),
	}
	return job
}

func validPackage(pkg *BinPackage) bool {
	return len(pkg.Branch) > 0 && len(pkg.CommitHash) > 0
}

func (job *TestJob) ParseFromJSON(data []byte) error {
	if err := json.Unmarshal(data, job); err != nil {
		return err
	}

	return nil
}

func (job *TestJob) Dump() ([]byte, error) {
	return json.Marshal(job)
}

func (job *TestJob) Valid() bool {
	basic := len(job.Creator) > 0
	return basic && validPackage(&job.TiDB) && validPackage(&job.TiKV) && validPackage(&job.Pd)
}

func (job *TestJob) Run() error {
	job.Status = Testing

	fmt.Printf("[%s] job is running !\n", job.Id)
	for i := 0; i < 5 && job.Status == Testing; i++ {
		fmt.Printf("[%s] case %d ...\n", job.Id, i+1)
		time.Sleep(time.Microsecond * 1000)
	}

	job.close()
	return nil
}

func (job *TestJob) Abort(msg string) {
	job.Status = Abort
	job.log = msg
}

func (job *TestJob) close() {
	job.Status = Finish
	fmt.Printf("[%s] job end. !\n", job.Id)
}
