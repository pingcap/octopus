package backend

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/suite"
	"golang.org/x/net/context"
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
	Creator      string        `json:"user"`
	Note         string        `json:"note"`
	Upstream     string        `json:"from"`
	HttpCallback string        `json:"callback"`
	Packages     []*BinPackage `json:"packages"`
	TiDB         *BinPackage   `json:"-"`
	TiKV         *BinPackage   `json:"-"`
	Pd           *BinPackage   `json:"-"`
}

type BenchmarkResult struct {
	Cases   int           `json:"cases"`
	Details []*CaseResult `json:"details"`
	Message string        `json:"msg"`
}

type BenchmarkJob struct {
	mux    sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	ID         int64            `json:"id"`
	CreateTime string           `json:"created_time"`
	Status     string           `json:"status"`
	Meta       *BenchmarkMeta   `json:"meta"`
	Result     *BenchmarkResult `json:"result"`
}

func NewBenchmarkMeta() *BenchmarkMeta {
	return &BenchmarkMeta{
		Packages: make([]*BinPackage, 0),
	}
}

func NewBenchmarkResult() *BenchmarkResult {
	return &BenchmarkResult{
		Details: make([]*CaseResult, 0),
	}
}

func NewBenchmarkMetaFromJSON(data []byte) (*BenchmarkMeta, error) {
	meta := new(BenchmarkMeta)
	if err := json.Unmarshal(data, meta); err != nil {
		return nil, err
	}

	for _, item := range meta.Packages {
		switch item.Repo {
		case "tidb":
			meta.TiDB = item
		case "tikv":
			meta.TiKV = item
		case "pd":
			meta.Pd = item
		default:
			// return errors.New(fmt.Sprint("unsupported item : ", item.Repo))
		}
	}
	return meta, nil
}

func (meta *BenchmarkMeta) Valid() bool {
	return len(meta.Creator) > 0 && meta.TiDB.valid() && meta.TiKV.valid() && meta.Pd.valid()
}

func (pkg *BinPackage) valid() bool {
	return (len(pkg.Branch) > 0 || len(pkg.Tag) > 0) && len(pkg.GitHash) > 0
}

func NewBenchmarkJob() *BenchmarkJob {
	job := &BenchmarkJob{
		ID:         -1,
		Status:     Pending,
		CreateTime: time.Now().Format(TimeFormat),
		Meta:       NewBenchmarkMeta(),
		Result:     NewBenchmarkResult(),
	}

	job.ctx, job.cancel = context.WithCancel(context.Background())

	return job
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

func (job *BenchmarkJob) Run() (err error) {
	var cluster Cluster
	defer func() {
		success := (err == nil)
		if cluster != nil {
			cluster.Destory()
			cluster.Close()
		}
		job.onJobDone(success)
	}()

	cluster, err = job.initCluster()
	if err != nil {
		return
	}

	if err = job.run(cluster); err != nil {
		return
	}

	return
}

func (job *BenchmarkJob) onJobDone(success bool) {
	defer func() {
		job.mux.Lock()
		job.Status = Finished
		job.mux.Unlock()
	}()

	callbackUrl := job.Meta.HttpCallback
	if len(callbackUrl) > 0 {
		// TODO ... timeout limitaion
		data, _ := json.Marshal(map[string]interface{}{"id": job.ID})
		resp, err := http.Post(callbackUrl, "application/json", bytes.NewReader(data))
		if err != nil {
			log.Errorf("[job-%d] cause error on callback (%s) : %s", job.ID, callbackUrl, err.Error())
		} else {
			respData, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			log.Infof("[job-%d] on done callback : %s", job.ID, string(respData))
		}
	}
}

func (job *BenchmarkJob) initCluster() (cluster Cluster, err error) {
	if err = job.updateStatus(Deploying); err != nil {
		return
	}

	cluster = clusterManager.applyAnsibleCluster(job.Meta.TiDB, job.Meta.TiKV, job.Meta.Pd)
	defer func() {
		if err != nil && cluster != nil {
			cluster.Destory()
			cluster.Close()
		}
	}()

	if cluster == nil {
		// TODO ... retry ?
		err = errors.New("not any availble cluster to run")
		return
	}

	ops := [](func() error){cluster.Prepare, cluster.Deploy, cluster.Run}
	for step, op := range ops {
		// TODO :
		// so as to suspend operation imidate, call "kill -9" while op in another gorountine.
		res := make(chan error)
		go func() {
			res <- op()
		}()

		select {
		case err = <-res:
			if err != nil {
				log.Infof("job-[%d] cluster init (step=%d) failed : %s", job.ID, step, err.Error())
			}
		case <-job.ctx.Done():
			log.Infof("job-[%d] receive aborted signal during cluster init.", job.ID)
			<-res
			err = job.abortedError()
		}

		close(res)
		if err != nil {
			break
		}
	}

	return
}

func (job *BenchmarkJob) run(cluster Cluster) error {
	if err := job.updateStatus(Running); err != nil {
		return err
	}

	log.Infof("[job-%d] start running bench ...", job.ID)

	time.Sleep(time.Millisecond * 500) // ps : pretend to run cases

	log.Infof("[job-%d] bench finish !", job.ID)

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

func (job *BenchmarkJob) Abort(note string) error {
	job.mux.Lock()
	defer job.mux.Unlock()

	if !(job.Status == Pending || job.Status == Deploying || job.Status == Running) {
		return fmt.Errorf("[job-%d] can't aborted with status = '%s'", job.ID, job.Status)
	}

	job.cancel()

	// TODO :
	// status setting is not safe, need to imporve it in smart way !!!
	job.Status = Aborted
	job.Result.Message = fmt.Sprintf("[abort] : %s\n", note)

	return nil
}

func (job *BenchmarkJob) Stat() string {
	job.mux.RLock()
	defer job.mux.RUnlock()

	return job.Status
}
