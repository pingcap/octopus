package backend

import (
	_ "bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/pkg"
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
	Status     string           `json:"status"`
	CreateTime string           `json:"created_time"`
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

func (job *BenchmarkJob) Clone() *BenchmarkJob {
	dst := new(BenchmarkJob)
	if err := DeepClone(dst, job); err != nil {
		return nil
	}
	return dst
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
	return fmt.Errorf("job [%d] aborted", job.ID)
}

func (job *BenchmarkJob) Run(suites []BenchSuite) (err error) {
	cluster, err := job.initCluster()
	if err != nil {
		return
	}
	defer func() {
		cluster.Close()
		job.onJobDone(err == nil)
	}()

	if err = job.run(cluster, suites); err != nil {
		log.Errorf("[job-%d] run error : %s", job.ID, err)
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
		// data, _ := json.Marshal(map[string]interface{}{"id": job.ID})
		// resp, err := http.Post(callbackUrl, "application/json", bytes.NewReader(data))
		resp, err := http.Get(callbackUrl)
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
	if cluster == nil {
		err = errors.New("no available cluster to run")
		return
	}

	ops := [](func() error){cluster.Prepare, cluster.Deploy, cluster.Reset}
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

func (job *BenchmarkJob) run(cluster Cluster, suites []BenchSuite) error {
	if err := job.updateStatus(Running); err != nil {
		return err
	}

	log.Infof("[job-%d] start running bench ...", job.ID)

	results := make([]*CaseResult, 0, len(suites))
	for _, s := range suites {
		cluster.Run()

		if err := cluster.Ping(); err != nil {
			log.Errorf("failed to access cluster: %s", err)
			return err
		}

		db := cluster.Accessor()
		db.SetMaxIdleConns(1024)
		db.SetMaxOpenConns(1024)
		db.SetConnMaxLifetime(-1)
		time.Sleep(time.Second * 30)

		if res, err := s.Run(job.ctx, db); err == nil {
			results = append(results, res...)
		}

		cluster.Stop()
		cluster.Reset()
	}

	job.Result.Cases = len(results)
	job.Result.Details = results

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
