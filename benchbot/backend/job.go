package backend

import (
	_ "bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	. "github.com/pingcap/octopus/benchbot/cluster"
	. "github.com/pingcap/octopus/benchbot/suite"
)

const (
	TimeFormat       string = "2006-01-02 15:04:05"
	BenchmarkJobUUID string = "benchmark_job_uuid"
)

type BenchmarkMeta struct {
	Creator      string        `json:"user"`
	Note         string        `json:"note"`
	Upstream     string        `json:"from"`
	HttpCallback string        `json:"callback"`
	Packages     []*BinPackage `json:"packages"`
	Pd           *BinPackage   `json:"-"`
	TiKV         *BinPackage   `json:"-"`
	TiDB         *BinPackage   `json:"-"`
}

type BenchmarkResult struct {
	Cases   int           `json:"cases"`
	Message string        `json:"message"`
	Details []*CaseResult `json:"details"`
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
		case "pd":
			meta.Pd = item
		case "tikv":
			meta.TiKV = item
		case "tidb":
			meta.TiDB = item
		default:
			return nil, fmt.Errorf("unsupported repo: %s", item.Repo)
		}
	}

	return meta, nil
}

func (meta *BenchmarkMeta) Valid() bool {
	return len(meta.Creator) > 0 && meta.TiDB.Valid() && meta.TiKV.Valid() && meta.Pd.Valid()
}

func NewBenchmarkJob() *BenchmarkJob {
	job := &BenchmarkJob{
		ID:         -1,
		CreateTime: time.Now().Format(TimeFormat),
		Status:     Pending,
		Meta:       NewBenchmarkMeta(),
		Result:     NewBenchmarkResult(),
	}
	job.ctx, job.cancel = context.WithCancel(context.Background())
	return job
}

func (job *BenchmarkJob) Run(suites []BenchSuite, clusters *ClusterManager) error {
	cluster, err := job.deploy(clusters)
	if err != nil {
		return err
	}
	defer cluster.Close()

	if err := job.run(suites, cluster); err != nil {
		return err
	}
	return job.finish()
}

func (job *BenchmarkJob) deploy(clusters *ClusterManager) (Cluster, error) {
	job.updateStatus(Deploying)

	cluster, err := clusters.RequireCluster(job.Meta.Pd, job.Meta.TiKV, job.Meta.TiDB)
	if err != nil {
		return nil, err
	}

	ops := [](func() error){cluster.Prepare, cluster.Deploy}
	for step, op := range ops {
		res := make(chan error)
		go func() {
			res <- op()
		}()

		var err error

		select {
		case err = <-res:
			if err != nil {
				log.Errorf("[job-%d] failed to init cluster (step=%d): %s", job.ID, step, err)
			}
		case <-job.ctx.Done():
			log.Infof("[job-%d] receive aborted signal during cluster init", job.ID)
			err = fmt.Errorf("[job-%d] aborted", job.ID)
		}

		if err != nil {
			cluster.Close()
			return nil, err
		}
	}

	return cluster, nil
}

func (job *BenchmarkJob) run(suites []BenchSuite, cluster Cluster) error {
	job.updateStatus(Running)

	results := make([]*CaseResult, 0, len(suites))
	for _, s := range suites {
		res, err := s.Run(cluster)
		if err != nil {
			return err
		}
		results = append(results, res...)
	}

	job.Result.Cases = len(results)
	job.Result.Details = results
	return nil
}

func (job *BenchmarkJob) finish() error {
	job.updateStatus(Finished)

	callbackUrl := job.Meta.HttpCallback
	if len(callbackUrl) == 0 {
		return nil
	}

	resp, err := http.Get(callbackUrl)
	if err != nil {
		return err
	}

	respData, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	log.Infof("[job-%d] callback: %s", job.ID, string(respData))
	return nil
}

func (job *BenchmarkJob) Abort(note string) error {
	job.mux.Lock()
	defer job.mux.Unlock()

	if !(job.Status == Pending || job.Status == Deploying || job.Status == Running) {
		return fmt.Errorf("[job-%d] can't abort with status '%s'", job.ID, job.Status)
	}

	job.cancel()
	job.Status = Aborted
	job.Result.Message = note

	return nil
}

func (job *BenchmarkJob) GetStatus() string {
	job.mux.RLock()
	defer job.mux.RUnlock()

	return job.Status
}

func (job *BenchmarkJob) updateStatus(status string) {
	job.mux.Lock()
	defer job.mux.Unlock()

	job.Status = status
	log.Infof("[job-%d] %s", job.ID, status)
}
