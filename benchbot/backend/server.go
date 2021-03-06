package backend

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	. "github.com/pingcap/octopus/benchbot/cluster"
	. "github.com/pingcap/octopus/benchbot/suite"
)

const (
	pendingJobLimit int = 10
)

type Server struct {
	wg     sync.WaitGroup
	mux    sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	cfg         *ServerConfig
	suites      []BenchSuite
	clusters    *ClusterManager
	jobs        *JobSet
	runningJob  *BenchmarkJob
	pendingJobs chan *BenchmarkJob
}

type Status struct {
	TotalCount   int
	RunningCount int
	PendingCount int
}

func NewServer(cfg *ServerConfig, suites []BenchSuite) (*Server, error) {
	jobs, err := NewJobSet(filepath.Join(cfg.Dir, "jobs.db"))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	clusters, err := NewClusterManager(ctx, cfg.Ansible)
	if err != nil {
		return nil, err
	}

	svr := &Server{
		ctx:         ctx,
		cancel:      cancel,
		cfg:         cfg,
		suites:      suites,
		clusters:    clusters,
		jobs:        jobs,
		runningJob:  nil,
		pendingJobs: make(chan *BenchmarkJob, pendingJobLimit),
	}

	log.Info("Server start")
	svr.wg.Add(1)
	go svr.Start()
	return svr, nil
}

func (svr *Server) Start() {
	defer svr.wg.Done()
	for {
		select {
		case job, _ := <-svr.pendingJobs:
			if job != nil {
				svr.RunJob(job)
			}
		case <-svr.ctx.Done():
			return
		}
	}
}

func (svr *Server) Close() {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	svr.cancel()
	svr.wg.Wait()
	svr.jobs.Close()
}

func (svr *Server) Status() *Status {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	status := &Status{
		TotalCount:   svr.jobs.Size(),
		RunningCount: 0,
		PendingCount: len(svr.pendingJobs),
	}
	if svr.runningJob != nil {
		status.RunningCount = 1
	}

	return status
}

func (svr *Server) AddJob(meta *BenchmarkMeta) (*BenchmarkJob, error) {
	job := NewBenchmarkJob()
	job.Meta = meta
	svr.jobs.AddJob(job)

	select {
	case svr.pendingJobs <- job:
	default:
		return nil, errors.New("pending jobs limit exceeded")
	}

	return job, nil
}

func (svr *Server) RunJob(job *BenchmarkJob) {
	svr.mux.Lock()
	svr.runningJob = job
	svr.mux.Unlock()

	if err := job.Run(svr.suites, svr.clusters); err != nil {
		log.Errorf("[job-%d] run error: %s", job.ID, err)
	}

	svr.mux.Lock()
	svr.runningJob = nil
	svr.mux.Unlock()
}

func (svr *Server) AbortJob(jobID int64, note string) error {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	job := svr.jobs.GetJob(jobID)
	if job == nil {
		return fmt.Errorf("[job-%d] not found", jobID)
	}

	if err := job.Abort(note); err != nil {
		log.Errorf("[job-%d] abort: %s", jobID, err)
	}
	return nil
}

func (svr *Server) GetJob(jobID int64) *BenchmarkJob {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	return svr.jobs.GetJob(jobID)
}

func (svr *Server) ListJobs(lastN int) []*BenchmarkJob {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	size := svr.jobs.Size()
	if size > lastN {
		size = lastN
	}

	jobs := svr.jobs.ListJobs()
	return jobs[len(jobs)-size:]
}
