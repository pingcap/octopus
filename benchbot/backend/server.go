package backend

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

const (
	penndingJobLimit        int    = 10
	defaultHistoryShowCount int    = 10
	logFileName             string = "server.log"
)

type Server struct {
	wg  sync.WaitGroup
	mux sync.RWMutex

	conf   *ServerConfig
	ctx    context.Context
	cancel context.CancelFunc

	runningJob *BenchmarkJob
	orders     chan *BenchmarkJob
	jobs       *JobSet
}

type Summary struct {
	RunningCount int
	PendingCount int
	HistoryCount int
}

func NewServer(cfg *ServerConfig) (*Server, error) {
	svr := &Server{conf: cfg}
	svr.ctx, svr.cancel = context.WithCancel(context.Background())

	var err error
	if err = svr.init(); err != nil {
		return nil, err
	}

	svr.jobs, err = NewJobSet("")
	if err != nil {
		return nil, err
	}

	svr.runningJob = nil
	svr.orders = make(chan *BenchmarkJob, penndingJobLimit)

	svr.wg.Add(1)
	go svr.mainloop()
	log.Info("Server running ...")

	return svr, nil
}

func (svr *Server) init() (err error) {
	if svr.ctx == nil {
		panic("conetext required !")
	}

	if err = os.MkdirAll(svr.conf.Dir, os.ModePerm); err != nil {
		return err
	}

	if err = initAnsibleEnv(svr.conf); err != nil {
		return
	}

	if err = initClusterManager(svr.conf, svr.ctx); err != nil {
		return
	}

	return
}

func (svr *Server) Close() {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	svr.cancel()
	svr.wg.Wait()
	svr.jobs.Close()
}

func (svr *Server) DumpSummary() *Summary {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	info := &Summary{
		PendingCount: len(svr.orders),
		RunningCount: 0,
		HistoryCount: svr.jobs.Size(),
	}

	if svr.runningJob != nil {
		info.RunningCount = 1
	}

	return info
}

func (svr *Server) mainloop() {
	defer svr.wg.Done()

	var err error
	var job *BenchmarkJob

	for {
		select {
		case job, _ = <-svr.orders:
			if job == nil {
				continue
			}
		case <-svr.ctx.Done():
			return
		}

		svr.setRunningJob(job)
		if err = job.Run(); err != nil {
			log.Errorf("job-[%d] run failed : %s !", job.ID, err.Error())
		}

		svr.setRunningJob(nil)
	}

	return
}

func (svr *Server) setRunningJob(job *BenchmarkJob) {
	svr.mux.Lock()
	svr.runningJob = job
	svr.mux.Unlock()
}

func (svr *Server) CreateJob(meta *BenchmarkMeta) (*BenchmarkJob, error) {
	job := NewBenchmarkJob()
	job.Meta = meta

	select {
	case svr.orders <- job:
		svr.jobs.AddBenchJob(job)
	default:
		return nil, errors.New("too many jobs waitting !")
	}
	return job, nil
}

func (svr *Server) AbortJob(jobID int64, note string) error {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	job := svr.jobs.GetByID(jobID)
	if job == nil {
		return fmt.Errorf("job '%d' not found !", jobID)
	}

	if err := job.Abort(note); err != nil {
		log.Warnf(err.Error())
	}
	return nil
}

func (svr *Server) GetJob(jobID int64) *BenchmarkJob {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	job := svr.jobs.GetByID(jobID)
	if job == nil {
		return nil
	}

	clone := NewBenchmarkJob()
	DeepCopy(clone, job)

	return clone
}

func (svr *Server) GetHistoryJobs(lastN int) []*BenchmarkJob {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	size := svr.jobs.Size()
	if lastN == 0 {
		lastN = defaultHistoryShowCount
	}
	if lastN < size {
		size = lastN
	}

	jobs := svr.jobs.List()
	cloneJobs := make([]*BenchmarkJob, 0, len(jobs))
	for _, job := range jobs {
		clone := NewBenchmarkJob()
		DeepCopy(clone, job)
		cloneJobs = append(cloneJobs, clone)
	}

	return cloneJobs
}
