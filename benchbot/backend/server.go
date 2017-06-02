package backend

import (
	"errors"
	"os"
	"sync"

	log "github.com/ngaut/log"
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

	conf            *ServerConfig
	ctx             context.Context
	suspendMainloop context.CancelFunc

	uuidAllocator *UUIDAllocator

	runningJob *BenchmarkJob
	orders     chan *BenchmarkJob
	jobs       []*BenchmarkJob
	jobsByID   map[int64]*BenchmarkJob
}

type Summary struct {
	RunningCount int
	PendingCount int
	HistoryCount int
}

func NewServer(cfg *ServerConfig) (*Server, error) {
	svr := &Server{conf: cfg}
	if err := svr.init(); err != nil {
		return nil, err
	}

	svr.ctx, svr.suspendMainloop = context.WithCancel(context.Background())
	svr.uuidAllocator = NewUUIDAllocator()

	svr.runningJob = nil
	svr.orders = make(chan *BenchmarkJob, penndingJobLimit)
	svr.jobs = make([]*BenchmarkJob, 0, 1024) // TODO ...
	svr.jobsByID = make(map[int64]*BenchmarkJob)

	svr.wg.Add(1)
	go svr.mainloop()
	log.Info("Server running ...")

	return svr, nil
}

func (svr *Server) init() (err error) {
	err = os.MkdirAll(svr.conf.Dir, os.ModePerm)
	if err != nil {
		return
	}

	return
}

func (svr *Server) Close() {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	svr.suspendMainloop()
	svr.wg.Wait()
}

func (svr *Server) DumpSummary() *Summary {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	info := &Summary{
		PendingCount: len(svr.orders),
		RunningCount: 0,
		HistoryCount: len(svr.jobs),
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

func (svr *Server) AddJob(job *BenchmarkJob) error {
	var err error = nil
	select {
	case svr.orders <- job:
		// nothing to do
		svr.jobs = append(svr.jobs, job)
		svr.jobsByID[job.ID] = job
	default:
		return errors.New("too many jobs waitting !")
	}
	return err
}

func (svr *Server) AbortJob(jobID int64, note string) bool {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	if job, ok := svr.jobsByID[jobID]; ok {
		return job.Abort(note)
	}
	return false
}

func (svr *Server) GetJob(jobID int64) *BenchmarkJob {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	if job, ok := svr.jobsByID[jobID]; ok {
		return job
	}
	return nil
}

func (svr *Server) GetHistoryJobs(lastN int) []*BenchmarkJob {
	svr.mux.RLock()
	defer svr.mux.RUnlock()

	size := len(svr.jobs)
	if lastN == 0 {
		lastN = defaultHistoryShowCount
	}
	if lastN < size {
		size = lastN
	}

	his := make([]*BenchmarkJob, size)
	for i := 0; i < size; i++ {
		his[i] = svr.jobs[i]
	}

	return his
}
