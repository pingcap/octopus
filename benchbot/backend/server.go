package backend

import (
	"errors"
	"sync"

	"golang.org/x/net/context"
)

const (
	penndingJobLimit    int = 10
	defHistoryShowCount int = 10
)

type Server struct {
	wg  sync.WaitGroup
	mux sync.Mutex
	ctx context.Context

	uuidAllocator *UUIDAllocator

	jobs         chan *BenchmarkJob
	runningJob   *BenchmarkJob
	historys     []*BenchmarkJob
	historysByID map[int64]*BenchmarkJob
}

type Summary struct {
	RunningCount int
	PendingCount int
	HistoryCount int
}

func NewServer(cfg *ServerConfig, ctx context.Context) *Server {
	svr := new(Server)
	svr.wg = sync.WaitGroup{}
	svr.ctx = ctx
	svr.uuidAllocator = NewUUIDAllocator()

	svr.runningJob = nil
	svr.jobs = make(chan *BenchmarkJob, penndingJobLimit)
	svr.historys = make([]*BenchmarkJob, 0, 1024) // TODO ...
	svr.historysByID = make(map[int64]*BenchmarkJob)

	go svr.mainloop()

	return svr
}

/*
func (svr *Server) initLogger(logRoot string) error {
	return nil
}
*/

func (svr *Server) Close() {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	if svr.runningJob != nil {
		svr.runningJob.Abort("close server")
	}
	close(svr.jobs)
	svr.wg.Wait()
}

func (svr *Server) DumpSummary() *Summary {
	info := &Summary{
		PendingCount: len(svr.jobs),
		HistoryCount: len(svr.historys),
		RunningCount: 0,
	}

	if svr.runningJob != nil {
		info.RunningCount = 1
	}

	return info
}

func (svr *Server) mainloop() {
	svr.wg.Add(1)
	defer svr.wg.Done()

	var err error
	var job *BenchmarkJob
	var ok, end bool = false, false

	for !end {
		select {
		case <-svr.ctx.Done():
			end = true
		case job, ok = <-svr.jobs:
			if !ok {
				job = nil
			}
		}

		if job == nil || job.Stat() != Pending {
			continue
		}

		svr.setRunningJob(job)
		if err = job.Run(); err != nil {
			// fmt.Printf("[%d] job run failed : %s !\n", job.ID, err.Error())
			// TODO ... replace by logger
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
	svr.mux.Lock()
	defer svr.mux.Unlock()

	var err error = nil
	select {
	case svr.jobs <- job:
		svr.historys = append(svr.historys, job)
		svr.historysByID[job.ID] = job
		break
	default:
		return errors.New("too many jobs waitting !")
	}
	return err
}

func (svr *Server) AbortJob(jobID int64, note string) bool {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	if job, ok := svr.historysByID[jobID]; ok {
		return job.Abort(note)
	}
	return false
}

func (svr *Server) GetJob(jobID int64) *BenchmarkJob {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	if job, ok := svr.historysByID[jobID]; ok {
		return job
	}
	return nil
}

func (svr *Server) GetHistoryJobs(lastN int) []*BenchmarkJob {
	svr.mux.Lock()
	defer svr.mux.Unlock()

	size := len(svr.historys)
	if lastN == 0 {
		lastN = defHistoryShowCount
	}
	if lastN < size {
		size = lastN
	}

	his := make([]*BenchmarkJob, size)
	for i := 0; i < size; i++ {
		his[i] = svr.historys[i]
	}

	return his
}
