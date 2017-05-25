package backend

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	penndingJobLimit int = 10
)

type Server struct {
	end        bool
	wg         sync.WaitGroup
	uuidCenter *UUIDCenter

	jobs         chan *BenchmarkJob
	runningJob   *BenchmarkJob
	historys     []*BenchmarkJob
	historysByID map[int64]*BenchmarkJob
}

type SummaryInfo struct {
	RunningCount int
	PendingCount int
	HistoryCount int
}

func NewServer(cfg *ServerConfig) *Server {
	svr := new(Server)
	svr.end = false
	svr.wg = sync.WaitGroup{}
	svr.uuidCenter = NewUUIDCenter()

	svr.runningJob = nil
	svr.jobs = make(chan *BenchmarkJob, penndingJobLimit)
	svr.historys = make([]*BenchmarkJob, 0, 1024) // TODO ...
	svr.historysByID = make(map[int64]*BenchmarkJob)

	go svr.mainloop()

	return svr
}

func (svr *Server) initLogger(logRoot string) error {
	return nil
}

func (svr *Server) Close() {
	close(svr.jobs)
}

func (svr *Server) DumpInfo(info *SummaryInfo) {
	info.PendingCount = len(svr.jobs)
	info.HistoryCount = len(svr.historys)
	info.RunningCount = 0
	if svr.runningJob != nil {
		info.RunningCount = 1
	}
}

func (svr *Server) mainloop() error {
	var err error
	for !svr.end {
		job, ok := <-svr.jobs
		if !ok {
			break
		}

		if job.Status != Pending {
			continue
		}

		svr.wg.Add(1)
		svr.runningJob = job
		if err = job.Run(); err != nil {
			fmt.Printf("[%d] job run failed : %s !\n", job.ID, err.Error())
			// TODO ... replace by logger
		}
		svr.runningJob = nil
		svr.wg.Done()
	}
	return nil
}

func (svr *Server) ApplyBenchmark(job *BenchmarkJob) error {
	var err error = nil

	select {
	case <-time.After(time.Second * 1):
		err = errors.New("too many jobs waitting !")
		break
	case svr.jobs <- job:
		svr.historys = append(svr.historys, job)
		svr.historysByID[job.ID] = job
		break
	}
	return err
}

func (svr *Server) AbortBenchmark(jobID int64, note string) bool {
	if job, ok := svr.historysByID[jobID]; ok {
		if job.Status == Pending || job.Status == Deploying || job.Status == Running {
			job.Abort(note)
			return true
		}
	}
	return false
}

func (svr *Server) GetJob(jobID int64) *BenchmarkJob {
	if job, ok := svr.historysByID[jobID]; ok {
		return job
	}
	return nil
}

func (svr *Server) GetHistoryJobs(lastN int) []*BenchmarkJob {
	size := len(svr.historys)
	if lastN < size {
		size = lastN
	}

	his := make([]*BenchmarkJob, 0)
	for i := 0; i < size; i++ {
		his = append(his, svr.historys[i])
	}

	return his
}
