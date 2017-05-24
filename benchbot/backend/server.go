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
	clusterMgr *ClusterManager

	jobs         chan *BenchmarkJob
	runningJob   *BenchmarkJob
	historys     []*BenchmarkJob
	historysByID map[string]*BenchmarkJob
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
	svr.clusterMgr = newClusterManager(cfg)

	svr.runningJob = nil
	svr.jobs = make(chan *BenchmarkJob, penndingJobLimit)
	svr.historys = make([]*BenchmarkJob, 1024) // TODO ...
	svr.historysByID = make(map[string]*BenchmarkJob)

	go svr.mainloop()

	return svr
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
			fmt.Printf("[%s] job run failed : %s !\n", job.ID, err.Error())
		}
		svr.runningJob = nil
		svr.wg.Done()
	}
	return nil
}

// 186 128 900 97
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

func (svr *Server) AbortBenchmark(jobID string, note string) bool {
	if job, ok := svr.historysByID[jobID]; ok {
		if job.Status == Pending || job.Status == Deploying || job.Status == Running {
			job.Abort(note)
			return true
		}
	}
	return false
}

func (svr *Server) GetJob(jobID string) *BenchmarkJob {
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
