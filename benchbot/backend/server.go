package backend

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const penndingJobLimit int = 10

type Server struct {
	end          bool
	wg           sync.WaitGroup
	jobs         chan *TestJob
	runningJob   *TestJob
	historys     []*TestJob
	historysByID map[string]*TestJob
}

type SummaryInfo struct {
	RunningCount int
	PendingCount int
	HistoryCount int
}

func NewServer() *Server {
	svr := new(Server)
	svr.end = false
	svr.runningJob = nil
	svr.jobs = make(chan *TestJob, penndingJobLimit)
	svr.historys = make([]*TestJob, 0) // TODO ...
	svr.historysByID = make(map[string]*TestJob)
	svr.wg = sync.WaitGroup{}

	go svr.mainloop()
	svr.wg.Wait()

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

	//for job := range svr.jobs {
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
			fmt.Printf("[%s] job run failed : s !\n", job.Id, err.Error())
		}
		svr.runningJob = nil
		svr.wg.Done()
	}
	return nil
}

func (svr *Server) LaunchTesting(job *TestJob) error {
	var err error = nil
	select {
	case <-time.After(time.Second * 1):
		err = errors.New("too many jobs waitting !")
		break
	case svr.jobs <- job:
		svr.historys = append(svr.historys, job)
		svr.historysByID[job.Id] = job
		break
	}
	return err
}

func (svr *Server) AbortTesting(jobId string) bool {
	/*if svr.runningJob != nil && svr.runningJob.Id == jobId {
		svr.runningJob.Abort("")
		return true
	}*/
	if job, ok := svr.historysByID[jobId]; ok {
		if job.Status == Pending || job.Status == Testing {
			job.Abort("")
			return true
		}
	}
	return false
}

func (svr *Server) GetJob(jobId string) *TestJob {
	if job, ok := svr.historysByID[jobId]; ok {
		return job
	}
	return nil
}

func (svr *Server) GetHistoryJobs(lastN int) []*TestJob {
	size := len(svr.historys)
	if lastN < size {
		size = lastN
	}

	his := make([]*TestJob, 0)
	for i := 0; i < size; i++ {
		his = append(his, svr.historys[i])
	}

	return his
}
