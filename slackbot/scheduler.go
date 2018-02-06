package main

import (
	"github.com/ngaut/log"
	"time"
)

//BuildSchedule build schedule
func (r *Run) BuildSchedule(bt *BenchTestJob) {
	for _, bj := range bt.BuildJobs {
		if !r.BuildJobIsFinished(bj) {
			if newBj, _ := r.AchieveBuildJobByBuildID(bj.BuildID); r.BuildJobIsFinished(newBj) {
				r.UpdateBuildJob(bt, newBj)
			}
		}
	}

}

//BenchTestSchedule benchtest schedule
func (r *Run) BenchTestSchedule(bt *BenchTestJob) {
	if bt.CallBackFlag || (!bt.CallBackFlag && !bt.SlackOutFlag && !r.BenchTestIsFinished(bt) && time.Since(bt.Ctime) > 10*time.Hour) {
		r.UpdateBenchTestJob(bt)
	}

	if r.BenchTestIsFinished(bt) && !bt.SlackOutFlag {
		for i := 0; i < 3; i++ {
			oldBbj, err := r.AchieveBenchTestJobByBenchID(bt.BenchBotID - 1)
			if err == nil {
				r.BenchTestOutMsg(bt.BenchBotJob, oldBbj)
				bt.SlackOutFlag = true
				return
			}
			log.Errorf("get before bench id %d task failed error %v", bt.BenchBotID-1, err)
			time.Sleep(3 * time.Second)
		}
	}

}

//LoopBenchTestJobs loop chan
func (r *Run) LoopBenchTestJobs() {
	for i := 0; i < len(r.BenchTestJobs); i++ {
		bt := <-r.BenchTestJobs
		if bt.BuildFinishedNum < 3 && time.Since(bt.Ctime) > 1*time.Hour {
			r.BuildSchedule(bt)
		} else if bt.BuildFinishedNum >= 3 && bt.BenchBotID == 0 {
			r.CreateBenchTestJob(bt)
		} else if bt.BuildFinishedNum >= 3 && bt.BenchBotID != 0 {
			r.BenchTestSchedule(bt)
		}

		if time.Since(bt.Ctime) < 10*24*time.Hour {
			r.BenchTestJobs <- bt
		}
	}
}

//RunScheduler for schedule
func (r *Run) RunScheduler() {
	ticker := time.NewTicker(BenchScheduleTime)
	defer ticker.Stop()
	for {
		select {
		case <-r.Ctx.Done():
			return
		case <-ticker.C:
			r.LoopBenchTestJobs()
		}
	}
}
