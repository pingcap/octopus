package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/benchbot/cluster"
)

//InitBuildJob return buildjob
func (r *Run) InitBuildJob() *BuildJob {
	return &BuildJob{
		Infomation: &cluster.BinPackage{},
	}
}

//BuildJobIsFinished return build is finished
func (r *Run) BuildJobIsFinished(bj *BuildJob) bool {
	return bj.Status == "finished"
}

//CreateBuildJob for call build
func (r *Run) CreateBuildJob(bj *BuildJob) (string, error) {
	newBj := r.InitBuildJob()
	err := r.HTTPPost(BuildBaseURL, bj, newBj)
	if err != nil || newBj.BuildID == "" {
		return "", errors.Errorf("can not http<post> %v with error %v", BuildBaseURL, err)
	}

	return newBj.BuildID, nil
}

//GetBuildJobByBuildID get buildjob by buildid
func (r *Run) GetBuildJobByBuildID(btjID, buildID string) (*BenchTestJob, error) {
	if btjID != "" {
		bt, err := r.GetBenchTestJobByJobID(btjID)
		if err != nil {
			return nil, errors.Errorf("can not get bench id %s build id %s ", btjID, buildID)
		}
		if _, ok := bt.BuildJobs[buildID]; ok {
			return bt, nil
		}
		return nil, errors.Errorf("can not get build id %s from bench id %s", buildID, btjID)
	}
	for i := 0; i < len(r.BenchTestJobs); i++ {
		bt := <-r.BenchTestJobs
		r.BenchTestJobs <- bt
		if _, ok := bt.BuildJobs[buildID]; ok {
			return bt, nil
		}
	}
	return nil, errors.Errorf("can not get build id %s ", buildID)
}

//UpdateBuildJob update buildjob information
func (r *Run) UpdateBuildJob(bt *BenchTestJob, newBj *BuildJob) error {
	if !r.BuildJobIsFinished(newBj) {
		return errors.Errorf("buidjob %v is not finished", newBj)
	}

	if _, ok := bt.BuildJobs[newBj.BuildID]; ok {
		bt.BuildJobs[newBj.BuildID] = newBj
		bt.BuildFinishedNum++
		return nil

	}

	return errors.Errorf("buidjob id %s not found", newBj.BuildID)
}

//AchieveBuildJobByBuildID achieve buildjob information
func (r *Run) AchieveBuildJobByBuildID(buildID string) (*BuildJob, error) {
	newBj := r.InitBuildJob()
	getBuildJob := fmt.Sprintf("%s/%s", BuildBaseURL, buildID)
	err := r.HTTPGet(getBuildJob, newBj)
	if err != nil || newBj.BuildID == "" {
		return newBj, errors.Errorf("can not http<get> %v with error %v", getBuildJob, err)
	}

	return newBj, nil

}
