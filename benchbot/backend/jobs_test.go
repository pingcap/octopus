package backend

import (
	"fmt"
	"os"
	"testing"

	_ "github.com/pingcap/octopus/benchbot/suite"
)

func checkErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func assert(res bool, msg string) {
	if !res {
		panic(msg)
	}
}

// func TestConvert(t *testing.T) {
// 	dumpFile := "/data1/octopus/benchbot/jobs.db"
// 	jobSet, _ := NewJobSet(dumpFile)
// 	defer jobSet.Close()

// 	for i := 0; i < jobSet.Size(); i++ {
// 		jobid := int64(i + 10)
// 		fmt.Println(i, "============================================")
// 		job := jobSet.GetByID(jobid)
// 		if job != nil {
// 			job.Result.Message = ""
// 			for _, caseRet := range job.Result.Details {
// 				if len(caseRet.Stages) > 50 {
// 					// newStages := make([]*)
// 				}
// 			}
// 		}
// 	}

// 	return
// }

func TestJobSet(t *testing.T) {
	dumpFile := "jobs.db"
	defer os.Remove(dumpFile)

	var err error
	var job *BenchmarkJob
	var jobSet *JobSet

	{
		jobSet, err = NewJobSet(dumpFile)
		defer jobSet.Close()
		checkErr(err)

		job = NewBenchmarkJob()
		job.Status = "abc"
		err = jobSet.AddBenchJob(job)
		checkErr(err)
		assert(job.ID == 1, fmt.Sprintf("%#v", job))

		job = NewBenchmarkJob()
		job.Status = "zxy"
		err = jobSet.AddBenchJob(job)
		checkErr(err)
		assert(job.ID == 2, fmt.Sprintf("%#v", job))
	}

	{
		jobSet, err = NewJobSet(dumpFile)
		defer jobSet.Close()
		checkErr(err)

		size := jobSet.Size()
		assert(size > 0, fmt.Sprintf("SIZE = %d", size))

		for id := 1; id <= size; id++ {
			job = jobSet.GetByID(int64(id))
			assert(job.ID == int64(id), fmt.Sprintf("id=%d VS %#v", id, job))
		}

		job = NewBenchmarkJob()
		job.Status = "ABCDEF"
		err = jobSet.AddBenchJob(job)
		checkErr(err)
		assert(job.ID == 3, fmt.Sprintf("%#v", job))

		job = NewBenchmarkJob()
		job.Status = "NBAcba"
		err = jobSet.AddBenchJob(job)
		checkErr(err)
		assert(job.ID == 4, fmt.Sprintf("%#v", job))

		job.Meta.Creator = "CQC"
		job.Result.Message = "AC !"

		jobUpdated := jobSet.GetByID(job.ID)
		assert(jobUpdated != nil && jobUpdated.ID == job.ID &&
			jobUpdated.Meta.Creator == job.Meta.Creator &&
			jobUpdated.Result.Message == job.Result.Message,
			fmt.Sprintf("updated_job -> %#v", jobUpdated))

		job = jobSet.GetByID(5)
		assert(job == nil, fmt.Sprintf("%#v", job))
	}

	{
		jobSet, err = NewJobSet(dumpFile)
		defer jobSet.Close()
		checkErr(err)

		size := jobSet.Size()
		assert(size > 0, fmt.Sprintf("SIZE = %d", size))

		for id := 1; id <= size; id++ {
			jobSet.GetByID(int64(id))
		}

		list := jobSet.List()
		assert(len(list) == jobSet.Size(), fmt.Sprintf("%d vs %d", len(list), jobSet.Size()))
	}
}
