package backend

import (
	"fmt"
	"log"
	"testing"
)

func assert(res bool, msg string) {
	if !res {
		panic(msg)
	}
}

func TestJobSet(t *testing.T) {
	js, err := NewJobSet("jobs.db")
	if err != nil {
		log.Fatal(err)
	}

	job := NewBenchmarkJob()
	job.Status = "1"
	if err := js.AddJob(job); err != nil {
		log.Fatal(err)
	}
	assert(job.ID == 1, fmt.Sprintf("%#v", job))
	assert(js.Size() == 1, fmt.Sprintf("%d", js.Size()))

	job = NewBenchmarkJob()
	job.Status = "2"
	if err := js.AddJob(job); err != nil {
		log.Fatal(err)
	}
	assert(job.ID == 2, fmt.Sprintf("%#v", job))
	assert(js.Size() == 2, fmt.Sprintf("%d", js.Size()))

	js.Close()

	js, err = NewJobSet("jobs.db")
	if err != nil {
		log.Fatal(err)
	}

	job = js.GetJob(1)
	assert(job.ID == 1, fmt.Sprintf("%#v", job))
	assert(job.Status == "1", fmt.Sprintf("%#v", job))

	job = js.GetJob(2)
	assert(job.ID == 2, fmt.Sprintf("%#v", job))
	assert(job.Status == "2", fmt.Sprintf("%#v", job))

	jobs := js.ListJobs()
	assert(len(jobs) == 2, fmt.Sprintf("%#v", jobs))

	job = NewBenchmarkJob()
	job.Status = "3"
	if err := js.AddJob(job); err != nil {
		log.Fatal(err)
	}
	assert(job.ID == 3, fmt.Sprintf("%#v", job))
	assert(js.Size() == 3, fmt.Sprintf("%d", js.Size()))
}
