package suite

import (
	"database/sql"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ngaut/log"
)

func RunBenchCase(c BenchCase, db *sql.DB) (*CaseResult, error) {
	log.Infof("[case:%s] run", c.Name())
	res, err := c.Run(db)
	if err != nil {
		log.Errorf("[case:%s] err: %s", c.Name(), err)
		return nil, err
	}
	log.Infof("[case:%s] end: %s", c.Name(), res.Stat.FormatJSON())
	return res, err
}

func RunBenchCases(cases []BenchCase, db *sql.DB) ([]*CaseResult, error) {
	results := make([]*CaseResult, 0, len(cases))
	for _, c := range cases {
		res, err := RunBenchCase(c, db)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}

type BenchFunc func(*rand.Rand) (OPKind, error)

func ParallelBench(c BenchCase, exec BenchFunc, numThreads, numRequests int) (*CaseResult, error) {
	stat := NewStatManager()
	stat.Start()

	wg := sync.WaitGroup{}
	avgRequests := int(math.Ceil(float64(numRequests) / float64(numThreads)))
	for id, requests := 0, 0; requests < numRequests; id++ {
		wg.Add(1)
		endRequests := requests + avgRequests
		endRequests = int(math.Min(float64(endRequests), float64(numRequests)))

		go func(id, requests int) {
			rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
			for i := 0; i < requests; id++ {
				start := time.Now()
				op, err := exec(rander)
				if err != nil {
					log.Errorf("[case:%s] exec: %s", c.Name(), err)
				}
				stat.Record(op, err, time.Since(start))
			}
			wg.Done()
		}(id, endRequests-requests)

		requests = endRequests
	}
	wg.Wait()

	stat.Close()
	return NewCaseResult(c.Name(), stat.Result()), nil
}

type SQLBenchFunc func(*StatDB, *rand.Rand) error

func ParallelSQLBench(c BenchCase, exec SQLBenchFunc, numThreads, numRequests int, db *sql.DB) (*CaseResult, error) {
	stdb := NewStatDB(db)
	stdb.Start()

	wg := sync.WaitGroup{}
	avgRequests := int(math.Ceil(float64(numRequests) / float64(numThreads)))
	for id, requests := 0, 0; requests < numRequests; id++ {
		wg.Add(1)
		endRequests := requests + avgRequests
		endRequests = int(math.Min(float64(endRequests), float64(numRequests)))

		go func(id, requests int) {
			rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
			for i := 0; i < requests; i++ {
				if err := exec(stdb, rander); err != nil {
					log.Errorf("[case:%s] exec: %s", c.Name(), err)
				}
			}
			wg.Done()
		}(id, endRequests-requests)

		requests = endRequests
	}
	wg.Wait()

	stdb.Close()
	return NewCaseResult(c.Name(), stdb.Result()), nil
}
