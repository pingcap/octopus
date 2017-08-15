package suite

import (
	"database/sql"
	"math"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	. "github.com/pingcap/octopus/benchbot/cluster"
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

func RunBenchCasesWithReset(cases []BenchCase, cluster Cluster) ([]*CaseResult, error) {
	results := make([]*CaseResult, 0, len(cases))
	for _, c := range cases {
		if err := cluster.Start(); err != nil {
			return nil, err
		}
		db := cluster.Accessor()
		caseRes, caseErr := RunBenchCase(c, db)
		if err := cluster.Reset(); err != nil {
			return nil, err
		}
		if caseErr != nil {
			return nil, caseErr
		}
		results = append(results, caseRes)
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
			for i := 0; i < requests; i++ {
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
