package suite

import (
	"database/sql"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"

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

type ExecFunc func(*rand.Rand)

func ParallelExec(exec ExecFunc, duration Duration, numThreads int) {
	wg := sync.WaitGroup{}
	ctx, stop := context.WithCancel(context.Background())

	t := time.NewTimer(duration.Duration)
	for id := 0; id < numThreads; id++ {
		go func(id int) {
			wg.Add(1)
			defer wg.Done()
			rander := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for {
				select {
				case <-ctx.Done():
					return
				default:
					exec(rander)
				}
			}
			wg.Done()
		}(id)
	}
	<-t.C

	stop()
	wg.Wait()
}

type BenchFunc func(*rand.Rand) (OPKind, error)

func ParallelBench(c BenchCase, exec BenchFunc, duration Duration, numThreads int) (*CaseResult, error) {
	stat := NewStatManager()
	stat.Start()

	f := func(rander *rand.Rand) {
		start := time.Now()
		op, err := exec(rander)
		if err != nil {
			log.Errorf("[case:%s] exec: %s", c.Name(), err)
		}
		stat.Record(op, err, time.Since(start))
	}

	ParallelExec(f, duration, numThreads)

	stat.Close()
	return NewCaseResult(c.Name(), stat.Result()), nil
}

type SQLBenchFunc func(*StatDB, *rand.Rand) error

func ParallelSQLBench(c BenchCase, exec SQLBenchFunc, duration Duration, numThreads int, db *sql.DB) (*CaseResult, error) {
	stdb := NewStatDB(db)
	stdb.Start()

	f := func(rander *rand.Rand) {
		if err := exec(stdb, rander); err != nil {
			log.Errorf("[case:%s] exec: %s", c.Name(), err)
		}
	}

	ParallelExec(f, duration, numThreads)

	stdb.Close()
	return NewCaseResult(c.Name(), stdb.Result()), nil
}
