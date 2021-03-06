package ycsb

import (
	"log"
	"testing"
	"time"

	. "github.com/pingcap/octopus/benchbot/suite"
)

func TestYCSB(t *testing.T) {
	cfg := &YCSBConfig{
		Databases: []string{
			"raw://127.0.0.1:2379",
			"txn://127.0.0.1:2379",
			"tidb://root@tcp(127.0.0.1:4000)/test",
		},
		Workloads:  []string{"a", "b"},
		Duration:   Duration{Duration: time.Second * 10},
		NumThreads: 3,
	}

	s := NewYCSBSuite(cfg)

	for _, dbURL := range s.cfg.Databases {
		for _, workload := range s.cfg.Workloads {
			c := NewYCSBCase(s, dbURL, workload)
			if c == nil {
				log.Fatal(c)
			}
			if _, err := RunBenchCase(c, nil); err != nil {
				log.Fatal(err)
			}
		}
	}
}
