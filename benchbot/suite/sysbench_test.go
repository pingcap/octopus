package suite

import (
	"log"
	"testing"
)

func TestSysbench(t *testing.T) {
	cfg := &SysbenchConfig{
		ScriptsDir: "sysbench_scripts",
	}

	s := NewSysbenchSuite(cfg)
	cases := []BenchCase{
		NewSysbenchCase(s, "oltp"),
		NewSysbenchCase(s, "select"),
		NewSysbenchCase(s, "insert"),
		NewSysbenchCase(s, "delete"),
	}

	if _, err := RunBenchCases(cases, nil); err != nil {
		log.Fatal(err)
	}
}
