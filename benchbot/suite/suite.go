package suite

import (
	"database/sql"
	"sync"

	"github.com/BurntSushi/toml"

	. "github.com/pingcap/octopus/benchbot/cluster"
)

type BenchCase interface {
	Name() string
	Run(*sql.DB) (*CaseResult, error)
}

type BenchSuite interface {
	Name() string
	Run(Cluster) ([]*CaseResult, error)
}

type BenchSuiteConfigs struct {
	Suites map[string]toml.Primitive
}

func NewBenchSuites(fpath string) ([]BenchSuite, error) {
	cfgs := new(BenchSuiteConfigs)
	meta, err := toml.DecodeFile(fpath, cfgs)
	if err != nil {
		return nil, err
	}

	suites := make([]BenchSuite, 0)
	for name, value := range cfgs.Suites {
		if builder, ok := benchSuiteBuilders[name]; ok {
			suites = append(suites, builder(meta, value))
		}
	}
	return suites, nil
}

type BenchSuiteBuilder func(toml.MetaData, toml.Primitive) BenchSuite

var (
	benchSuiteMutex    sync.Mutex
	benchSuiteBuilders map[string]BenchSuiteBuilder
)

func RegisterBenchSuite(name string, builder BenchSuiteBuilder) {
	benchSuiteMutex.Lock()
	defer benchSuiteMutex.Unlock()
	if benchSuiteBuilders == nil {
		benchSuiteBuilders = make(map[string]BenchSuiteBuilder)
	}
	benchSuiteBuilders[name] = builder
}