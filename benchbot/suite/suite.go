package suite

import (
	"database/sql"
	"fmt"

	"github.com/BurntSushi/toml"
	"golang.org/x/net/context"
)

type BenchCase interface {
	String() string
	Run(context.Context, *sql.DB) (*CaseResult, error)
}

type BenchSuite interface {
	String() string
	Run(context.Context, *sql.DB) ([]*CaseResult, error)
}

type BenchSuiteConfig struct {
	Suites map[string]toml.Primitive
}

func NewBenchSuites(fpath string) ([]BenchSuite, error) {
	benchCfg := new(BenchSuiteConfig)
	meta, err := toml.DecodeFile(fpath, benchCfg)
	if err != nil {
		return nil, err
	}

	suites := make([]BenchSuite, 0, 10)
	for name, value := range benchCfg.Suites {
		switch name {
		case "simple-ops":
			cfg := new(SimpleOpsConfig)
			if err := meta.PrimitiveDecode(value, cfg); err != nil {
				return nil, err
			}
			suites = append(suites, NewSimpleOpsSuite(cfg))
		case "simple-oltp":
			cfg := new(SimpleOLTPConfig)
			if err := meta.PrimitiveDecode(value, cfg); err != nil {
				return nil, err
			}
			suites = append(suites, NewSimpleOLTPSuite(cfg))
		case "block-write":
			cfg := new(BlockWriteConfig)
			if err := meta.PrimitiveDecode(value, cfg); err != nil {
				return nil, err
			}
			suites = append(suites, NewBlockWriteSuite(cfg))
		default:
			return nil, fmt.Errorf("unknown bench suite %s", name)
		}
	}
	return suites, nil
}
