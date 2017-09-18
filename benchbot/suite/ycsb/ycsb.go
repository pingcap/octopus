package ycsb

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/BurntSushi/toml"

	. "github.com/pingcap/octopus/benchbot/cluster"
	. "github.com/pingcap/octopus/benchbot/suite"
)

func init() {
	builder := func(meta toml.MetaData, value toml.Primitive) BenchSuite {
		cfg := new(YCSBConfig)
		meta.PrimitiveDecode(value, cfg)
		return NewYCSBSuite(cfg)
	}
	RegisterBenchSuite("ycsb", builder)
}

const (
	ycsbTableName   = "ycsb"
	ycsbTableSchema = `(
        id BIGINT NOT NULL,
        FIELD1 TEXT,
		FIELD2 TEXT,
		FIELD3 TEXT,
		FIELD4 TEXT,
		FIELD5 TEXT,
		FIELD6 TEXT,
		FIELD7 TEXT,
		FIELD8 TEXT,
		FIELD9 TEXT,
		FIELD10 TEXT,
        PRIMARY KEY (id)
	)`
	ycsbTableFieldsCount  = 10
	ycsbTableFieldsLength = 100
)

type YCSBConfig struct {
	Databases  []string `toml:"databases"`
	Workloads  []string `toml:"workloads"`
	Duration   Duration `toml:"duration"`
	NumThreads int      `toml:"num_threads"`
}

type YCSBSuite struct {
	cfg *YCSBConfig
}

func NewYCSBSuite(cfg *YCSBConfig) *YCSBSuite {
	return &YCSBSuite{cfg: cfg}
}

func (s *YCSBSuite) Name() string {
	return "ycsb"
}

func (s *YCSBSuite) Run(cluster Cluster) ([]*CaseResult, error) {
	var cases []BenchCase
	for _, dbURL := range s.cfg.Databases {
		for _, workload := range s.cfg.Workloads {
			c := NewYCSBCase(s, dbURL, workload)
			if c == nil {
				return nil, fmt.Errorf("failed to create case %s", dbURL)
			}
			cases = append(cases, c)
		}
	}

	return RunBenchCasesWithReset(cases, cluster)
}

type YCSBCase struct {
	cfg       *YCSBConfig
	name      string
	dbURL     string
	rowID     uint64
	readRatio float32
}

func NewYCSBCase(s *YCSBSuite, dbURL, workload string) *YCSBCase {
	seps := strings.SplitN(dbURL, "://", 2)
	if len(seps) != 2 {
		return nil
	}
	mode := strings.ToLower(seps[0])
	workload = strings.ToLower(workload)
	name := fmt.Sprintf("ycsb-%s-%s", mode, workload)

	var readRatio float32
	switch workload {
	case "a":
		readRatio = 0.50
	case "b":
		readRatio = 0.95
	case "c":
		readRatio = 1.00
	case "f":
		readRatio = 0.00
	default:
		return nil
	}

	return &YCSBCase{
		cfg:       s.cfg,
		name:      name,
		dbURL:     dbURL,
		rowID:     0,
		readRatio: readRatio,
	}
}

func (c *YCSBCase) Name() string {
	return c.name
}

func (c *YCSBCase) Run(*sql.DB) (*CaseResult, error) {
	db, err := SetupDatabase(c.dbURL)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	prepare := func(rander *rand.Rand) (OPKind, error) {
		return OPWrite, c.prepare(db, rander)
	}
	_, err = ParallelBench(c, prepare, c.cfg.Duration, c.cfg.NumThreads)
	if err != nil {
		return nil, err
	}

	execute := func(rander *rand.Rand) (OPKind, error) {
		return c.execute(db, rander)
	}
	return ParallelBench(c, execute, c.cfg.Duration, c.cfg.NumThreads)
}

func (c *YCSBCase) prepare(db Database, rander *rand.Rand) error {
	return c.insertRow(db, rander)
}

func (c *YCSBCase) execute(db Database, rander *rand.Rand) (OPKind, error) {
	if rander.Float32() < c.readRatio {
		return OPRead, c.readRow(db, rander)
	}

	return OPWrite, c.insertRow(db, rander)
}

func (c *YCSBCase) readRow(db Database, rander *rand.Rand) error {
	maxID := atomic.LoadUint64(&c.rowID)
	rowID := uint64(rander.Int63n(int64(maxID)))
	_, err := db.ReadRow(rowID)
	return err
}

func (c *YCSBCase) insertRow(db Database, rander *rand.Rand) error {
	rowID := atomic.AddUint64(&c.rowID, 1)
	fields := make([]string, ycsbTableFieldsCount)
	for i := 0; i < len(fields); i++ {
		fields[i] = string(RandomAsciiBytes(rander, ycsbTableFieldsLength))
	}
	return db.InsertRow(rowID, fields)
}
