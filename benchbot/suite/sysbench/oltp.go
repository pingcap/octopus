package sysbench

import (
	"database/sql"
	"fmt"
	"math/rand"

	"github.com/BurntSushi/toml"

	. "github.com/pingcap/octopus/benchbot/cluster"
	. "github.com/pingcap/octopus/benchbot/suite"
)

func init() {
	builder := func(meta toml.MetaData, value toml.Primitive) BenchSuite {
		cfg := new(SysbenchOLTPConfig)
		meta.PrimitiveDecode(value, cfg)
		return NewSysbenchOLTPSuite(cfg)
	}
	RegisterBenchSuite("sysbench-oltp", builder)
}

type SysbenchOLTPConfig struct {
	TableSize   int `toml:"table_size"`
	NumTables   int `toml:"num_tables"`
	NumThreads  int `toml:"num_threads"`
	NumRequests int `toml:"num_requests"`
}

type SysbenchOLTPSuite struct {
	cfg    *SysbenchOLTPConfig
	tables []string
}

func NewSysbenchOLTPSuite(cfg *SysbenchOLTPConfig) *SysbenchOLTPSuite {
	return &SysbenchOLTPSuite{cfg: cfg}
}

func (s *SysbenchOLTPSuite) Name() string {
	return "sysbench-oltp"
}

func (s *SysbenchOLTPSuite) Run(cluster Cluster) ([]*CaseResult, error) {
	if err := cluster.Start(); err != nil {
		return nil, err
	}
	defer cluster.Reset()
	return s.run(cluster.Accessor())
}

func (s *SysbenchOLTPSuite) run(db *sql.DB) ([]*CaseResult, error) {
	if err := s.prepare(db); err != nil {
		return nil, err
	}

	cases := []BenchCase{
		NewSysbenchOLTPCase(s),
	}

	return RunBenchCases(cases, db)
}

func (s *SysbenchOLTPSuite) prepare(db *sql.DB) error {
	tables, err := prepare(db, s.cfg.TableSize, s.cfg.NumTables, s.cfg.NumThreads)
	if err == nil {
		s.tables = tables
	}
	return err
}

type SysbenchOLTPCase struct {
	cfg    *SysbenchOLTPConfig
	tables []string
}

func NewSysbenchOLTPCase(s *SysbenchOLTPSuite) *SysbenchOLTPCase {
	return &SysbenchOLTPCase{
		cfg:    s.cfg,
		tables: s.tables,
	}
}

func (c *SysbenchOLTPCase) Name() string {
	return "sysbench-oltp"
}

func (c *SysbenchOLTPCase) Run(db *sql.DB) (*CaseResult, error) {
	return ParallelSQLBench(c, c.execute, c.cfg.NumThreads, c.cfg.NumRequests, db)
}

func (c *SysbenchOLTPCase) execute(db *StatDB, rander *rand.Rand) error {
	table := c.tables[rander.Intn(len(c.tables))]

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	var stmt, field string

	stmt = fmt.Sprintf("SELECT `c` FROM `%s` WHERE `id` = ?", table)
	tx.QueryRow(stmt, rander.Intn(c.cfg.TableSize)).Scan(&field)

	stmt = fmt.Sprintf("UPDATE `%s` SET `k` = `k` + 1 WHERE `id` = ?", table)
	tx.Exec(stmt, rander.Intn(c.cfg.TableSize))

	id := rander.Intn(c.cfg.TableSize) + 1
	columnK := rander.Intn(c.cfg.TableSize)
	columnC := RandomAsciiBytes(rander, sysbenchColumnCSize)
	columnPad := RandomAsciiBytes(rander, sysbenchColumnPadSize)
	stmt = fmt.Sprintf("DELETE FROM `%s` WHERE `id` = ?", table)
	tx.Exec(stmt, id)
	stmt = fmt.Sprintf("INSERT INTO `%s` (`id`, `k`, `c`, `pad`) VALUES (?, ?, ?, ?)", table)
	tx.Exec(stmt, id, columnK, columnC, columnPad)

	return tx.Commit()
}
