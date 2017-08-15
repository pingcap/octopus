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
		cfg := new(SysbenchSimpleConfig)
		meta.PrimitiveDecode(value, cfg)
		return NewSysbenchSimpleSuite(cfg)
	}
	RegisterBenchSuite("sysbench-simple", builder)
}

type SysbenchSimpleConfig struct {
	TableSize  int `toml:"table_size"`
	NumTables  int `toml:"num_tables"`
	NumThreads int `toml:"num_threads"`
	NumSelects int `toml:"num_selects"`
	NumInserts int `toml:"num_inserts"`
	NumDeletes int `toml:"num_deletes"`
}

type SysbenchSimpleSuite struct {
	cfg    *SysbenchSimpleConfig
	tables []string
}

func NewSysbenchSimpleSuite(cfg *SysbenchSimpleConfig) *SysbenchSimpleSuite {
	return &SysbenchSimpleSuite{cfg: cfg}
}

func (s *SysbenchSimpleSuite) Name() string {
	return "sysbench-simple"
}

func (s *SysbenchSimpleSuite) Run(cluster Cluster) ([]*CaseResult, error) {
	if err := cluster.Start(); err != nil {
		return nil, err
	}
	defer cluster.Reset()
	return s.run(cluster.Accessor())
}

func (s *SysbenchSimpleSuite) run(db *sql.DB) ([]*CaseResult, error) {
	if err := s.prepare(db); err != nil {
		return nil, err
	}

	cases := []BenchCase{
		NewSysbenchSelectCase(s),
		NewSysbenchDeleteCase(s),
		NewSysbenchInsertCase(s),
	}

	return RunBenchCases(cases, db)
}

func (s *SysbenchSimpleSuite) prepare(db *sql.DB) error {
	tables, err := prepare(db, s.cfg.TableSize, s.cfg.NumTables, s.cfg.NumThreads)
	if err != nil {
		return err
	}

	for _, table := range tables {
		var count int
		stmt := fmt.Sprintf("SELECT count(*) FROM `%s`", table)
		if err := db.QueryRow(stmt).Scan(&count); err != nil {
			return err
		}
		if count != s.cfg.TableSize {
			return fmt.Errorf("expect count %d, found %d", s.cfg.TableSize, count)
		}
	}

	s.tables = tables
	return nil
}

type SysbenchSelectCase struct {
	cfg    *SysbenchSimpleConfig
	tables []string
}

func NewSysbenchSelectCase(s *SysbenchSimpleSuite) *SysbenchSelectCase {
	return &SysbenchSelectCase{
		cfg:    s.cfg,
		tables: s.tables,
	}
}

func (c *SysbenchSelectCase) Name() string {
	return "sysbench-select"
}

func (c *SysbenchSelectCase) Run(db *sql.DB) (*CaseResult, error) {
	return ParallelSQLBench(c, c.execute, c.cfg.NumThreads, c.cfg.NumSelects, db)
}

func (c *SysbenchSelectCase) execute(db *StatDB, rander *rand.Rand) error {
	table := c.tables[rander.Intn(len(c.tables))]
	id := rander.Intn(c.cfg.TableSize) + 1

	var pad string
	stmt := fmt.Sprintf("SELECT `pad` FROM `%s` WHERE `id` = ?", table)
	return db.QueryRow(stmt, id).Scan(&pad)
}

type SysbenchInsertCase struct {
	cfg    *SysbenchSimpleConfig
	tables []string
}

func NewSysbenchInsertCase(s *SysbenchSimpleSuite) *SysbenchInsertCase {
	return &SysbenchInsertCase{
		cfg:    s.cfg,
		tables: s.tables,
	}
}

func (c *SysbenchInsertCase) Name() string {
	return "sysbench-insert"
}

func (c *SysbenchInsertCase) Run(db *sql.DB) (*CaseResult, error) {
	return ParallelSQLBench(c, c.execute, c.cfg.NumThreads, c.cfg.NumInserts, db)
}

func (c *SysbenchInsertCase) execute(db *StatDB, rander *rand.Rand) error {
	table := c.tables[rander.Intn(len(c.tables))]
	columnK := rander.Intn(c.cfg.TableSize)
	columnC := RandomAsciiBytes(rander, sysbenchColumnCSize)
	columnPad := RandomAsciiBytes(rander, sysbenchColumnPadSize)

	stmt := fmt.Sprintf("INSERT INTO `%s` (`k`, `c`, `pad`) VALUES (?, ?, ?)", table)
	_, err := db.Exec(stmt, columnK, columnC, columnPad)
	return err
}

type SysbenchDeleteCase struct {
	cfg    *SysbenchSimpleConfig
	tables []string
}

func NewSysbenchDeleteCase(s *SysbenchSimpleSuite) *SysbenchDeleteCase {
	return &SysbenchDeleteCase{
		cfg:    s.cfg,
		tables: s.tables,
	}
}

func (c *SysbenchDeleteCase) Name() string {
	return "sysbench-delete"
}

func (c *SysbenchDeleteCase) Run(db *sql.DB) (*CaseResult, error) {
	return ParallelSQLBench(c, c.execute, c.cfg.NumThreads, c.cfg.NumDeletes, db)
}

func (c *SysbenchDeleteCase) execute(db *StatDB, rander *rand.Rand) error {
	table := c.tables[rander.Intn(len(c.tables))]
	id := rander.Intn(c.cfg.TableSize) + 1

	stmt := fmt.Sprintf("DELETE FROM `%s` WHERE `id` = ?", table)
	_, err := db.Exec(stmt, id)
	return err
}
