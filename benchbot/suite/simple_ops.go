package suite

import (
	"fmt"
	"math/rand"
	"time"

	"database/sql"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

type SimpleOpsConfig struct {
	NumTables   int `toml:"num_tables"`
	NumThreads  int `toml:"num_threads"`
	NumRequests int `toml:"num_requests"`
}

type SimpleOpsSuite struct {
	cfg    *SimpleOpsConfig
	tables []string
}

func NewSimpleOpsSuite(cfg *SimpleOpsConfig) *SimpleOpsSuite {
	return &SimpleOpsSuite{cfg: cfg}
}

func (s *SimpleOpsSuite) String() string {
	return "simple-ops"
}

func (s *SimpleOpsSuite) Run(ctx context.Context, db *sql.DB) ([]*CaseResult, error) {
	if err := s.prepare(db); err != nil {
		return nil, err
	}

	cases := []BenchCase{
		NewSimpleSelectCase(s),
		NewSimpleDeleteCase(s),
		NewSimpleInsertCase(s),
	}

	results := make([]*CaseResult, 0, len(cases))
	for _, c := range cases {
		log.Infof("[case:%s] start ...", c)
		res, err := c.Run(ctx, db)
		if err != nil {
			log.Errorf("[case:%s] run: %s", c, err)
			continue
		}
		log.Infof("[case:%s] end: %s", c, res.Summary.FormatJSON())
		results = append(results, res)
		time.Sleep(time.Second * 30)
	}

	return results, nil
}

func (s *SimpleOpsSuite) prepare(db *sql.DB) (err error) {
	s.tables, err = prepare(db, s.cfg.NumRequests, s.cfg.NumTables, s.cfg.NumThreads)
	return
}

type SimpleSelectCase struct {
	suite *SimpleOpsSuite
}

func NewSimpleSelectCase(suite *SimpleOpsSuite) *SimpleSelectCase {
	return &SimpleSelectCase{suite: suite}
}

func (c *SimpleSelectCase) String() string {
	return "simple-select"
}

func (c *SimpleSelectCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	cfg := c.suite.cfg
	stat := processStatistic(c.execute, ctx, db, cfg.NumThreads, cfg.NumRequests)
	result := &CaseResult{
		Name:    c.String(),
		Summary: stat.summary,
		Stages:  stat.stages,
	}
	return result, nil
}

func (c *SimpleSelectCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	cfg := c.suite.cfg
	tables := c.suite.tables
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	for i := 0; i < requests; i++ {
		table := tables[rander.Intn(len(tables))]
		id := rander.Intn(cfg.NumRequests)

		query := fmt.Sprintf("SELECT `id` FROM `%s` WHERE `id` = ?", table)
		db.QueryRow(query, id).Scan(&id)
	}
}

type SimpleInsertCase struct {
	suite *SimpleOpsSuite
}

func NewSimpleInsertCase(suite *SimpleOpsSuite) *SimpleInsertCase {
	return &SimpleInsertCase{suite: suite}
}

func (c *SimpleInsertCase) String() string {
	return "simple-insert"
}

func (c *SimpleInsertCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	cfg := c.suite.cfg
	stat := processStatistic(c.execute, ctx, db, cfg.NumThreads, cfg.NumRequests)
	result := &CaseResult{
		Name:    c.String(),
		Summary: stat.summary,
		Stages:  stat.stages,
	}
	return result, nil
}

func (c *SimpleInsertCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	tables := c.suite.tables
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	for i := 0; i < requests/len(tables); i++ {
		name := randomAsciiBytes(defaultStringSize, rander)
		for _, table := range tables {
			query := fmt.Sprintf("INSERT INTO `%s` (`seq`, `name`) VALUES (1, ?)", table)
			if _, err := db.Exec(query, name); err != nil {
				log.Errorf("[case:%s] exec: %s", c, err)
			}
		}
	}
}

type SimpleDeleteCase struct {
	suite *SimpleOpsSuite
}

func NewSimpleDeleteCase(suite *SimpleOpsSuite) *SimpleDeleteCase {
	return &SimpleDeleteCase{suite: suite}
}

func (c *SimpleDeleteCase) String() string {
	return "simple-delete"
}

func (c *SimpleDeleteCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	cfg := c.suite.cfg
	stat := processStatistic(c.execute, ctx, db, cfg.NumThreads, cfg.NumRequests)
	result := &CaseResult{
		Name:    c.String(),
		Summary: stat.summary,
		Stages:  stat.stages,
	}
	return result, nil
}

func (c *SimpleDeleteCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	tables := c.suite.tables

	for i := 0; i < requests/len(tables); i++ {
		rowID := id*requests + i + 1
		for _, table := range tables {
			query := fmt.Sprintf("DELETE FROM `%s` WHERE `id` = ?", table)
			if _, err := db.Exec(query, rowID); err != nil {
				log.Errorf("[case:%s] exec: %s", c, err)
			}
		}
	}
}
