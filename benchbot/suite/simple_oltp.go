package suite

import (
	"fmt"
	_ "math"
	"math/rand"
	"time"

	"database/sql"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

/*
Cases :
	-- SQL : select `string` where id = xxxx
	-- SQL : select `string` where id between 1 and 0xff
	-- SQL : select sum(`number`) where id between 1 and 0xff
	-- SQL : select `string` where id between 1 and 0xff order by `field`
	-- SQL : select distinct `string` where id between 1 and 0xff
	-- SQL : update set `number`=`number`+1 where `id` = XXX
	-- SQL : update set ``string``=`xxxxxxx where `id` = XXX
	-- SQL : delete where `id` = XXX
	-- SQL : insert into (`id`, `number`, `string`) values (xx, xxx, xxxx)
*/

type SimpleOLTPConfig struct {
	TableSize   int `toml:"table_size"`
	NumTables   int `toml:"num_tables"`
	NumThreads  int `toml:"num_threads"`
	NumRequests int `toml:"num_requests"`
}

type SimpleOLTPSuite struct {
	cfg    *SimpleOLTPConfig
	tables []string
}

func NewSimpleOLTPSuite(cfg *SimpleOLTPConfig) *SimpleOLTPSuite {
	return &SimpleOLTPSuite{cfg: cfg}
}

func (s *SimpleOLTPSuite) String() string {
	return "simple-oltp"
}

func (s *SimpleOLTPSuite) Run(ctx context.Context, db *sql.DB) ([]*CaseResult, error) {
	if err := s.prepare(db); err != nil {
		return nil, err
	}

	cases := []BenchCase{
		NewSimpleOLTPCase(s),
	}

	results := make([]*CaseResult, 0, len(cases))
	for _, c := range cases {
		res, err := c.Run(ctx, db)
		if err != nil {
			log.Errorf("[case:%s] run: %s", c, err)
			continue
		}
		log.Infof("[case:%s] end: %s", c, res.Summary.FormatJSON())
		results = append(results, res)
	}

	return results, nil
}

func (s *SimpleOLTPSuite) prepare(db *sql.DB) (err error) {
	log.Infof("[suite:%s] prepare (table_size = %d / num_tables = %d)",
		s, s.cfg.TableSize, s.cfg.NumTables)
	s.tables, err = prepare(db, s.cfg.TableSize, s.cfg.NumTables, s.cfg.NumThreads)
	return
}

type SimpleOLTPCase struct {
	suite *SimpleOLTPSuite
}

func NewSimpleOLTPCase(suite *SimpleOLTPSuite) *SimpleOLTPCase {
	return &SimpleOLTPCase{suite: suite}
}

func (c *SimpleOLTPCase) String() string {
	return "simple-oltp"
}

func (c *SimpleOLTPCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	log.Infof("[case:%s] start ...", c)

	cfg := c.suite.cfg
	stat := processStatistic(ctx, db, cfg.NumThreads, cfg.NumRequests, c.execute)
	result := &CaseResult{
		Name:    c.String(),
		Summary: stat.summary,
		Stages:  stat.stages,
	}

	return result, nil
}

func (c *SimpleOLTPCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	cfg := c.suite.cfg
	tables := c.suite.tables
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	var query, name string
	for i := 0; i < requests; i++ {
		table := tables[rander.Intn(len(tables))]
		id := rander.Intn(cfg.TableSize)

		tx, err := db.Begin()
		if err != nil {
			continue
		}

		query = fmt.Sprintf("SELECT `name` FROM `%s` WHERE `id` = ?", table)
		tx.QueryRow(query, id).Scan(&name)

		query = fmt.Sprintf("DELETE FROM `%s` WHERE `id` = ?", table)
		tx.Exec(query, id)

		query = fmt.Sprintf("INSERT INTO `%s` (`id`, `seq`, `name`) VALUES (?, 1, ?)", table)
		tx.Exec(query, id, name)

		query = fmt.Sprintf("UPDATE `%s` SET `seq` = `seq` + 1 WHERE `id` = ?", table)
		tx.Exec(query, id)

		tx.Commit()
	}
}
