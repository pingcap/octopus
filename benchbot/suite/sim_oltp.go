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

const (
	defaultCount = 0

	simOLTPTables         = 16
	simOLTPConcurrency    = 1000
	simOLTPGetSetRequests = 500000
)

type SimpleOLTPSuite struct{}

func NewSimpleOLTPSuite() *SimpleOLTPSuite {
	return new(SimpleOLTPSuite)
}

func (s *SimpleOLTPSuite) String() string {
	return "SimpleOLTPSuite"
}

func (s *SimpleOLTPSuite) Run(ctx context.Context, db *sql.DB) (results []*CaseResult, err error) {
	tables, err := s.initTables(db)
	if err != nil {
		return nil, err
	}

	cases := []BenchCase{
		NewOltpGetSetCase(tables),
	}

	results = make([]*CaseResult, 0, len(cases))
	for _, c := range cases {
		ret, err := c.Run(ctx, db)
		if err == nil {
			log.Infof("case end : %s", ret.Summary.FormatJSON())
			results = append(results, ret)
		}
	}

	return
}

func (s *SimpleOLTPSuite) initTables(db *sql.DB) ([]string, error) {
	log.Infof("[SimpleOLTPSuite] prepare (table = %d / table_rows = %d)", simOpsTables, 0)

	return prepare(db, simOLTPTables, 0)
}

//
type OltpGetSetCase struct {
	desc   string
	tables []string
}

func NewOltpGetSetCase(tables []string) *OltpGetSetCase {
	return &OltpGetSetCase{
		desc:   "simple-oltp-get-set",
		tables: tables,
	}
}

func (c *OltpGetSetCase) String() string { return c.desc }

func (c *OltpGetSetCase) Initialize(ctx context.Context, db *sql.DB) error { return nil }

func (c *OltpGetSetCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	log.Info("[SimOltpGetSet] start !")

	stat := processStatistic(ctx, db, simOLTPConcurrency, simOLTPGetSetRequests, c.execute)
	res := &CaseResult{
		Name:    c.desc,
		Summary: stat.summary,
		Stages:  stat.stages,
	}

	return res, nil
}

func (c *OltpGetSetCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	num := len(c.tables)

	var str, query, table string
	var seq int
	for i := 0; i < requests; i++ {
		table = c.tables[rander.Intn(num)]
		tx, err := db.Begin()
		if err != nil {
			continue
		}

		query = fmt.Sprintf("select `seq`, `name` from `%s` where id = %d", table, id)
		err = tx.QueryRow(query).Scan(&seq, &str)

		name := randomAsciiBytes(defaultStringSize, rander)
		if err != nil {
			query = fmt.Sprintf("INSERT INTO `%s`(`id`,`seq`,`name`) VALUES (?, 1, ?)", table)
			tx.Exec(query, id, name)
		} else {
			query = fmt.Sprintf("UPDATE `%s` SET `seq`=`seq`+1, `name`=? WHERE `id` = ?", table)
			tx.Exec(query, name, id)
		}
		tx.Commit()
	}

	return
}
