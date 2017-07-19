package suite

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"database/sql"
	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

const (
	simOpsTables = 16

	simSetConcurrency = 1000   // sysbench : num-threads
	simSetRequests    = 1000000 // sysbench : max-requests

	simGetConcurrency = 1000   // sysbench : num-threads
	simGetRequests    = 2000000 // sysbench : max-requests

	simDelConcurrency = 1000  // sysbench : num-threads
	simDelRequests    = 1000000 // sysbench : max-requests
)

type simExeContext struct {
	tables      []string
	tableMaxSeq int // ps : max value of field `Seq` for each table
}

//
// bench suite
//
type SimpleOperationSuite struct{}

func NewSimpleOperationSuite() *SimpleOperationSuite {
	return new(SimpleOperationSuite)
}

func (s *SimpleOperationSuite) String() string {
	return "SimpleOperationSuite"
}

func (s *SimpleOperationSuite) Run(ctx context.Context, db *sql.DB) (results []*CaseResult, err error) {
	tables, err := s.initTables(db)
	if err != nil {
		return nil, err
	}

	exeCtx := &simExeContext{tables: tables}
	cases := []BenchCase{
		NewSimpleSetCase(exeCtx),
		NewSimpleGetCase(exeCtx),
		NewSimpleDelCase(exeCtx),
	}

	results = make([]*CaseResult, 0, len(cases))
	for _, c := range cases {
		if err = c.Initialize(ctx, db); err != nil {
			break
		}
		if res, err := c.Run(ctx, db); err != nil {
			break
		} else {
			log.Infof("case end : %s", res.Summary.FormatJSON())
			results = append(results, res)
		}
		time.Sleep(time.Second * 60)
	}

	return
}

func (s *SimpleOperationSuite) initTables(db *sql.DB) ([]string, error) {
	log.Infof("[SimpleOperationSuite] prepare (table = %d / table_rows = %d)", simOpsTables, 0)

	return prepare(db, simOpsTables, 0)
}

//
// sim insert case
//
type SimpleSetCase struct {
	desc string
	ctx  *simExeContext
}

func NewSimpleSetCase(ctx *simExeContext) *SimpleSetCase {
	return &SimpleSetCase{
		desc: "simple-insert",
		ctx:  ctx,
	}
}

func (c *SimpleSetCase) String() string {
	return c.desc
}

func (c *SimpleSetCase) Initialize(ctx context.Context, db *sql.DB) error { return nil }

func (c *SimpleSetCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	log.Info("[SimpleSetCase] start ...")

	stat := processStatistic(ctx, db, simSetConcurrency, simSetRequests, c.execute)
	res := &CaseResult{
		Name:    c.desc,
		Summary: stat.summary,
		Stages:  stat.stages,
	}

	// ps : After finish filling tables with data,
	//      set the max `Seq` base on the executing stragety above !
	rowsPerTable := float64(simSetRequests) / float64(simOpsTables)
	if c.ctx != nil {
		c.ctx.tableMaxSeq = int(math.Floor(rowsPerTable))
	}
	
	// ps : while tikv configre wtih "sync-log = false" ,
	//		here wait for moment to aviod query via index causing long time latency !!!

	return res, nil
}

func (c *SimpleSetCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	tables := c.ctx.tables
	num := len(tables)

	for i := 0; i < requests; i++ {
		table := tables[rander.Intn(num)]
		uid := rander.Int63()
		seq := rander.Int31()
		name := randomAsciiBytes(defaultStringSize, rander)

		query := fmt.Sprintf("INSERT INTO `%s` (`id`, `seq`, `name`) VALUES (?, ?, ?)", table)
		if _, err := db.Exec(query, uid, seq, name); err != nil {
			log.Errorf(err.Error())
		}
	}
}

//
// simple select case
//
type SimpleGetCase struct {
	desc string
	ctx  *simExeContext
}

func NewSimpleGetCase(ctx *simExeContext) *SimpleGetCase {
	return &SimpleGetCase{
		desc: "simple-select",
		ctx:  ctx,
	}
}

func (c *SimpleGetCase) String() string {
	return c.desc
}

func (c *SimpleGetCase) Initialize(ctx context.Context, db *sql.DB) error { return nil }

func (c *SimpleGetCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	log.Info("[SimpleGetCase] start ...")

	stat := processStatistic(ctx, db, simGetConcurrency, simGetRequests, c.execute)
	res := &CaseResult{
		Name:    c.desc,
		Summary: stat.summary,
		Stages:  stat.stages,
	}

	return res, nil
}

func (c *SimpleGetCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	maxSeq := c.ctx.tableMaxSeq
	tables := c.ctx.tables
	num := len(tables)

	var val int64
	for i := 0; i < requests; i++ {
		table := tables[rander.Intn(num)]
		seq := rander.Intn(maxSeq)

		r := db.QueryRow(fmt.Sprintf("SELECT `id` FROM `%s` WHERE `seq` = ?", table), seq)
		r.Scan(&val)
	}
}

//
// simple delete case
//
type SimpleDelCase struct {
	desc string
	ctx  *simExeContext
}

func NewSimpleDelCase(ctx *simExeContext) *SimpleDelCase {
	return &SimpleDelCase{
		desc: "simple-delete",
		ctx:  ctx,
	}
}

func (c *SimpleDelCase) String() string {
	return c.desc
}

func (c *SimpleDelCase) Initialize(ctx context.Context, db *sql.DB) error { return nil }

func (c *SimpleDelCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	log.Info("[SimpleDelCase] start ...")

	stat := processStatistic(ctx, db, simDelConcurrency, simDelRequests, c.execute)
	res := &CaseResult{
		Name:    c.desc,
		Summary: stat.summary,
		Stages:  stat.stages,
	}

	return res, nil
}

func (c *SimpleDelCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	maxSeq := c.ctx.tableMaxSeq
	tables := c.ctx.tables
	num := len(tables)

	for i := 0; i < requests; i++ {
		table := tables[rander.Intn(num)]
		seq := rander.Intn(maxSeq)
		query := fmt.Sprintf("DELETE FROM `%s` WHERE `seq` = ?", table)
		if _, err := db.Exec(query, seq); err != nil {
			log.Errorf(err.Error())
		}
	}
}
