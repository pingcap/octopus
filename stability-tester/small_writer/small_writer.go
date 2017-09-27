package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/util"
	"golang.org/x/net/context"
)

//this case for test TiKV perform when write small datavery frequently
type SmallWriterCase struct {
	tableNum    int
	concurrency int
	sws         []*smallDataWriter
}

const smallWriterBatchSize = 20

//the small data is a int number
type smallDataWriter struct {
	rand   *rand.Rand
	values []string
}

//NewSmallWriterCase returns the smallWriterCase.
func NewSmallWriterCase(tableNum, concurrency int) *SmallWriterCase {
	c := &SmallWriterCase{
		tableNum:    tableNum,
		concurrency: concurrency,
	}

	c.initSmallDataWriter(c.concurrency)
	return c
}

func (c *SmallWriterCase) initSmallDataWriter(concurrency int) {
	c.sws = make([]*smallDataWriter, concurrency)
	for i := 0; i < concurrency; i++ {
		source := rand.NewSource(int64(time.Now().UnixNano()))
		c.sws[i] = &smallDataWriter{
			rand:   rand.New(source),
			values: make([]string, smallWriterBatchSize),
		}
	}
}

// Initialize implements Case Initialize interface.
func (c *SmallWriterCase) Initialize(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to init...", c.String())
	defer func() {
		log.Infof("[%s] init end...", c.String())
	}()
	util.MustExec(db, "create table if not exists small_writer(id bigint auto_increment, data bigint, primary key(id))")

	return nil
}

// Execute implements Case Execute interface.
func (c *SmallWriterCase) Execute(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to test...", c.String())
	defer func() {
		log.Infof("[%s] test end...", c.String())
	}()
	var wg sync.WaitGroup
	for i := 0; i < c.concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := c.sws[i].batchExecute(db); err != nil {
					smallWriteFailedCounter.Inc()
					log.Errorf("[%s] execute failed %v", c.String(), err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// String implements fmt.Stringer interface.
func (c *SmallWriterCase) String() string {
	return "small_writer"
}

// Insert values.
func (sw *smallDataWriter) batchExecute(db *sql.DB) error {
	var err error
	for i := 0; i < smallWriterBatchSize; i++ {
		start := time.Now()
		_, err = db.Exec(
			fmt.Sprintf(
				"INSERT INTO small_writer (data) VALUES (%d)",
				sw.rand.Int()),
		)

		if err != nil {
			return fmt.Errorf("[small writer] insert err %v", err)
		}
		smallWriteDuration.Observe(time.Since(start).Seconds())
	}
	return nil
}
