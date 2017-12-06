package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/util"
	"golang.org/x/net/context"
)

var gcInterval = 6 * time.Hour

// LogCaseConfig is for Log test case
type LogCaseConfig struct {
	MaxCount    int           `toml:"max_count"`
	DeleteCount int           `toml:"delete_count"`
	Interval    time.Duration `toml:"interval"`
	TableNum    int           `toml:"table_num"`
	Concurrency int
}

// LogCase is for simulating writing Log.
// in this case, we will continuously write data.
// when the count of log entries is more than MaxCount,the specified number of logs are deleted.
type LogCase struct {
	cfg *LogCaseConfig
	lws []*logWriter
}

const logWriterBatchSize = 20

//the log size in range minSize and maxSize.
type logWriter struct {
	minSize       int
	maxSize       int
	rand          *rand.Rand
	logDataBuffer []byte
	values        []string
	index         int
}

//NewLogCase returns the LogCase.
func NewLogCase(cfg *LogCaseConfig) *LogCase {
	c := &LogCase{
		cfg: cfg,
	}
	c.initLogWrite()
	if c.cfg.TableNum <= 1 {
		c.cfg.TableNum = 1
	}
	return c
}

func (c *LogCase) initLogWrite() {
	c.lws = make([]*logWriter, c.cfg.Concurrency)
	for i := 0; i < c.cfg.Concurrency; i++ {
		source := rand.NewSource(int64(time.Now().UnixNano()))
		c.lws[i] = &logWriter{
			minSize:       100,
			maxSize:       1024,
			rand:          rand.New(source),
			logDataBuffer: make([]byte, 1024),
			values:        make([]string, logWriterBatchSize),
		}
	}
}

func (c *LogCase) String() string {
	return "log"
}

// Initialize implements Case Initialize interface.
func (c *LogCase) Initialize(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to init...", c.String())
	defer func() {
		log.Infof("[%s] init end...", c.String())
	}()
	for i := 0; i < c.cfg.TableNum; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		var s string
		if i > 0 {
			s = fmt.Sprintf("%d", i)
		}
		util.MustExec(db, fmt.Sprintf("create table if not exists log%s (id bigint auto_increment,data varchar(1024),primary key(id))", s))
	}

	c.startCheck(ctx, db)
	return nil
}

func (c *LogCase) truncate(ctx context.Context, db *sql.DB) error {
	for i := 0; i < c.cfg.TableNum; i++ {
		select {
		case <-ctx.Done():
			log.Error("truncate logctx done")
			return nil
		default:
		}
		var s string
		if i > 0 {
			s = fmt.Sprintf("%d", i)
		}
		log.Infof("truncate table log%s", s)
		err := util.RunWithRetry(ctx, 200, 3*time.Second, func() error {
			_, err := db.Exec(fmt.Sprintf("truncate table log%s", s))
			return err
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *LogCase) startCheck(ctx context.Context, db *sql.DB) {
	for i := 0; i < c.cfg.TableNum; i++ {
		go func(i int) {
			ticker := time.NewTicker(c.cfg.Interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					c.reviseLogCount(db, i)
				}
			}
		}(i)
	}
}

func (c *LogCase) reviseLogCount(db *sql.DB, id int) {
	var (
		count int
		index string
	)

	start := time.Now()
	if id > 0 {
		index = fmt.Sprintf("%d", id)
	}
	query := fmt.Sprintf("select count(*) as count from log%s", index)
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		logFailedCounterVec.WithLabelValues("count").Inc()
		log.Errorf("[%s] select count failed: %v", c, err)
		return
	}
	logDurationVec.WithLabelValues("count").Observe(time.Since(start).Seconds())

	if count > c.cfg.MaxCount {
		var err error
		sql := fmt.Sprintf("delete from log%s where id > 0 limit %d", index, c.cfg.DeleteCount)
		start := time.Now()
		_, err = db.Exec(sql)
		if err != nil {
			logFailedCounterVec.WithLabelValues("delete").Inc()
			log.Errorf("[%s] delete log failed: %v", c, err)
			return
		}
		logDurationVec.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	}
}

// Execute implements Case Execute interface.
func (c *LogCase) Execute(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to test...", c.String())
	defer func() {
		log.Infof("[%s] test end...", c.String())
	}()
	var wg sync.WaitGroup
	var ticker = time.NewTicker(gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
		default:
			err := c.truncate(ctx, db)
			if err != nil {
				log.Errorf("truncate table error %v", err)
			}
		}
		for i := 0; i < c.cfg.Concurrency; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						return
					default:
					}
					if i >= len(c.lws) {
						log.Error("[log case]: index out of range")
						return
					}
					if err := c.lws[i].batchExecute(db, c.cfg.TableNum); err != nil {
						log.Errorf("[%s] execute failed %v", c.String(), err)
					}
				}
			}(i)
		}

		wg.Wait()
	}
}

func (lw *logWriter) randomLogData() []byte {
	size := lw.rand.Intn(lw.maxSize-lw.minSize) + lw.minSize

	util.RandString(lw.logDataBuffer[:size], lw.rand)
	return lw.logDataBuffer[:size]
}

// Insert logWriterBatchSize values in one SQL.
//
// TODO: configure it from outside.

func (lw *logWriter) batchExecute(db *sql.DB, tableNum int) error {
	// buffer values
	for i := 0; i < logWriterBatchSize; i++ {
		lw.values[i] = fmt.Sprintf("('%s')", lw.randomLogData())
	}

	start := time.Now()
	var (
		err   error
		index string
	)
	if lw.index > 0 {
		index = fmt.Sprintf("%d", lw.index)
	}

	_, err = db.Exec(
		fmt.Sprintf(
			"INSERT INTO log%s (data) VALUES %s",
			index, strings.Join(lw.values, ",")),
	)

	if err != nil {
		logFailedCounterVec.WithLabelValues("batch_insert").Inc()
		log.Errorf("[log] insert log failed: %v", err)
		return err
	}

	lw.index = (lw.index + 1) % tableNum
	logDurationVec.WithLabelValues("batch_insert").Observe(time.Since(start).Seconds())
	return nil
}
