package suite

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

type BlockWriteConfig struct {
	NumThreads   int `toml:"num_threads"`
	NumRequests  int `toml:"num_requests"`
	MinBlockSize int `toml:"min_block_size"`
	MaxBlockSize int `toml:"max_block_size"`
}

const (
	blockWriteTable       = "block_write"
	blockWriteTableSchema = `(
        block_id BIGINT NOT NULL,
        block_data BLOB NOT NULL,
        PRIMARY KEY (block_id)
	)`
)

type BlockWriteSuite struct {
	cfg *BlockWriteConfig
}

func NewBlockWriteSuite(cfg *BlockWriteConfig) *BlockWriteSuite {
	return &BlockWriteSuite{cfg: cfg}
}

func (s *BlockWriteSuite) String() string {
	return "block-write"
}

func (s *BlockWriteSuite) Run(ctx context.Context, db *sql.DB) ([]*CaseResult, error) {
	if err := s.prepare(db); err != nil {
		return nil, err
	}

	cases := []BenchCase{
		NewBlockWriteCase(s),
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
	}

	return results, nil
}

func (s *BlockWriteSuite) prepare(db *sql.DB) (err error) {
	return createTable(db, blockWriteTable, blockWriteTableSchema)
}

// BlockWriteCase is for concurrent writing blocks.
type BlockWriteCase struct {
	suite   *BlockWriteSuite
	blockID uint64
}

func NewBlockWriteCase(suite *BlockWriteSuite) *BlockWriteCase {
	return &BlockWriteCase{
		suite:   suite,
		blockID: 0,
	}
}

func (c *BlockWriteCase) String() string {
	return "block-write"
}

func (c *BlockWriteCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	cfg := c.suite.cfg
	stat := processStatistic(c.execute, ctx, db, cfg.NumThreads, cfg.NumRequests)
	result := &CaseResult{
		Name:    c.String(),
		Summary: stat.summary,
		Stages:  stat.stages,
	}
	return result, nil
}

func (c *BlockWriteCase) execute(ctx context.Context, db *StatDB, id, requests int) {
	cfg := c.suite.cfg
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))

	for i := 0; i < requests; i++ {
		blockID := atomic.AddUint64(&c.blockID, 1)
		blockSize := rand.Intn(cfg.MaxBlockSize-cfg.MinBlockSize) + cfg.MinBlockSize
		blockData := randomAsciiBytes(blockSize, rander)

		query := fmt.Sprintf(
			"INSERT INTO `%s` (`block_id`, `block_data`) VALUES (?, ?)", blockWriteTable)
		if _, err := db.Exec(query, blockID, blockData); err != nil {
			log.Errorf("[case:%s] exec: %s", c, err)
		}
	}
}
