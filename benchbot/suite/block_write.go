package suite

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync/atomic"

	"github.com/BurntSushi/toml"

	. "github.com/pingcap/octopus/benchbot/cluster"
)

type BlockWriteConfig struct {
	NumThreads   int `toml:"num_threads"`
	NumRequests  int `toml:"num_requests"`
	MinBlockSize int `toml:"min_block_size"`
	MaxBlockSize int `toml:"max_block_size"`
}

const (
	blockWriteTableName   = "block_write"
	blockWriteTableSchema = `(
        block_id BIGINT NOT NULL,
        block_data BLOB NOT NULL,
        PRIMARY KEY (block_id)
	)`
)

func init() {
	builder := func(meta toml.MetaData, value toml.Primitive) BenchSuite {
		cfg := new(BlockWriteConfig)
		meta.PrimitiveDecode(value, cfg)
		return NewBlockWriteSuite(cfg)
	}
	RegisterBenchSuite("block-write", builder)
}

type BlockWriteSuite struct {
	cfg *BlockWriteConfig
}

func NewBlockWriteSuite(cfg *BlockWriteConfig) *BlockWriteSuite {
	return &BlockWriteSuite{cfg: cfg}
}

func (s *BlockWriteSuite) Name() string {
	return "block-write"
}

func (s *BlockWriteSuite) Run(cluster Cluster) ([]*CaseResult, error) {
	if err := cluster.Start(); err != nil {
		return nil, err
	}
	defer cluster.Reset()
	return s.run(cluster.Accessor())
}

func (s *BlockWriteSuite) run(db *sql.DB) ([]*CaseResult, error) {
	if err := s.prepare(db); err != nil {
		return nil, err
	}

	cases := []BenchCase{
		NewBlockWriteCase(s),
	}

	return RunBenchCases(cases, db)
}

func (s *BlockWriteSuite) prepare(db *sql.DB) error {
	return CreateTable(db, blockWriteTableName, blockWriteTableSchema)
}

type BlockWriteCase struct {
	cfg     *BlockWriteConfig
	blockID uint64
}

func NewBlockWriteCase(s *BlockWriteSuite) *BlockWriteCase {
	return &BlockWriteCase{
		cfg:     s.cfg,
		blockID: 0,
	}
}

func (c *BlockWriteCase) Name() string {
	return "block-write"
}

func (c *BlockWriteCase) Run(db *sql.DB) (*CaseResult, error) {
	return ParallelSQLBench(c, c.Execute, c.cfg.NumThreads, c.cfg.NumRequests, db)
}

func (c *BlockWriteCase) Execute(db *StatDB, rander *rand.Rand) error {
	blockID := atomic.AddUint64(&c.blockID, 1)
	blockSize := rander.Intn(c.cfg.MaxBlockSize-c.cfg.MinBlockSize) + c.cfg.MinBlockSize
	blockData := RandomAsciiBytes(rander, blockSize)

	stmt := fmt.Sprintf(
		"INSERT INTO `%s` (`block_id`, `block_data`) VALUES (?, ?)", blockWriteTableName)
	_, err := db.Exec(stmt, blockID, blockData)
	return err
}
