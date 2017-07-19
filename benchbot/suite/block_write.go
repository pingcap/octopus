package suite

import (
	"database/sql"
	//"encoding/json"
	"fmt"
	//"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/twinj/uuid"
	"golang.org/x/net/context"
)

const (
	bwConcurrency = 1000
	round         = 1000
	minBlockSize  = 1024 * 3
	maxBlockSize  = 1024 * 4 - 1

	defaultFilling = byte('a')

	blockWriteTableSchema = `block_writer (
      block_id BIGINT NOT NULL,
      writer_id VARCHAR(64) NOT NULL,
      block_num BIGINT NOT NULL,
      raw_bytes BLOB NOT NULL,
      PRIMARY KEY (block_id, writer_id, block_num)
	)`
)

var (
	dataFilter = []byte{byte('\''), byte('"'), byte('\\')}
)

type BlockWriteSuite struct{}

func NewBlockWriteSuite() *BlockWriteSuite {
	return new(BlockWriteSuite)
}

func (s *BlockWriteSuite) String() string {
	return "BlockWriteSuite"
}

func (s *BlockWriteSuite) Run(ctx context.Context, db *sql.DB) (results []*CaseResult, err error) {
	cases := []BenchCase{
		NewBlockWriteCase(),
	}

	results = make([]*CaseResult, 0, len(cases))
	for _, c := range cases {
		c.Initialize(ctx, db)
		if res, err := c.Run(ctx, db); err != nil {
			break
		} else {
			log.Infof("case end : %s", res.Summary.FormatJSON())
			results = append(results, res)
		}
	}

	return
}

// BlockWriteCase is for concurrent writing blocks.
type BlockWriteCase struct {
	desc string
	bws  []*blockWriter
	stat *statisticManager
}

type blockWriter struct {
	id         string
	minSize    int
	maxSize    int
	blockCount int

	rand *rand.Rand
	stat *statisticManager
}

// NewBlockWriteCase returns the BlockWriteCase.
func NewBlockWriteCase() BenchCase {
	c := &BlockWriteCase{
		desc: "block-write",
		stat: newStatisticManager(), // TODO ... not start until run() !
	}
	c.initBlocks()
	return c
}

func (bw *BlockWriteCase) String() string {
	return bw.desc
}

func (c *BlockWriteCase) initBlocks() {
	c.bws = make([]*blockWriter, bwConcurrency)
	for i := 0; i < bwConcurrency; i++ {
		c.bws[i] = newBlockWriter(c.stat)
	}
}

// block writer
func newBlockWriter(stat *statisticManager) *blockWriter {
	source := rand.NewSource(int64(time.Now().UnixNano()))
	bw := &blockWriter{
		id:      uuid.NewV4().String(),
		rand:    rand.New(source),
		minSize: minBlockSize,
		maxSize: maxBlockSize,
		// summary: newBlockWriteSummary(),
		stat: stat,
	}

	return bw
}

func (bw *blockWriter) randomBlock() []byte {
	blockSize := bw.rand.Intn(bw.maxSize-bw.minSize) + bw.minSize
	blockData := randomAsciiBytes(blockSize, bw.rand)

	var v byte
	for i := range blockData {
		v = byte(bw.rand.Int() & 0xff)
		for _, c := range dataFilter {
			if c == v {
				v = defaultFilling
				break
			}
		}
		blockData[i] = v
	}
	return blockData
}

func (bw *blockWriter) execute(db *sql.DB) error {
	bw.blockCount++
	blockID := bw.rand.Int63()
	blockData := bw.randomBlock()

	start := time.Now()
	_, err := db.Exec(
		`INSERT INTO block_writer (block_id, writer_id, block_num, raw_bytes) VALUES (?, ?, ?, ?)`,
		blockID, bw.id, bw.blockCount, blockData)

	bw.stat.record(&dbop{
		class:   opWrite,
		bytes:   uint64(len(blockData)),
		latency: time.Now().Sub(start).Seconds(),
		err:     err != nil,
	})

	return err
}

// Initialize implements Case Initialize interface.
func (c *BlockWriteCase) Initialize(ctx context.Context, db *sql.DB) (err error) {
	if _, err = db.Exec("DROP TABLE IF EXISTS block_writer"); err != nil {
		return
	}

	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s`, blockWriteTableSchema))

	return
}

func (c *BlockWriteCase) Run(ctx context.Context, db *sql.DB) (*CaseResult, error) {
	c.stat.start()
	{
		wg := sync.WaitGroup{}
		wg.Add(bwConcurrency)

		var err error
		process := func(w *blockWriter, id int) {
			defer wg.Done()
			for i := 0; i < round; i++ {
				select {
				case <-ctx.Done():
					return
				default:
					if err = w.execute(db); err != nil {
						log.Errorf("[block_write] error : %s", err.Error())
					}
				}
			}
		}

		for i := 0; i < bwConcurrency; i++ {
			go process(c.bws[i], i)
		}

		wg.Wait() // Wait all over.
	}
	c.stat.close()

	res := &CaseResult{
		Name:    c.desc,
		Summary: c.stat.getSummary(),
		Stages:  c.stat.getStages(),
	}

	return res, ctx.Err()
}
