package suite

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/pkg"
	"golang.org/x/net/context"
)

const (
	opRead = iota
	opWrite
	opTxBegin
	opTxCommit
	opTxRollback

	batchFlushLimit = 100000

	percentile = 0.95
)

type statLatency struct {
	TotalMs      float64
	AvgMs        float64
	MinMs        float64
	MaxMs        float64
	PercentileMs float64
	Percentile   float64
}

type statSql struct {
	Read        uint64
	Write       uint64
	Other       uint64 // begin / commit / rollback ...
	Transaction uint64
}

type StatIndex struct {
	Total int
	Bytes uint64
	Error uint64

	Duration float64
	Qps      float64
	Bps      float64
	Eps      float64
	Tps      float64

	Sql     statSql
	Latency statLatency
}

type dbop struct {
	class     int
	err       bool
	bytes     uint64
	latency   float64
	latencyMs float64
}

type opsBatch struct {
	duration float64
	ops      []*dbop
}

type statisticManager struct {
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mux     sync.RWMutex
	running bool

	batchNews chan *opsBatch
	begining  time.Time
	opsBuffer []*dbop

	summary *StatIndex
	stages  []*StatIndex
	records []float64 // record operation latency
}

func calcPercentileF64(vals []float64, percent float64) float64 {
	sort.Float64s(vals)

	size := len(vals)
	index := int(float64(size) * percent)
	if index >= 1 {
		return (vals[index] + vals[index-1]) / 2
	} else {
		return vals[index]
	}
}

func roundF64(val float64, places int) float64 {
	v, _ := strconv.ParseFloat(fmt.Sprintf("%0.3f", val), 64)
	return v
}

//
// Indicate the statistic index for db operation
//
func newStatIndex() *StatIndex {
	stat := new(StatIndex)
	stat.Latency.MinMs = math.MaxFloat64
	stat.Latency.MaxMs = 0
	stat.Latency.Percentile = percentile // TODO .. config
	return stat
}

func (s *StatIndex) merge(other *StatIndex) {
	// general
	s.Total += other.Total
	s.Bytes += other.Bytes
	s.Error += other.Error
	s.Duration += other.Duration

	// sql
	s.Sql.Read += other.Sql.Read
	s.Sql.Write += other.Sql.Write
	s.Sql.Other += other.Sql.Other
	s.Sql.Transaction += other.Sql.Transaction

	// latency
	s.Latency.TotalMs += other.Latency.TotalMs

	if s.Latency.MaxMs < other.Latency.MaxMs {
		s.Latency.MaxMs = other.Latency.MaxMs
	}

	if s.Latency.MinMs > other.Latency.MinMs {
		s.Latency.MinMs = other.Latency.MinMs
	}

	s.update()
}

func (s *StatIndex) update() {
	s.Qps = roundF64(float64(s.Total)/s.Duration, 3)
	s.Tps = roundF64(float64(s.Sql.Transaction)/s.Duration, 3)
	s.Bps = roundF64(float64(s.Bytes)/s.Duration, 3)
	s.Eps = roundF64(float64(s.Error)/s.Duration, 5)
	s.Latency.AvgMs = roundF64(s.Latency.TotalMs/float64(s.Total), 3)

}

func (s *StatIndex) adjust() {
	s.Duration = roundF64(s.Duration, 3)

	s.Qps = roundF64(s.Qps, 3)
	s.Bps = roundF64(s.Bps, 3)
	s.Tps = roundF64(s.Tps, 3)
	s.Eps = roundF64(s.Eps, 5)

	s.Latency.TotalMs = roundF64(s.Latency.TotalMs, 3)
	s.Latency.AvgMs = roundF64(s.Latency.AvgMs, 3)
	s.Latency.MinMs = roundF64(s.Latency.MinMs, 3)
	s.Latency.MaxMs = roundF64(s.Latency.MaxMs, 3)
	s.Latency.PercentileMs = roundF64(s.Latency.PercentileMs, 3)
}

func (s *StatIndex) updateLatencyPercentile(latencyRecords []float64) {
	if len(latencyRecords) > 0 {
		s.Latency.PercentileMs = calcPercentileF64(latencyRecords, s.Latency.Percentile)
	}
}

func (s *StatIndex) clone() *StatIndex {
	cp := new(StatIndex)
	if err := DeepClone(cp, s); err != nil {
		return nil
	}
	return cp
}

func (s *StatIndex) FormatJSON() string {
	cp := s.clone()
	data, err := DumpJSON(cp, true)
	if err != nil {
		return ""
	}
	return data
}

func (stat *StatIndex) formatHuman() string {
	information := fmt.Sprintf("") +
		fmt.Sprintf("----------------\n") +
		fmt.Sprintf("General :\n") +
		fmt.Sprintf("	duration : %.3f (s)\n", stat.Duration) +
		fmt.Sprintf("	total : %d\n", stat.Total) +
		fmt.Sprintf("	error : %d\n", stat.Error) +
		fmt.Sprintf("\n") +
		fmt.Sprintf("	qps : %.3f \n", stat.Qps) +
		fmt.Sprintf("	tps : %.3f \n", stat.Tps) +
		fmt.Sprintf("	bps : %.3f \n", stat.Bps) +
		fmt.Sprintf("	eps : %.3f \n", stat.Eps) +
		fmt.Sprintf("----------------\n") +
		fmt.Sprintf("SQL") +
		fmt.Sprintf("	r : %d\n", stat.Sql.Read) +
		fmt.Sprintf("	w : %d\n", stat.Sql.Write) +
		fmt.Sprintf("	tx : %d\n", stat.Sql.Transaction) +
		fmt.Sprintf("	other : %d\n", stat.Sql.Other) +
		fmt.Sprintf("----------------\n") +
		fmt.Sprintf("Latency") +
		fmt.Sprintf("	tol : %.3f (ms)\n", stat.Latency.TotalMs) +
		fmt.Sprintf("	min : %.3f (ms)\n", stat.Latency.MinMs) +
		fmt.Sprintf("	max : %.3f (ms)\n", stat.Latency.MaxMs) +
		fmt.Sprintf("	avg : %.3f (ms)\n", stat.Latency.AvgMs)

	return information
}

//
// response to gather db operation and to statistic in batch
//
func newStatisticManager() *statisticManager {
	mgr := &statisticManager{
		running:   false,
		begining:  time.Now(),
		opsBuffer: make([]*dbop, 0, batchFlushLimit+1),
		batchNews: make(chan *opsBatch, 128),

		summary: newStatIndex(),
		stages:  make([]*StatIndex, 0, 128),
		records: make([]float64, 0, 1024),
	}

	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())

	return mgr
}

func (mgr *statisticManager) start() {
	mgr.mux.Lock()
	defer mgr.mux.Unlock()

	if !mgr.running {
		mgr.running = true
		mgr.wg.Add(1)
		go mgr.run()
	}
}

func (mgr *statisticManager) record(op *dbop) {
	if op.latency > 0.00000001 {
		op.latencyMs = op.latency * 1000
	}

	if op.latency > 10.0 {
		log.Warnf("long time sql op : cls = %d / time = %.1f sec", op.class, op.latency)
	}

	mgr.mux.Lock()
	mgr.opsBuffer = append(mgr.opsBuffer, op)
	if len(mgr.opsBuffer) >= batchFlushLimit {
		mgr.flush()
	}
	mgr.mux.Unlock()
}

func (mgr *statisticManager) run() {
	defer mgr.wg.Done()

	end := false
	for !end {
		select {
		case batch := <-mgr.batchNews:
			mgr.update(batch.ops, batch.duration)
		case <-mgr.ctx.Done():
			end = true
		}
	}

	if mgr.flush() > 0 {
		close(mgr.batchNews)
		for batch := range mgr.batchNews {
			mgr.update(batch.ops, batch.duration)
		}
	}

	return
}

func (mgr *statisticManager) update(ops []*dbop, duration float64) {
	if len(ops) == 0 {
		return
	}

	stat := newStatIndex()
	sqls := &stat.Sql
	stat.Duration = duration

	for _, op := range ops {
		switch op.class {
		case opRead:
			sqls.Read++
		case opWrite:
			sqls.Write++
		case opTxBegin:
			sqls.Other++
			sqls.Transaction++
		case opTxCommit:
			sqls.Other++
		case opTxRollback:
			sqls.Other++
			sqls.Transaction--
		default:
		}

		stat.Total++
		stat.Latency.TotalMs += op.latencyMs
		stat.Bytes += op.bytes
		if op.err {
			stat.Error++
		}

		if stat.Latency.MaxMs < op.latencyMs {
			stat.Latency.MaxMs = op.latencyMs
		}
		if stat.Latency.MinMs > op.latencyMs {
			stat.Latency.MinMs = op.latencyMs
		}

		mgr.records = append(mgr.records, op.latencyMs)
	}

	stat.update()
	stat.updateLatencyPercentile(mgr.records[len(mgr.records)-stat.Total:])
	stat.adjust()

	// merge
	mgr.stages = append(mgr.stages, stat)
	mgr.summary.merge(stat)
	mgr.summary.updateLatencyPercentile(mgr.records) // Notice : might cost long time !!
	mgr.summary.adjust()
}

func (mgr *statisticManager) flush() int {
	num := len(mgr.opsBuffer)
	if num > 0 {
		mgr.batchNews <- &opsBatch{
			ops:      mgr.opsBuffer,
			duration: time.Now().Sub(mgr.begining).Seconds(),
		}

		mgr.begining = time.Now()
		mgr.opsBuffer = make([]*dbop, 0, batchFlushLimit+1)
	}
	return num
}

func (mgr *statisticManager) close() {
	mgr.cancel()
	mgr.wg.Wait()

	mgr.mux.Lock()
	mgr.running = false
	mgr.mux.Unlock()
}

func (mon *statisticManager) getSummary() *StatIndex {
	mon.mux.RLock()
	defer mon.mux.RUnlock()

	return mon.summary.clone()
}

func (mon *statisticManager) getStages() []*StatIndex {
	// TODO ... copy ?
	return mon.stages
}
