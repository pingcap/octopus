package suite

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"

	. "github.com/pingcap/octopus/benchbot/common"
)

type OPKind int

const (
	OPRead OPKind = iota
	OPWrite
	OPBegin
	OPCommit
	OPRollback
)

type StatResult struct {
	Qps       float64
	Eps       float64
	Total     float64
	Error     float64
	Duration  float64
	Histogram StatHistogram
}

func (s *StatResult) Record(op OPKind, err error) {
	s.Total++
	if err != nil {
		s.Error++
	}
}

func (s *StatResult) Update(d time.Duration, h StatHistogram) {
	s.Duration = roundF64(d.Seconds())
	s.Histogram = h
	s.Qps = roundF64(s.Total / s.Duration)
	s.Eps = roundF64(s.Error / s.Duration)
}

func (s *StatResult) FormatJSON() string {
	data, err := DumpJSON(s, true)
	if err != nil {
		err.Error()
	}
	return data
}

type StatHistogram struct {
	AvgMs        float64
	MaxMs        float64
	Percentile   float64
	PercentileMs float64
}

func NewStatHistogram(h *hdrhistogram.Histogram, percent float64) StatHistogram {
	return StatHistogram{
		AvgMs:        roundF64(h.Mean()),
		MaxMs:        roundF64(float64(h.Max())),
		Percentile:   roundF64(percent),
		PercentileMs: roundF64(float64(h.ValueAtQuantile(percent * 100))),
	}
}

func roundF64(f float64) float64 {
	v, _ := strconv.ParseFloat(fmt.Sprintf("%0.3f", f), 64)
	return v
}

type StatManager struct {
	mux   sync.Mutex
	start time.Time
	stat  *StatResult
	hist  *hdrhistogram.Histogram
}

func NewStatManager() *StatManager {
	return &StatManager{
		stat: new(StatResult),
		hist: hdrhistogram.New(0, int64(10*time.Second), 4),
	}
}

func (s *StatManager) Start() {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.start = time.Now()
}

func (s *StatManager) Close() {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.stat.Update(time.Since(s.start), NewStatHistogram(s.hist, 0.95))
}

func (s *StatManager) Result() *StatResult {
	s.mux.Lock()
	defer s.mux.Unlock()

	return s.stat
}

func (s *StatManager) Record(op OPKind, err error, duration time.Duration) {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.stat.Record(op, err)
	s.hist.RecordValue(int64(duration / time.Millisecond))
}
