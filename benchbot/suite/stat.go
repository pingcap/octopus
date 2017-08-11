package suite

import (
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

func (s *StatResult) Update(h *hdrhistogram.Histogram, percent float64) {
	s.Qps = s.Total / s.Duration
	s.Eps = s.Error / s.Duration
	s.Histogram = NewStatHistogram(h, percent)
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
	MinMs        float64
	Percentile   float64
	PercentileMs float64
}

func NewStatHistogram(h *hdrhistogram.Histogram, percent float64) StatHistogram {
	return StatHistogram{
		AvgMs:        h.Mean(),
		MaxMs:        float64(h.Max()),
		MinMs:        float64(h.Min()),
		Percentile:   percent,
		PercentileMs: float64(h.ValueAtQuantile(percent)),
	}
}

type StatManager struct {
	mux  sync.Mutex
	stat *StatResult
	hist *hdrhistogram.Histogram
}

func NewStatManager() *StatManager {
	return &StatManager{
		stat: new(StatResult),
		hist: hdrhistogram.New(0, int64(10*time.Second), 4),
	}
}

func (s *StatManager) Result() *StatResult {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.stat.Update(s.hist, 95)
	return s.stat
}

func (s *StatManager) Record(op OPKind, err error, duration time.Duration) {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.stat.Total++
	if err != nil {
		s.stat.Error++
	}
	s.stat.Duration += duration.Seconds()
	s.hist.RecordValue(int64(duration / time.Millisecond))
}
