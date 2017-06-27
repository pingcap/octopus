package suite

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
	Transaction uint64
}

type StatIndex struct {
	Total    int
	Bytes    uint64
	Error    uint64
	Duration float64
	Qps      float64
	Bps      float64
	Eps      float64

	Sql     statSql
	Latency statLatency
}
