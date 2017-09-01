package suite

import (
	"database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"

	. "github.com/pingcap/octopus/benchbot/cluster"
)

func init() {
	builder := func(meta toml.MetaData, value toml.Primitive) BenchSuite {
		cfg := new(SysbenchConfig)
		meta.PrimitiveDecode(value, cfg)
		return NewSysbenchSuite(cfg)
	}
	RegisterBenchSuite("sysbench", builder)
}

type SysbenchConfig struct {
	ScriptsDir string `toml:"scripts_dir"`
}

type SysbenchSuite struct {
	cfg *SysbenchConfig
}

func NewSysbenchSuite(cfg *SysbenchConfig) *SysbenchSuite {
	return &SysbenchSuite{cfg: cfg}
}

func (s *SysbenchSuite) Name() string {
	return "sysbench"
}

func (s *SysbenchSuite) Run(cluster Cluster) ([]*CaseResult, error) {
	cases := []BenchCase{
		NewSysbenchCase(s, "oltp"),
		NewSysbenchCase(s, "select"),
		NewSysbenchCase(s, "insert"),
		NewSysbenchCase(s, "delete"),
	}

	return RunBenchCasesWithReset(cases, cluster)
}

type SysbenchCase struct {
	cfg  *SysbenchConfig
	name string
}

func NewSysbenchCase(s *SysbenchSuite, name string) *SysbenchCase {
	return &SysbenchCase{
		cfg:  s.cfg,
		name: name,
	}
}

func (c *SysbenchCase) Name() string {
	return "sysbench-" + c.name
}

func (c *SysbenchCase) Run(*sql.DB) (*CaseResult, error) {
	if _, err := c.execute("parallel-prepare"); err != nil {
		return nil, err
	}
	output, err := c.execute(c.name)
	if err != nil {
		return nil, err
	}

	total, err := extractTarget(output, "total")
	if err != nil {
		return nil, err
	}
	totalTime, err := extractTarget(output, "total time")
	if err != nil {
		return nil, err
	}
	duration := time.Duration(totalTime*1000) * time.Millisecond
	avgMs, err := extractTarget(output, "avg")
	if err != nil {
		return nil, err
	}
	maxMs, err := extractTarget(output, "max")
	if err != nil {
		return nil, err
	}
	percentileMs, err := extractTarget(output, "95th percentile")
	if err != nil {
		return nil, err
	}

	histogram := StatHistogram{
		AvgMs:        avgMs,
		MaxMs:        maxMs,
		Percentile:   0.95,
		PercentileMs: percentileMs,
	}
	stat := &StatResult{
		Total: total,
		Error: 0,
	}
	stat.Update(duration, histogram)

	res := &CaseResult{
		Name: c.Name(),
		Stat: stat,
	}
	return res, err
}

func (c *SysbenchCase) execute(name string) (string, error) {
	cmd := exec.Command("bash", name+".sh")
	cmd.Dir = c.cfg.ScriptsDir
	buf, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf("[case:%s] execute %s: %s", c.Name(), name, err)
	}
	return string(buf), err
}

func extractTarget(output, target string) (float64, error) {
	pattern := fmt.Sprintf("%s:[ ]+[0-9.]+", target)
	re := regexp.MustCompile(pattern)
	s := re.Find([]byte(output))
	if s == nil {
		return 0, fmt.Errorf("failed to extract %s", target)
	}

	seps := strings.Split(string(s), ":")
	if len(seps) != 2 {
		return 0, fmt.Errorf("failed to extract %s", target)
	}

	var res float64
	if n, err := fmt.Sscanf(seps[1], "%f", &res); n != 1 || err != nil {
		return 0, fmt.Errorf("failed to extract %s", target)
	}

	return res, nil
}
