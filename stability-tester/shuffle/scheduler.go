package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var (
	shuffleRegion bool
	shuffleLeader bool
	interval      time.Duration
	pds           string
	tidbs         string
	tikvs         string
	lb            string
	metricAddr    string
)

var defaultPushMetricsInterval = 15 * time.Second

func init() {
	flag.BoolVar(&shuffleRegion, "shuffle-region", true, "enable shuffle region")
	flag.BoolVar(&shuffleLeader, "shuffle-leader", true, "enable shuffle leader")
	flag.DurationVar(&interval, "interval", time.Minute, "schedule interval")
	flag.StringVar(&pds, "pds", "", "separated by \",\"")
	flag.StringVar(&tidbs, "tidbs", "", "separated by \",\"")
	flag.StringVar(&tikvs, "tikvs", "", "separated by \",\"")
	flag.StringVar(&lb, "lb-service", "", "lb")
	flag.StringVar(&metricAddr, "metric-addr", "", "metric address")

}

func main() {
	// Initialize the default random number source.
	rand.Seed(time.Now().UTC().UnixNano())
	flag.Parse()

	err := detectParameters()
	if err != nil {
		log.Fatal(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		os.Exit(0)
	}()

	pdAddrs := strings.Split(pds, ",")
	if len(pdAddrs) == 0 {
		return
	}

	for i := range pdAddrs {
		pdAddrs[i] = fmt.Sprintf("http://%s", pdAddrs[i])
	}

	cfg := &SchedulerConfig{
		Interval:      interval,
		PDAddrs:       pdAddrs,
		ShuffleLeader: shuffleLeader,
		ShuffleRegion: shuffleRegion,
	}

	runScheduler(cfg)
}

func detectParameters() error {
	if len(pds) == 0 {
		return errors.New("lack of pds partermeters")
	}
	return nil
}

// SchedulerConfig is used to customize the scheduler configs for PD.
type SchedulerConfig struct {
	Interval      time.Duration `toml:"interval"`
	PDAddrs       []string      `toml:"pd"`
	ShuffleLeader bool          `toml:"shuffle-leader"`
	ShuffleRegion bool          `toml:"shuffle-region"`
}

// RunConfigScheduler for ctx
func runScheduler(cfg *SchedulerConfig) {
	var schedulerNames []string
	if cfg.ShuffleLeader {
		schedulerNames = append(schedulerNames, "shuffle-leader-scheduler")
	}
	if cfg.ShuffleRegion {
		schedulerNames = append(schedulerNames, "shuffle-region-scheduler")
	}

	if len(schedulerNames) == 0 {
		return
	}

	for {
		for _, name := range schedulerNames {
			data := map[string]string{"name": name}
			b, _ := json.Marshal(data)
			for _, addr := range cfg.PDAddrs {
				_, err := http.Post(addr+"/pd/api/v1/schedulers", "application/json", bytes.NewBuffer(b))
				if err != nil {
					log.Errorf("add scheduler to pd %v err: %v", addr, err)
				}
			}
		}

		time.Sleep(cfg.Interval)
	}
}
