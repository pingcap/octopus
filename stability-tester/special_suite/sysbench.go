/*
 * 1) prepare
 * sysbench --test=./sysbench/insert.lua --mysql-host=${host} --mysql-port=${port} --mysql-user=${user}
 * --mysql-password=${passwd} --oltp-tables-count=10 --oltp-table-size=10000000 --rand-init=on prepare
 *
 * 2) run
 * sysbench --test=./sysbench/insert.lua --mysql-host=${host} --mysql-port=${port} --mysql-user=${user}
 * --mysql-password=${passwd} --oltp-tables-count=10 --oltp-table-size=10000000 --num-threads=${threads}
 * --oltp-read-only=off --report-interval=60 --rand-type=uniform --max-time=600 --percentile=99
 * --max-requests=1000000000 run
 *
 *
 * OLTP test statistics:
 *   queries performed:
 *       read:                            2101540
 *       write:                           600440
 *       other:                           300220
 *       total:                           3002200
 *   transactions:                        150110 (2501.72 per sec.)
 *   read/write requests:                 2701980 (45030.91 per sec.)
 *   other operations:                    300220 (5003.43 per sec.)
 *   ignored errors:                      0      (0.00 per sec.)
 *   reconnects:                          0      (0.00 per sec.)
 *
 * General statistics:
 *   total time:                          60.0028s
 *   total number of events:              150110
 *   total time taken by event execution: 479.8373s
 *   response time:
 *        min:                                  1.06ms
 *        avg:                                  3.20ms
 *        max:                                306.37ms
 *        approx.  99 percentile:              13.83ms
 *
 * Threads fairness:
 *   events (avg/stddev):           18763.7500/272.53
 *   execution time (avg/stddev):   59.9797/0.00
 */

package special_suite

import (
	"fmt"
	"os/exec"

	"github.com/pingcap/octopus/stability-tester/config"
	"golang.org/x/net/context"
	"time"
)

func init() {
	RegisterSuite("sysbench", NewSysbenchCase)
}

// SysbenchCase is configuration for sysbench.
type SysbenchCase struct {
	cfg      *config.SysbenchConfig
	host     string
	port     int
	user     string
	password string
	ctx      context.Context
}

// NewSysbenchCase returns the SysbenchCase.
func NewSysbenchCase(cfg *config.Config) Case {
	return &SysbenchCase{
		cfg:      &cfg.SpecialSuite.Sysbench,
		host:     cfg.Host,
		port:     cfg.Port,
		user:     cfg.User,
		password: cfg.Password,
	}
}

// InitSuite implements Case Initialize interface.
func (s *SysbenchCase) InitSuite(ctx context.Context) error {
	s.ctx = ctx
	err := s.clean()
	if err != nil {
		return err
	}
	return nil
}

// Execute implements Case Execute interface.
func (s *SysbenchCase) Execute() error {
	ticker := time.NewTicker(s.cfg.Interval)
	for {
		select {
		case <-s.ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.prepare(); err != nil {
				return err
			}
			time.Sleep(2 * time.Second)
			if err := s.run(); err != nil {
				return err
			}
			if err := s.clean(); err != nil {
				return err
			}
		}
	}
	return nil
}

// String implements fmt.Stringer interface.
func (s *SysbenchCase) String() string {
	return "bank"
}

func (s *SysbenchCase) prepare() error {
	var err error
	createDBArgs := fmt.Sprintf(`/usr/bin/mysql -h%s -P%d -u%s -e"create database IF NOT EXISTS sbtest"`, s.host, s.port, s.user)
	fmt.Println("prepare command: ", createDBArgs)
	cmdCreate := exec.Command("/bin/sh", "-c", createDBArgs)
	if err = cmdCreate.Run(); err != nil {
		fmt.Println("create database failed", err)
		return err
	}

	cmdStr := fmt.Sprintf(`sysbench --test=./sysbench/oltp.lua --mysql-host=%s --mysql-port=%d --mysql-user=%s --mysql-password=%s --oltp-tables-count=%d --oltp-table-size=%d --rand-init=on prepare`,
		s.host, s.port, s.user, s.password, s.cfg.TableCount, s.cfg.TableSize)
	fmt.Println("prepare command: ", cmdStr)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	if err := cmd.Run(); err != nil {
		fmt.Println("run sysbench prepare failed", err)
		return err
	}

	return nil
}

func (s *SysbenchCase) run() error {
	cmdStr := fmt.Sprintf(`sysbench --test=./sysbench/oltp.lua --mysql-host=%s --mysql-port=%d --mysql-user=%s --mysql-password=%s --oltp-tables-count=%d --oltp-table-size=%d --num-threads=%d --oltp-read-only=off --report-interval=60 --rand-type=uniform --max-time=%d --percentile=99 --max-requests=1000000000 run`,
		s.host, s.port, s.user, s.password, s.cfg.TableCount, s.cfg.TableSize, s.cfg.Threads, s.cfg.MaxTime)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	fmt.Println("run command: ", cmdStr)
	if err := cmd.Run(); err != nil {
		fmt.Println("run sysbench run failed", err)
		return err
	}

	return nil
}

func (s *SysbenchCase) clean() error {
	cmdStrArgs := fmt.Sprintf(`/usr/bin/mysql -h%s -P%d -u%s -e"drop database if exists sbtest"`, s.host, s.port, s.user)
	fmt.Println("clean command: ", cmdStrArgs)
	cmd := exec.Command("/bin/sh", "-c", cmdStrArgs)
	if err := cmd.Run(); err != nil {
		fmt.Println("run drop database failed", err)
		return err
	}
	return nil
}
