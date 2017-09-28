package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/util"
	"golang.org/x/net/context"
)

// SysbenchCase is configuration for sysbench.
type SysbenchCase struct {
	cfg *Config
}

// Config is the configuration for the sysbench test.
type Config struct {
	Host       string        `toml:"host"`
	Port       int           `toml:"port"`
	User       string        `toml:"user"`
	Password   string        `toml:"password"`
	TableCount int           `toml:"table_count"`
	TableSize  int           `toml:"table_size"`
	Threads    int           `toml:"threads"`
	MaxTime    int           `toml:"max_time"`
	Interval   util.Duration `toml:"interval"`
	DBName     string        `toml:"database"`
	LuaPath    string        `toml:"lua_path"`
}

// NewSysbenchCase returns the SysbenchCase.
func NewSysbenchCase(cfg *Config) *SysbenchCase {
	return &SysbenchCase{
		cfg: cfg,
	}
}

// Initialize implements Case Initialize interface.
func (s *SysbenchCase) Initialize() error {
	err := s.clean()
	if err != nil {
		return err
	}
	return nil
}

// Execute implements Case Execute interface.
func (s *SysbenchCase) Execute(ctx context.Context) error {
	err := s.runAction()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(s.cfg.Interval.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := s.runAction()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// String implements fmt.Stringer interface.
func (s *SysbenchCase) String() string {
	return "sysbench"
}

func (s *SysbenchCase) runAction() error {
	if err := s.prepare(); err != nil {
		return err
	}
	time.Sleep(10 * time.Second)
	if err := s.run(); err != nil {
		return err
	}
	if err := s.clean(); err != nil {
		return err
	}
	return nil
}

func (s *SysbenchCase) prepare() error {
	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", s.cfg.User, s.cfg.Password, s.cfg.Host, s.cfg.Port, "sbtest")
	db, err := util.OpenDB(dbAddr)
	if err != nil {
		log.Errorf("open db failed: ", err)
		return err
	}
	defer db.Close()
	_, err = db.Exec("create database IF NOT EXISTS sbtest")
	if err != nil {
		log.Errorf("create database failed: %v", err)
		return err
	}

	cmdStr := fmt.Sprintf(`sysbench --test=%s/oltp_insert.lua --mysql-host=%s --mysql-port=%d --mysql-user=%s --mysql-password=%s --oltp-tables-count=%d --oltp-table-size=%d --rand-init=on --db-driver=mysql prepare`,
		s.cfg.LuaPath, s.cfg.Host, s.cfg.Port, s.cfg.User, s.cfg.Password, s.cfg.TableCount, 0)
	log.Infof("create tables command: %s", cmdStr)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		log.Errorf("%s\n", out.String())
		return err
	}

	return nil
}

func (s *SysbenchCase) run() error {
	cmdStr := fmt.Sprintf(`sysbench --test=%s/oltp_insert.lua --mysql-host=%s --mysql-port=%d --mysql-user=%s --mysql-password=%s --oltp-tables-count=%d --oltp-table-size=%d --num-threads=%d --oltp-read-only=off --report-interval=600 --rand-type=uniform --max-time=%d --percentile=99 --max-requests=1000000000 --db-driver=mysql run`,
		s.cfg.LuaPath, s.cfg.Host, s.cfg.Port, s.cfg.User, s.cfg.Password, s.cfg.TableCount, s.cfg.TableSize, s.cfg.Threads, s.cfg.MaxTime)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	log.Infof("run command: %s", cmdStr)

	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		log.Errorf("%s\n", out.String())
		return err
	}

	return nil
}

func (s *SysbenchCase) clean() error {
	cmdStrArgs := fmt.Sprintf(`/usr/bin/mysql -h%s -P%d -u%s -e"drop database if exists sbtest"`, s.cfg.Host, s.cfg.Port, s.cfg.User)
	fmt.Println("clean command: ", cmdStrArgs)
	cmd := exec.Command("/bin/sh", "-c", cmdStrArgs)
	if err := cmd.Run(); err != nil {
		fmt.Println("run drop database failed", err)
		return err
	}
	return nil
}
