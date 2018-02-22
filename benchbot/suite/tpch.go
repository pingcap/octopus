// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package suite

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/benchbot/common"
	"golang.org/x/net/context"

	. "github.com/pingcap/octopus/benchbot/cluster"
	log "github.com/sirupsen/logrus"
)

const (
	mysqlOutDir = "mysql_r"
	tidbOutDir  = "tidb_r"
	costOutDir  = "cost_r"
)

func init() {
	builder := func(meta toml.MetaData, value toml.Primitive) BenchSuite {
		cfg := new(TPCHConfig)
		meta.PrimitiveDecode(value, cfg)
		return NewTPCHSuite(cfg)
	}
	RegisterBenchSuite("tpch", builder)
}

type TPCHConfig struct {
	Host       string `toml:"host"`
	Port       int    `toml:"port"`
	ScriptsDir string `toml:"scripts_dir"`
}

type TPCHSuite struct {
	cfg *TPCHConfig

	stat *TPCHResultStat
}

func NewTPCHSuite(cfg *TPCHConfig) *TPCHSuite {
	return &TPCHSuite{
		cfg:  cfg,
		stat: newTPCHDetailStat(),
	}
}

func (s *TPCHSuite) Name() string {
	return "tpch"
}

func getFileScanner(fileName string) (*bufio.Scanner, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	return bufio.NewScanner(file), nil
}

// DiffType defines the type of a diff item.
type DiffType int8

const (
	diffNone DiffType = iota
	diffFileLength
	diffLineContent
	fileEqual
)

func diffTypeToString(tp DiffType) string {
	switch tp {
	case diffNone:
		return "diffNone"
	case diffFileLength:
		return "diffFileLength"
	case diffLineContent:
		return "diffLineContent"
	case fileEqual:
		return "fileEqual"
	default:
		return "Unknown"
	}
}

type lineDiff struct {
	line   int
	column int
}

func (s *TPCHSuite) diffFiles(f1, f2 string) (tp DiffType, diffs []lineDiff, err error) {
	scanner1, err := getFileScanner(f1)
	if err != nil {
		return diffNone, nil, errors.Trace(err)
	}
	scanner2, err := getFileScanner(f2)
	if err != nil {
		return diffNone, nil, errors.Trace(err)
	}

	f1Eof, f2Eof := false, false
	li, col := 0, 0
	for {
		if !scanner1.Scan() {
			f1Eof = true
		}

		if !scanner2.Scan() {
			f2Eof = true
		}

		if f1Eof || f2Eof {
			break
		}

		line1 := scanner1.Text()
		line2 := scanner2.Text()
		seps1 := strings.Split(line1, "\t")
		seps2 := strings.Split(line2, "\t")

		if len(seps1) != len(seps2) {
			diff := lineDiff{line: li, column: col}
			diffs = append(diffs, diff)
			continue
		}

		for i := 0; i < len(seps1); i++ {
			sep1 := seps1[i]
			sep2 := seps2[i]
			if sep1 != sep2 {
				diff := lineDiff{line: li, column: col}
				diffs = append(diffs, diff)
				// only report the first diff in one line
				break
			}
			col++
		}
		li++
		col = 0
	}

	if err = scanner1.Err(); err != nil {
		return diffNone, nil, errors.Trace(err)
	}

	if err = scanner2.Err(); err != nil {
		return diffNone, nil, errors.Trace(err)
	}

	// file length is not equal
	if f1Eof == false || f2Eof == false {
		return diffFileLength, diffs, nil
	}

	if len(diffs) > 0 {
		return diffLineContent, diffs, nil
	}
	return fileEqual, diffs, nil
}

func (s *TPCHSuite) resultFile(resultName string, index int) string {
	return fmt.Sprintf("%s/%s/q%d.out", s.cfg.ScriptsDir, resultName, index)
}

func (s *TPCHSuite) costFile(resultName string, index int) string {
	return fmt.Sprintf("%s/%s/q%d.time", s.cfg.ScriptsDir, resultName, index)
}

func (s *TPCHSuite) recordCost(index int, cost time.Duration) {
	s.stat.addCost(fmt.Sprintf("q%d.sql", index), cost)
}

func (s *TPCHSuite) fetchCost(fileName string) (time.Duration, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}

	readder := bufio.NewReader(file)
	line, _, err := readder.ReadLine()
	if err != nil && err != io.EOF {
		return 0, err
	}

	cost, err := strconv.ParseInt(strings.TrimSpace(string(line)), 10, 64)
	if err != nil {
		return 0, err
	}

	return time.Duration(cost), nil
}

func (s *TPCHSuite) checkAnswers(stat *StatManager) error {
	for i := 1; i < 23; i++ {
		if i == 15 {
			// skip view test
			continue
		}
		mysqlOut := s.resultFile(mysqlOutDir, i)
		tidbOut := s.resultFile(tidbOutDir, i)
		costOut := s.costFile(costOutDir, i)

		cost, err := s.fetchCost(costOut)
		if err != nil {
			return err
		}

		tp, diffs, err := s.diffFiles(mysqlOut, tidbOut)
		if err != nil {
			return err
		}
		if tp == fileEqual {
			s.recordCost(i, cost)
			stat.Record(OPRead, nil, cost)
			continue
		}
		err = fmt.Errorf("[tpch failed]: Query:%d %v", i, diffTypeToString(tp))
		stat.Record(OPRead, err, cost)
		log.Errorf("%v\n", err)
		for _, diff := range diffs {
			log.Errorf("Query:%d Result Unmatch (Line, Column):(%v, %v)\n", i, diff.line, diff.column)
		}
	}

	stat.RecordOther(s.Name(), json.RawMessage(s.stat.FormatJSON()))
	return nil
}

// Run runs tpch suit
func (s *TPCHSuite) Run(cluster Cluster) ([]*CaseResult, error) {
	err := cluster.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer cluster.Close()
	return s.run()
}

func (s *TPCHSuite) run() ([]*CaseResult, error) {
	stat := NewStatManager()
	stat.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := fmt.Sprintf("bash %s/run-tpch.sh tidb_r %s %d", s.cfg.ScriptsDir, s.cfg.Host, s.cfg.Port)
	output, err := common.ExecCmd(ctx, cmd)
	if err != nil {
		err = fmt.Errorf("err:%v, output:%v", err, output)
		return nil, errors.Trace(err)
	}

	err = s.checkAnswers(stat)
	if err != nil {
		return nil, errors.Trace(err)
	}

	stat.Close()
	res := []*CaseResult{}
	res = append(res, NewCaseResult(s.Name(), stat.Result()))
	log.Infof("[case:%s] end:%v", s.Name(), stat.Result().FormatJSON())
	return res, nil
}
