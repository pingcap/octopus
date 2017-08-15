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
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/benchbot/common"

	. "github.com/pingcap/octopus/benchbot/cluster"
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
}

func NewTPCHSuite(cfg *TPCHConfig) *TPCHSuite {
	return &TPCHSuite{cfg: cfg}
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
	return ""
}

type lineDiff struct {
	line   int
	column int
}

func (s *TPCHSuite) diffFiles(f1, f2 string) (tp DiffType, diffs []lineDiff, err error) {
	scanner1, err := getFileScanner(f1)
	if err != nil {
		return diffNone, nil, err
	}
	scanner2, err := getFileScanner(f2)
	if err != nil {
		return diffNone, nil, err
	}

	f1Eof, f2Eof := false, false
	li, col := 0, 0
	for {
		if !scanner1.Scan() {
			f1Eof = true
		}

		if !scanner1.Scan() {
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
		return diffNone, nil, err
	}

	if err = scanner2.Err(); err != nil {
		return diffNone, nil, err
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
	return fmt.Sprintf("%s/check_answers/%s/q%d.out", s.cfg.ScriptsDir, resultName, index)
}

func (s *TPCHSuite) checkAnswers(stat *StatManager) error {
	start := time.Now()
	for i := 0; i < 23; i++ {
		mysqlOut := s.resultFile("mysql_r", i)
		tidbOut := s.resultFile("tidb_r", i)
		tp, diffs, err := s.diffFiles(mysqlOut, tidbOut)
		if err != nil {
			return err
		}
		if tp == fileEqual {
			stat.Record(OPRead, nil, time.Since(start))
			continue
		}
		err = fmt.Errorf("[tpch failed]: Query:%d %v", i, diffTypeToString(tp))
		stat.Record(OPRead, err, time.Since(start))
		log.Error("%v\n", err)
		for _, diff := range diffs {
			log.Errorf("Query:%d Result Unmatch (Line, Column):(%v, %v)\n", i, diff.line, diff.column)
		}
	}
	return nil
}

func (s *TPCHSuite) Run(cluster Cluster) ([]*CaseResult, error) {
	stat := NewStatManager()
	stat.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := fmt.Sprintf("bash %s/run-tpch.sh tidb_r %s %d", s.cfg.ScriptsDir, s.cfg.Host, s.cfg.Port)
	_, err := common.ExecCmd(ctx, cmd)
	if err != nil {
		return nil, err
	}

	err = s.checkAnswers(stat)
	if err != nil {
		return nil, err
	}

	stat.Close()
	res := []*CaseResult{}
	res = append(res, NewCaseResult(s.Name(), stat.Result()))
	return res, nil
}
