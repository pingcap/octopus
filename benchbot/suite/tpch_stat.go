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
	"bytes"
	"fmt"
	"math"
	"sort"
	"time"

	. "github.com/pingcap/octopus/benchbot/common"
)

// TPCHDetailStat contians detail result of tpch
type TPCHResultStat struct {
	Cost map[string]time.Duration
}

func newTPCHDetailStat() *TPCHResultStat {
	return &TPCHResultStat{
		Cost: make(map[string]time.Duration),
	}
}

func (t *TPCHResultStat) addCost(query string, cost time.Duration) {
	t.Cost[query] = cost
}

// FormatJSON formats json
func (t *TPCHResultStat) FormatJSON() string {
	data, err := DumpJSON(t, true)
	if err != nil {
		return err.Error()
	}
	return data
}

type CompareResult struct {
	query           string
	cost, otherCost time.Duration
	diff            float64
}

type CRS []*CompareResult

func (c CRS) Len() int           { return len(c) }
func (c CRS) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c CRS) Less(i, j int) bool { return c[i].diff < c[j].diff }

// CompareTPCHCost compares two tpch result
func CompareTPCHCost(s, t *TPCHResultStat) string {
	var (
		output bytes.Buffer
		stats  = make(CRS, len(s.Cost))
	)

	for query, cost := range s.Cost {
		if otherCost, ok := t.Cost[query]; ok {
			r := &CompareResult{
				query:     query,
				cost:      cost,
				otherCost: otherCost,
			}
			if cost == 0 {
				r.diff = math.MaxFloat64
			} else {
				r.diff = float64(otherCost-cost) / float64(cost)
			}
		}
	}

	sort.Sort(stats)
	for _, stat := range stats {
		sign := "+"
		if stat.diff < 0 {
			sign = "-"
		}
		fmt.Fprintf(&output, "query %s -	%d ms	|	%d ms	[%s%v%%]\n", stat.query, stat.cost, stat.otherCost, sign, stat.diff)
	}
	return output.String()
}
