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
	"math"
	"sort"
	"time"

	. "github.com/pingcap/octopus/benchbot/common"
	log "github.com/sirupsen/logrus"
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
	Query           string
	Cost, otherCost time.Duration
	Diff            float64
}

type CRS []*CompareResult

func (c CRS) Len() int           { return len(c) }
func (c CRS) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c CRS) Less(i, j int) bool { return math.Abs(c[i].Diff) > math.Abs(c[j].Diff) }

// CompareTPCHCost compares two tpch result
func CompareTPCHCost(s, t *TPCHResultStat) CRS {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("recover from panic %v", r)
		}
	}()
	var (
		//output bytes.Buffer
		stats = make(CRS, 0, len(s.Cost))
	)

	for query, cost := range s.Cost {
		if otherCost, ok := t.Cost[query]; ok {
			r := &CompareResult{
				Query:     query,
				Cost:      cost,
				otherCost: otherCost,
			}
			if cost == 0 {
				r.Diff = math.MaxFloat64
			} else {
				r.Diff = float64(otherCost-cost) / float64(cost)
			}
			stats = append(stats, r)
		}
	}

	sort.Sort(stats)
	/*for _, stat := range stats {
		fmt.Fprintf(&output, "query %s    %6d ms | %6d ms    [ %4.2f %%]\n", stat.query, stat.cost, stat.otherCost, stat.diff)
	}
	*/

	return stats
}
