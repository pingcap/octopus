package suite

import (
	. "github.com/pingcap/octopus/benchbot/pkg"
)

type CaseResult struct {
	Name    string       `json:"name"`
	Summary *StatIndex   `json:"summary"`
	Stages  []*StatIndex `json:"stages"`
}

func NewCaseResult(name string) *CaseResult {
	return &CaseResult{
		Name:   name,
		Stages: make([]*StatIndex, 0),
	}
}

func (c *CaseResult) Dump() string {
	if data, err := DumpJSON(c, true); err != nil {
		return ""
	} else {
		return data
	}
}
