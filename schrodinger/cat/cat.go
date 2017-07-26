package cat

import (
	"github.com/pingcap/octopus/schrodinger/cluster"
	"github.com/pingcap/octopus/stability-tester/nemesis"
)

const (
	OK       = "running"
	ERROR    = "error"
	STOP     = "stop"
	BUILDING = "building"
)

type Status string

type Cat struct {
	Name    string          `yaml:"name" json:"name, omitempty"`
	Control Control         `yaml:"control" json:"control"`
	Cases   []Case          `yaml:"case" json:"case"`
	Cluster cluster.Cluster `yaml:"cluster" json:"cluster, omitempty"`
	Status  CATStatus       `yaml:"status", json:"status"`
	Nemesis nemesis.Nemesis `yaml:"nemesis" json:"nemesis"`
}

type Control struct {
	AlertURL  string `yaml:"alert-url" json:"alert-url, omitempty"`
	Repeat    int    `yaml:"repeat" json:"repeat"`
	TimeLimit string `yaml:"time-limit" json:"time-limit"`
	CIHook    string `yaml:"ci-hook" json:"ci-hool"`
}

func (c *Cat) Valid() bool {
	// TODO: check cat
	return true
}
