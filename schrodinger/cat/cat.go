package cat

import (
	"time"

	"github.com/pingcap/octopus/schrodinger/cluster"
)

const (
	OK       = "running"
	ERROR    = "error"
	STOP     = "stop"
	BUILDING = "building"
)

type CATStatus int

type Cat struct {
	Name      string          `yaml:"name" json:"name, omitempty"`
	CodePath  string          `yaml:"code-path" json:"code-path, omitempty"`
	RunTime   time.Duration   `yaml:"duration" json:"duration, omitempty"`
	AlertURL  string          `yaml:"alert-url" json:"alert-url, omitempty"`
	IsNamesis bool            `yaml:"namesis" json:"namesis, omitempty"`
	Cluster   cluster.Cluster `yaml:"cluster" json:"cluster, omitempty"`
	Status    CATStatus       `yaml:"-", json:"-"`
}

func (c *Cat) Valid() bool {
	return len(c.Name) > 0 && len(c.CodePath) > 0 && len(c.AlertURL) > 0 && c.Cluster.Valid()
}

//func (c *cat) start() {
//}

//func (c *cat) stop() {
//}

//func (c *cat) initCluster() {
//}

//func (c *cat) destroyCluster() {
//}

//func (c *cat) startNamesis() {
//}

//func (c *cat) stopNamesis() {
//}

//func (c *cat) parse(path string) {
//}

//func (c *cat) run() {
//}

//func (c *cat) alert() {
//}
