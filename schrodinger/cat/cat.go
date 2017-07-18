package cat

import "time"

const (
	STATUSOK = iota
	STATUSERROR
	STATUSSTOP
)

type CATStatus int

type Cat struct {
	Name      string        `yaml:"name"`
	CodePath  string        `yaml:"code-path"`
	RunTime   time.Duration `yaml:"duration"`
	AlertURL  string        `yaml:"alert-url"`
	IsNamesis bool          `yaml:"namesis"`
	Status    CATStatus     `yaml:"-"`
}

//func (c *Cat) start() {
//}

//func (c *Cat) stop() {
//}

//func (c *Cat) initCluster() {
//}

//func (c *Cat) destroyCluster() {
//}
