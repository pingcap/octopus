package cat

import "time"

const (
	STATUSOK = iota
	STATUSERROR
	STATUSSTOP
)

type CATStatus int

type cat struct {
	Name        string        `yaml:"name"`
	CodePath    string        `yaml:"code-path"`
	RunTime     time.Duration `yaml:"duration"`
	AlertURL    string        `yaml:"alert-url"`
	IsNamesis   bool          `yaml:"namesis"`
	Status      CATStatus     `yaml:"-"`
	namespace   string
	clusterName string
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
