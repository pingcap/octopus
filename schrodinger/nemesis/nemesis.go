package nemesis

type Nemesis struct {
	RandomKill       bool `yaml:"random-kill" json:"random-kill"`
	Lantency         bool `yaml:"lantency" json:"lantency"`
	NetworkIsolation bool `yaml:"network-isolation" json:"network-isolation"`
}
