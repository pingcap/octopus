package cat

type Case struct {
	CodePath string   `yaml:"code-path" json:"code-path"`
	Scripts  []string `yaml:"scripts" json:"scripts"`
	Order    int      `yaml:"order" json:"order"`
	Status   Status   `yaml:"status" json:"status"`
}
