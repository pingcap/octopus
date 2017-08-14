package cluster

type BinPackage struct {
	Repo     string `json:"repo"`
	Branch   string `json:"branch"`
	Tag      string `json:"tag"`
	GitHash  string `json:"git_hash"`
	Platform string `json:"platform"`
	BinUrl   string `json:"binary_url"`
}

func (p *BinPackage) Valid() bool {
	return (len(p.Branch) > 0 || len(p.Tag) > 0) && len(p.GitHash) > 0
}

type ClusterDSN struct {
	Name     string `toml:"name"`
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	DB       string `toml:"db"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

type AnsibleConfig struct {
	Dir      string        `toml:"dir"`
	Clusters []*ClusterDSN `toml:"clusters"`
}
