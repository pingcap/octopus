package backend

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

const (
	defaultPort       = 20170
	defaultDir        = "."
	defaultAnsibleDir = "ansible"
)

type ClusterDSN struct {
	Name         string `toml:"name"`
	Host         string `toml:"host"`
	Port         int    `toml:"port"`
	DB           string `toml:"db"`
	AuthUser     string `toml:"user"`
	AuthPassword string `toml:"password"`
}

type AnsibleConfig struct {
	Dir      string       `toml:"dir"`
	Clusters []ClusterDSN `toml:"clusters"`
}

type ServerConfig struct {
	Port    int           `toml:"port"`
	Dir     string        `toml:"dir"`
	Ansible AnsibleConfig `toml:"ansible"`
}

func ParseConfig(filePath string) (*ServerConfig, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	cfg := new(ServerConfig)
	if err = toml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	cfg.adjust()

	return cfg, nil
}

func NewServerConfig() *ServerConfig {
	cfg := new(ServerConfig)
	cfg.adjust()
	return cfg
}

func (svrCfg *ServerConfig) adjust() {
	adjustInt(&svrCfg.Port, defaultPort)
	adjustStr(&svrCfg.Dir, defaultDir)

	svrCfg.Ansible.adjust()
}

func (ansCfg *AnsibleConfig) adjust() {
	adjustStr(&ansCfg.Dir, defaultAnsibleDir)
}

func adjustInt(num *int, defVal int) {
	if *num == 0 {
		*num = defVal
	}
}

func adjustStr(sz *string, defVal string) {
	if len(*sz) == 0 {
		*sz = defVal
	}
}
