package backend

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

const (
	defaultPort       = 20170
	defaultDir        = "./"
	defaultAnsibleDir = "./ansible"
)

type ClusterDSN struct {
	Name         string `toml:"name"`
	Host         string `toml:"host"`
	Port         uint16 `toml:"port"`
	DB           string `toml:"db"`
	AuthUser     string `toml:"user"`
	AuthPassword string `toml:"password"`
}

type AnsibleConfig struct {
	Dir      string       `toml:"dir"`
	Clusters []ClusterDSN `toml:"clusters"`
}

type BenchConfig struct {
	Cases []string `toml:"port"`
}

type ServerConfig struct {
	Port    int           `toml:"port"`
	Dir     string        `toml:"dir"`
	Ansible AnsibleConfig `toml:"ansible"`
	Bench   BenchConfig   `toml:"bench"`
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

func adjustStr(sz *string, defVal string) {
	if len(*sz) == 0 {
		*sz = defVal
	}
}

func adjustInt(num *int, defVal int) {
	if *num == 0 {
		*num = defVal
	}
}

func (svrCfg *ServerConfig) adjust() {
	adjustInt(&svrCfg.Port, defaultPort)
	adjustStr(&svrCfg.Dir, defaultDir)

	svrCfg.Ansible.adjust()
	svrCfg.Bench.adjust()
}

func (ansCfg AnsibleConfig) adjust() {
	adjustStr(&ansCfg.Dir, defaultAnsibleDir)
}

func (bchCfg BenchConfig) adjust() {

}
