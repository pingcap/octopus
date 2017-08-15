package backend

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"

	. "github.com/pingcap/octopus/benchbot/cluster"
)

const (
	defaultPort       = 20170
	defaultDir        = "."
	defaultLogDir     = "logs"
	defaultAnsibleDir = "ansible"
)

type ServerConfig struct {
	Port    int            `toml:"port"`
	Dir     string         `toml:"dir"`
	LogDir  string         `toml:"log_dir"`
	Ansible *AnsibleConfig `toml:"ansible"`
}

func (cfg *ServerConfig) adjust() {
	adjustInt(&cfg.Port, defaultPort)
	adjustStr(&cfg.Dir, defaultDir)
	adjustStr(&cfg.LogDir, defaultLogDir)
	adjustStr(&cfg.Ansible.Dir, defaultAnsibleDir)
}

func adjustInt(i *int, defVal int) {
	if *i == 0 {
		*i = defVal
	}
}

func adjustStr(s *string, defVal string) {
	if len(*s) == 0 {
		*s = defVal
	}
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
