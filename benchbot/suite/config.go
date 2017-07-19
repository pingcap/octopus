package suite

import (
	_ "io/ioutil"

	_ "github.com/BurntSushi/toml"
)

type BenchSuiteConfig struct {
}

func NewBenchSuiteConfig(filepath string) *BenchSuiteConfig {
	return new(BenchSuiteConfig)
}
