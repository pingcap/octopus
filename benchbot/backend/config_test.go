package backend

import (
	"testing"
)

func TestLoadConfig(t *testing.T) {
	cfg, err := ParseConfig("../config.toml")
	if err != nil {
		t.Fatal(err)
	}

	t.Log(cfg)
}
