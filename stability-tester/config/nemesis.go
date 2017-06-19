// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

// Configuration for nemeses related sturcts.

// Targets contains lists of PD, TiKV and TiDB.
type Targets struct {
	Pd   []string `toml:"pd"`
	Tikv []string `toml:"tikv"`
	Tidb []string `toml:"tidb"`
}

// NemesesConfig is the configuration for nemeses.
type NemesesConfig struct {
	// Configs maps nemesis name to nemesis configuration. The values are
	// string literals of nemesis configurations.
	// Leave it empty to disable nemesis.
	Configs map[string]string `toml:"cfgs"`
	// Wait is the wait time before executing next nemesis.
	Wait Duration `toml:"wait"`
	// Targets is a list of string.
	Targets *Targets `toml:"targets"`
	// Ranges are strategys for picking targets.
	Ranges []string `toml:"ranges"`
}
