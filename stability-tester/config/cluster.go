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

import (
	"fmt"
	"os/user"

	"github.com/ngaut/log"
)

// SSHConfig is the configuration for `ssh`.
type SSHConfig struct {
	User    string `toml:"user"`
	Port    int    `toml:"port"`
	KeyPath string `toml:"keypath"`
}

// ServiceConfig is the configuration to run the service.
type ServiceConfig struct {
	// Service name
	Name string `toml:"name"`

	// The type of hte service node,Docker,Binary or others
	Type string `toml:"type"`

	// Host is the remote node host which we can connect to.
	Host string `toml:"host"`

	// Arguments for the service.specially,in binary node,we put all command in here
	Command []string `toml:"command"`

	Path string `toml:"path"`

	// SSH for the service, if not not, we will try to use global configuration
	// or the default ("root", 22).
	SSH SSHConfig `toml:"ssh"`
}

// ClusterConfig is the configuration to run the cluster with SSH.
type ClusterConfig struct {
	// SSH is the global SSH configuration.
	// Most of the cases, we have the same user and port for all services.
	SSH SSHConfig `toml:"ssh"`

	Services []ServiceConfig `toml:"services"`
}

func adjustString(v *string, defVal string) {
	if len(*v) == 0 {
		*v = defVal
	}
}

func adjustInt(v *int, defVal int) {
	if *v == 0 {
		*v = defVal
	}
}

func (c *ClusterConfig) adjust() {
	u, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	keyPath := fmt.Sprintf("%s/.ssh/id_rsa", u.HomeDir)

	if c.SSH.User == "" || c.SSH.Port == 0 {
		adjustString(&c.SSH.User, defaultSSHUser)
		adjustInt(&c.SSH.Port, defaultSSHPort)
	}
	if c.SSH.KeyPath == "" {
		adjustString(&c.SSH.KeyPath, keyPath)
	}

	for i := 0; i < len(c.Services); i++ {
		s := &c.Services[i]
		if s.Type != "docker" && s.Type != "binary" {
			adjustString(&s.Type, defaultType)
		}
		if s.SSH.KeyPath == "" {
			adjustString(&s.SSH.KeyPath, c.SSH.KeyPath)
		}
		if s.SSH.User == "" || s.SSH.Port == 0 {
			adjustString(&s.SSH.User, c.SSH.User)
			adjustInt(&s.SSH.Port, c.SSH.Port)
		}
	}
}
