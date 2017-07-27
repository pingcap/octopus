// Copyright 2017 PingCAP, Inc.
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

package volume

import (
	"github.com/pingcap/tidb-operator/pkg/util"
)

// Pool is an interface which returns tidb volumes
type Pool interface {
	// TiDBVolume returns a *TiDBVolumes
	TiDBVolumes() (*TiDBVolumes, error)

	// HostPaths returns all directories on all nodes.
	HostPaths() ([]*HostPath, error)

	// HostPathsFromNodes returns all hostPaths from kubernetes scheduler's nodes
	HostPathsFromNodes([]string) ([]*HostPath, error)
}

// HostPath represents a hostPath on a node.
type HostPath struct {
	NodeName string `json:"nodeName,omitempty"`
	Path     string `json:"path,omitempty"`
	Size     string `json:"size,omitempty"`
}

// TiDBVolumes represents tidb cluster's volumes among all nodes
type TiDBVolumes struct {
	Nodes []NodeVolumes `json:"nodes,omitempty"`
}

// NodeVolumes represents a single node's volumes
type NodeVolumes struct {
	Name        string      `json:"name,omitempty"`
	Directories []Directory `json:"directories,omitempty"`
}

// Directory represents a nodes's directory
type Directory struct {
	Path string `json:"path,omitempty"`
	Size string `json:"size,omitempty"`
}

// PVName return the PV name for a hostPath
func (h *HostPath) PVName() string {
	return util.PVName(h.NodeName, h.Path)
}
