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
	"encoding/json"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-operator/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type configMapPool struct {
	cli kubernetes.Interface
}

// NewConfigMapPool return a configmap volume Pool instance
func NewConfigMapPool(cli kubernetes.Interface) Pool {
	return &configMapPool{cli}
}

func (cmp *configMapPool) TiDBVolumes() (*TiDBVolumes, error) {
	cm, err := cmp.cli.CoreV1().ConfigMaps("kube-system").Get("tidb-volume", metav1.GetOptions{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	tidbVolume := &TiDBVolumes{}

	err = json.Unmarshal([]byte(cm.Data["volume"]), tidbVolume)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tidbVolume, nil
}

func (cmp *configMapPool) HostPaths() ([]*HostPath, error) {
	tidbVol, err := cmp.TiDBVolumes()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var paths []*HostPath

	for _, nodeVol := range tidbVol.Nodes {
		for _, dir := range nodeVol.Directories {
			paths = append(paths, &HostPath{NodeName: nodeVol.Name, Path: dir.Path, Size: dir.Size})
		}
	}

	return paths, nil
}

func (cmp *configMapPool) HostPathsFromNodes(nodes []string) ([]*HostPath, error) {
	tidbVol, err := cmp.TiDBVolumes()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var paths []*HostPath

	for _, nodeVol := range tidbVol.Nodes {
		if !util.ContainsString(nodes, nodeVol.Name) {
			continue
		}

		for _, dir := range nodeVol.Directories {
			paths = append(paths, &HostPath{NodeName: nodeVol.Name, Path: dir.Path, Size: dir.Size})
		}
	}

	return paths, nil
}
