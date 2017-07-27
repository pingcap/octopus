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
// limitations under the License.package spec

package framework

import (
	"encoding/json"
	"flag"
	"html/template"
	"os"
	"os/exec"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-operator/pkg/client"
	"github.com/pingcap/tidb-operator/pkg/scheduler/volume"

	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

var operatorImage string

func init() {
	flag.StringVar(&operatorImage, "operator-image", "", "The tidb-operator image")
}

// Bootstrap is responsible for starting operator
type Bootstrap struct {
	Client client.Interface
}

// NewBootstrap returns a Bootstrap
func NewBootstrap() (*Bootstrap, error) {
	cli, err := client.New("")
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Bootstrap{cli}, nil
}

// Setup creates or updates these components:
// 1, tidb-scheduler
// 2, tidb-operator
// 3, tidb-volume-manager
// 4, tidb-volume
func (b *Bootstrap) Setup() error {
	err := b.setupTiDBOperator()
	if err != nil {
		return err
	}

	return b.setupTidbVolumeConfigMap()
}

// Teardown does nothing now
func (b *Bootstrap) Teardown() error {
	return nil
}

func (b *Bootstrap) setupTiDBOperator() error {
	f, err := os.Create("/tmp/tidb-operator.yaml")
	if err != nil {
		return errors.Trace(err)
	}

	tmpl, err := template.ParseFiles("/tmp/tidb-operator.yaml.tmpl")
	if err != nil {
		return errors.Trace(err)
	}

	err = tmpl.Execute(f, struct{ Image string }{operatorImage})
	if err != nil {
		return errors.Trace(err)
	}

	clearCmd := exec.Command("/bin/sh", "-c", "/usr/local/bin/kubectl delete --namespace=kube-system -f /tmp/tidb-operator.yaml")
	clearData, err := clearCmd.CombinedOutput()
	if err != nil {
		Logf(string(clearData))
	}

	cmd := exec.Command("/bin/sh", "-c", "/usr/local/bin/kubectl apply --namespace=kube-system -f /tmp/tidb-operator.yaml")
	data, err := cmd.CombinedOutput()
	if err != nil {
		Logf(string(data))
	}

	return err
}

func (b *Bootstrap) nodes() ([]string, error) {
	var nodes []string
	nodeList, err := b.Client.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, node := range nodeList.Items {
		var addr string
		for _, nodeAddr := range node.Status.Addresses {
			if nodeAddr.Type == v1.NodeInternalIP {
				addr = nodeAddr.Address
				break
			}
		}

		if addr != "" {
			nodes = append(nodes, addr)
		}
	}

	return nodes, nil
}

func (b *Bootstrap) setupTidbVolumeConfigMap() error {
	nodes, err := b.nodes()
	if err != nil {
		return err
	}

	var tidbVolumes volume.TiDBVolumes
	for _, node := range nodes {
		tidbVolumes.Nodes = append(tidbVolumes.Nodes, volume.NodeVolumes{
			Name: node,
			// TODO supply volumes according to need: https://github.com/pingcap/tidb-operator/issues/81
			Directories: []volume.Directory{
				{Path: "/tidb/tikv-dir-1", Size: "100Gi"},
				{Path: "/tidb/tikv-dir-2", Size: "100Gi"},
				{Path: "/tidb/tikv-dir-3", Size: "100Gi"},
			},
		})
	}

	data, err := json.Marshal(tidbVolumes)
	if err != nil {
		return errors.Trace(err)
	}
	dataStr := string(data)

	cm, err := b.Client.CoreV1().ConfigMaps(NamespaceKubeSystem).Get(VolumeConfigMapName, metav1.GetOptions{})
	if err != nil {
		if !apierrs.IsNotFound(err) {
			return errors.Trace(err)
		}

		cm = &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      VolumeConfigMapName,
				Namespace: NamespaceKubeSystem,
			},
			Data: map[string]string{"volume": dataStr},
		}

		_, err := b.Client.CoreV1().ConfigMaps(NamespaceKubeSystem).Create(cm)
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	cm.Data = map[string]string{"volume": dataStr}
	_, err = b.Client.CoreV1().ConfigMaps(NamespaceKubeSystem).Update(cm)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
