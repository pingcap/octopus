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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-operator/pkg/client"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/pkg/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	// OperatorNamespace is the namespace of tidb-operator
	OperatorNamespace = v1.NamespaceDefault

	// NamespaceKubeSystem is kube-system
	NamespaceKubeSystem = "kube-system"

	// OperatorName is the name of tidb-operator
	OperatorName = "tidb-operator"

	// SchedulerName is the  name of tidb-scheduler
	SchedulerName = "tidb-scheduler"

	// VolumeManagerName is the  name of tidb-volume-manager
	VolumeManagerName = "tidb-volume-manager"

	// VolumeConfigMapName is the name of tidb-volume ConfigMap
	VolumeConfigMapName = "tidb-volume"

	// LabelNodeRoleMaster specifies that a node is a master
	LabelNodeRoleMaster = "node-role.kubernetes.io/master"
)

// Framework supports common operations used by e2e tests
type Framework struct {
	BaseName string
	Client   client.Interface
}

// New returns a new framework and sets up a client for you
func New(baseName string) *Framework {
	f := &Framework{
		BaseName: baseName,
	}

	BeforeEach(f.BeforeEach)
	AfterEach(f.AfterEach)

	return f
}

// BeforeEach gets a client
func (f *Framework) BeforeEach() {
	By("Creating a client")
	cli, err := client.New("")
	Expect(err).NotTo(HaveOccurred())

	f.Client = cli
}

// AfterEach does nothing now
func (f *Framework) AfterEach() {
}

// GetAllClusterPods gets all pods of a TidbCluster, include: pd, tidb, tikv
func (f *Framework) GetAllClusterPods(ns, clusterName string) (*v1.PodList, error) {
	return f.Client.CoreV1().Pods(ns).List(metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(
			label.New().Cluster(clusterName).Labels(),
		).String(),
	})
}

// GetAllPDPods gets all PD pods of a TidbCluster
func (f *Framework) GetAllPDPods(ns, clusterName string) (*v1.PodList, error) {
	return f.getAllPodsFor("pd", ns, clusterName)
}

// GetAllTiDBPods gets all TiDB pods of a TidbCluster
func (f *Framework) GetAllTiDBPods(ns, clusterName string) (*v1.PodList, error) {
	return f.getAllPodsFor("tidb", ns, clusterName)
}

// GetAllTiKVPods gets all TiKV pods of a TidbCluster
func (f *Framework) GetAllTiKVPods(ns, clusterName string) (*v1.PodList, error) {
	return f.getAllPodsFor("tikv", ns, clusterName)
}

func (f *Framework) getAllPodsFor(memberType, ns, clusterName string) (*v1.PodList, error) {
	return f.Client.CoreV1().Pods(ns).List(metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(
			label.New().Cluster(clusterName).App(memberType).Labels(),
		).String(),
	})
}

// WaitAllClusterResourcesDeleted ensures all pods of a TidbCluster deleted
// TODO also delete related: pv pvc
func (f *Framework) WaitAllClusterResourcesDeleted(ns, name string) error {
	err := f.Client.PingcapV1().TidbClusters(ns).Delete(name, nil)
	if err != nil {
		return errors.Trace(err)
	}

	return wait.Poll(5*time.Second, 5*time.Minute, func() (bool, error) {
		podList, err := f.GetAllClusterPods(ns, name)
		if err != nil {
			return false, err
		}

		if len(podList.Items) > 0 {
			return false, nil
		}

		return true, nil
	})
}
