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

package e2e

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/pingcap/tidb-operator/pkg/util/label"
	"github.com/pingcap/tidb-operator/test/e2e/framework"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/pkg/api/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TidbCluster", func() {
	f := framework.New("tidbcluster")

	It("should works as expected if only one TidbCluster", func() {
		clusterName := "e2e-one"

		By("Given a TidbCluster TPR")

		By("When it is created")
		cmd := exec.Command("/bin/sh", "-c", "/usr/local/bin/kubectl apply -f /tmp/data/e2e-one.yaml")
		data, err := cmd.CombinedOutput()
		if err != nil {
			framework.Logf(string(data))
		}
		Expect(err).NotTo(HaveOccurred())

		By("Then its members are running")
		err = wait.Poll(5*time.Second, 10*time.Minute, func() (bool, error) {
			podList, err := f.GetAllClusterPods(v1.NamespaceDefault, clusterName)
			if err != nil {
				return false, nil
			}

			// default: 1 pd, 2 tidb, 3 tikv
			// TODO refactor: add pd/tidb/tikv TPR
			if len(podList.Items) != 6 {
				return false, nil
			}

			for _, pod := range podList.Items {
				// FIXME pod running don't means ready
				if pod.Status.Phase != v1.PodRunning {
					return false, nil
				}
			}

			return true, nil
		})
		Expect(err).NotTo(HaveOccurred())

		By("And its PVCs are created")
		pvcList, err := f.Client.CoreV1().PersistentVolumeClaims(v1.NamespaceDefault).List(metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(
				label.New().Cluster(clusterName).Labels(),
			).String(),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(pvcList.Items)).To(Equal(3))

		By("And its PVs are created")
		pvList, err := f.Client.CoreV1().PersistentVolumes().List(metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(
				label.New().Cluster(clusterName).Labels(),
			).String(),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(pvList.Items)).To(Equal(3))

		By("And its cluster configmap is created")
		_, err = f.Client.CoreV1().ConfigMaps(v1.NamespaceDefault).Get(fmt.Sprintf("%s-config", clusterName), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("And its TiDB's service and PD's service are created")
		_, err = f.Client.CoreV1().Services(v1.NamespaceDefault).Get(fmt.Sprintf("%s-tidb", clusterName), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		_, err = f.Client.CoreV1().Services(v1.NamespaceDefault).Get(fmt.Sprintf("%s-pd", clusterName), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("When resize pd and tidb's replicas to 3 and tikv's replicas to 5")
		tc, err := f.Client.PingcapV1().TidbClusters(v1.NamespaceDefault).Get(clusterName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		tc.Spec.PD.Size = 3
		tc.Spec.TiDB.Size = 3
		tc.Spec.TiKV.Size = 5

		_, err = f.Client.PingcapV1().TidbClusters(v1.NamespaceDefault).Update(tc)
		Expect(err).NotTo(HaveOccurred())

		By("Then pd's pods size are 3")
		err = wait.Poll(5*time.Second, 10*time.Minute, func() (bool, error) {
			podList, err := f.GetAllPDPods(v1.NamespaceDefault, clusterName)
			if err != nil {
				return false, nil
			}

			if len(podList.Items) != 3 {
				return false, nil
			}

			for _, pod := range podList.Items {
				// framework.Logf("pod: %s, phase: %v", pod.Name, pod.Status.Phase)
				// FIXME pod running don't means ready
				if pod.Status.Phase != v1.PodRunning {
					return false, nil
				}
			}

			return true, nil
		})
		Expect(err).NotTo(HaveOccurred())

		By("And tidb's pods size are 3")
		err = wait.Poll(5*time.Second, 10*time.Minute, func() (bool, error) {
			podList, err := f.GetAllTiDBPods(v1.NamespaceDefault, clusterName)
			if err != nil {
				return false, nil
			}

			if len(podList.Items) != 3 {
				return false, nil
			}

			for _, pod := range podList.Items {
				// framework.Logf("pod: %s, phase: %v", pod.Name, pod.Status.Phase)
				// FIXME pod running don't means ready
				if pod.Status.Phase != v1.PodRunning {
					return false, nil
				}
			}

			return true, nil
		})
		Expect(err).NotTo(HaveOccurred())

		By("And tikv's pods size are 5")
		err = wait.Poll(5*time.Second, 10*time.Minute, func() (bool, error) {
			podList, err := f.GetAllTiKVPods(v1.NamespaceDefault, clusterName)
			if err != nil {
				return false, nil
			}

			if len(podList.Items) != 5 {
				return false, nil
			}

			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false, nil
				}
			}
			return true, nil
		})
		Expect(err).NotTo(HaveOccurred())

		By("When resize tikv's replicas to 3")
		tc, err = f.Client.PingcapV1().TidbClusters(v1.NamespaceDefault).Get(clusterName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		tc.Spec.TiKV.Size = 3

		_, err = f.Client.PingcapV1().TidbClusters(v1.NamespaceDefault).Update(tc)
		Expect(err).NotTo(HaveOccurred())

		By("Then tikv's pods size are 3")
		err = wait.Poll(5*time.Second, 30*time.Minute, func() (bool, error) {
			podList, err := f.GetAllTiKVPods(v1.NamespaceDefault, clusterName)
			if err != nil {
				return false, nil
			}

			if len(podList.Items) != 3 {
				return false, nil
			}

			for _, pod := range podList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false, nil
				}
			}
			return true, nil
		})
		Expect(err).NotTo(HaveOccurred())

		err = f.WaitAllClusterResourcesDeleted(v1.NamespaceDefault, clusterName)
		Expect(err).NotTo(HaveOccurred())
	})
})
