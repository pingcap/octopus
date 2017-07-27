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

package scheduler

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/client"
	"github.com/pingcap/tidb-operator/pkg/scheduler/volume"
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	apiv1 "k8s.io/kubernetes/pkg/api/v1"
	schedulerapiv1 "k8s.io/kubernetes/plugin/pkg/scheduler/api/v1"
)

const (
	// AnnotationHostPath is a hostPath annotation key
	AnnotationHostPath string = "volume.pingcap.com/hostPath"
	// AnnotationProvisioner is a provisioner annotation key
	AnnotationProvisioner string = "volume.pingcap.com/provisioner"
	// AnnotationPVCName is a pvcName annotation key
	AnnotationPVCName string = "volume.pingcap.com/pvcName"

	// LabelPodName is a podName label key
	LabelPodName string = "volume.pingcap.com/podName"
	// LabelPodNamespace is a podNamespace label key
	LabelPodNamespace string = "volume.pingcap.com/podNamespace"
	// LabelNodeName is a nodeName label key
	LabelNodeName string = "volume.pingcap.com/nodeName"

	// ProvisionerName is the provisioner that storage classes will specify.
	ProvisionerName string = "hostpath"
)

var _ Scheduler = &simpleScheduler{}

type simpleScheduler struct {
	volumePool volume.Pool
	cli        kubernetes.Interface
}

// NewSimpleScheduler returns a simpleScheduler instance
func NewSimpleScheduler(kubeconfig string) (Scheduler, error) {
	cfg, err := client.NewConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	cli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &simpleScheduler{volume.NewConfigMapPool(cli), cli}, nil
}

// Filter select a node from *schedulerapiv1.ExtenderArgs.Nodes when there is a app=tikv label in the pod's labels,
// else return the original nodes.
func (ss *simpleScheduler) Filter(args *schedulerapiv1.ExtenderArgs) (*schedulerapiv1.ExtenderFilterResult, error) {
	// only for tikv
	if l, ok := args.Pod.Labels[label.AppLabelKey]; ok && l == label.TiKVLabelVal {
		node, err := ss.selectANodeForTiKV(args)
		if err != nil {
			log.Errorf("can't select a node for tikv: %v", err)
			return nil, errors.Trace(err)
		}

		return &schedulerapiv1.ExtenderFilterResult{
			Nodes: &apiv1.NodeList{
				Items: []apiv1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{Name: node.Name},
					},
				},
			},
		}, nil
	}

	return &schedulerapiv1.ExtenderFilterResult{
		Nodes: args.Nodes,
	}, nil
}

// We didn't pass `prioritizeVerb` to kubernetes scheduler extender's config file, this method will not be called.
func (ss *simpleScheduler) Priority(args *schedulerapiv1.ExtenderArgs) (schedulerapiv1.HostPriorityList, error) {
	result := schedulerapiv1.HostPriorityList{}

	// avoid index out of range panic
	if len(args.Nodes.Items) > 0 {
		result = append(result, schedulerapiv1.HostPriority{
			Host:  args.Nodes.Items[0].Name,
			Score: 0,
		})
	}

	return result, nil
}

// Only select the first suitable node.
// TODO add more useful algorithm
// TODO extract this method to a provisioner
func (ss *simpleScheduler) selectANodeForTiKV(args *schedulerapiv1.ExtenderArgs) (*apiv1.Node, error) {
	podName := args.Pod.Name
	podNamespace := args.Pod.Namespace

	var pvcName string
	for _, vol := range args.Pod.Spec.Volumes {
		if vol.Name == "data" && vol.PersistentVolumeClaim != nil {
			pvcName = vol.PersistentVolumeClaim.ClaimName
		}
	}

	if pvcName == "" {
		return nil, errors.Errorf("pod: %s must have an associate PVC", podName)
	}

	// pvc must exist
	pvc, err := ss.cli.CoreV1().PersistentVolumeClaims(podNamespace).Get(pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	pvcQuantity, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	if !ok {
		return nil, errors.Errorf("PVC: %s must have a request storage size", pvcName)
	}

	// nodes from kubernetes's scheduler
	schedulerNodes := make([]string, len(args.Nodes.Items))
	for _, n := range args.Nodes.Items {
		var addr string
		for _, nodeAddr := range n.Status.Addresses {
			if nodeAddr.Type == apiv1.NodeInternalIP {
				addr = nodeAddr.Address
				break
			}
		}

		if addr == "" {
			log.Warnf("node: %s must have an ip address", n.Name)
		} else {
			schedulerNodes = append(schedulerNodes, addr)
		}
	}

	// all hostPaths
	hostPaths, err := ss.volumePool.HostPathsFromNodes(schedulerNodes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// range all hostPaths, and try to find the suitable node
	for _, hostPath := range hostPaths {
		pvName := hostPath.PVName()
		pvPath := hostPath.Path
		nodeIP := hostPath.NodeName
		pvSize := hostPath.Size

		pvQuantity, err := resource.ParseQuantity(pvSize)
		if err != nil {
			log.Warningf("try to parse PV size: %s failed: %v", pvSize, err)
			continue
		}

		if pvQuantity.Cmp(pvcQuantity) == -1 {
			log.Warningf("pv: %s size is less than pvc: %s size", pvName, pvcName)
			continue
		}

		clusterName, ok := args.Pod.Labels[label.ClusterLabelKey]
		if !ok {
			return nil, errors.Errorf("pod: %s don't have label: %s", podName, label.ClusterLabelKey)
		}

		annotations := map[string]string{
			AnnotationProvisioner: ProvisionerName,
			AnnotationHostPath:    pvPath,
			AnnotationPVCName:     pvcName,
		}

		labels := label.New().Cluster(clusterName).Labels()
		labels[LabelNodeName] = nodeIP
		labels[LabelPodName] = podName
		labels[LabelPodNamespace] = podNamespace

		// get or create a PV for given hostPath
		var pv *v1.PersistentVolume
		if pv, err = ss.cli.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				log.Errorf("get PV: %s for pod: %s failed: %v", pvName, podName, err)
				return nil, errors.Trace(err)
			}

			newPV := &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:        pvName,
					Annotations: annotations,
					Labels:      labels,
				},
				Spec: v1.PersistentVolumeSpec{
					PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain,
					AccessModes:                   []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},

					Capacity: v1.ResourceList{v1.ResourceStorage: pvQuantity},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: pvPath,
						},
					},
					StorageClassName: tsapi.TiDBStorageProvisioner,
				},
			}
			if pv, err = ss.cli.CoreV1().PersistentVolumes().Create(newPV); err != nil {
				log.Errorf("create PV: %s for pod: %s failed: %v", pvName, podName, err)
				return nil, errors.Trace(err)
			}
		}

		labelPVCName, ok := pv.Annotations[AnnotationPVCName]
		if !ok {
			return nil, errors.Errorf("pv: %s should have a label: %s", pv.Name, AnnotationPVCName)
		}

		// this PV is not related to current pod
		if labelPVCName != pvcName {
			continue
		}

		// associate PVC with PV
		pvc.Spec.VolumeName = pv.Name
		_, err = ss.cli.CoreV1().PersistentVolumeClaims(podNamespace).Update(pvc)
		if err != nil {
			return nil, errors.Trace(err)
		}

		return findANode(args.Nodes.Items, hostPath)
	}

	return nil, errors.Errorf("there is no suitable node for pod: %s", podName)
}

func findANode(nodes []apiv1.Node, hostPath *volume.HostPath) (*apiv1.Node, error) {
	for _, n := range nodes {
		for _, nodeAddr := range n.Status.Addresses {
			if nodeAddr.Type == apiv1.NodeInternalIP && nodeAddr.Address == hostPath.NodeName {
				return &n, nil
			}
		}
	}

	return nil, errors.Errorf("node is not in kubernetes scheduler's nodes, but this should not happen")
}
