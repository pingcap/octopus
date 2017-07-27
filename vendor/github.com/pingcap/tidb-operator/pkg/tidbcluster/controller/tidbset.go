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

package controller

import (
	"fmt"
	"reflect"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	"github.com/pingcap/tidb-operator/pkg/util"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

func (tcc *TiDBClusterController) syncTidbSetFor(cluster *tcapi.TidbCluster) error {
	clusterName := cluster.Metadata.GetName()
	clusterUID := cluster.Metadata.GetUID()
	ns := cluster.Metadata.GetNamespace()

	controller := true
	ownerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "TidbCluster",
		Name:       clusterName,
		UID:        clusterUID,
		Controller: &controller,
	}
	pdset := &tsapi.TidbSet{
		Metadata: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-pdset", clusterName),
			Namespace:       ns,
			Labels:          label.New().Cluster(clusterName).App("pd").Labels(),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Annotations:     map[string]string{},
		},
		Spec: tsapi.TidbSetSpec{
			Replicas: int32(cluster.Spec.PD.Size),
			Selector: label.New().Cluster(clusterName).App("pd").Selector(),
			Template: &v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Affinity: util.AffinityForNodeSelector(
						ns,
						label.New().Cluster(clusterName).App("pd"),
						cluster.Spec.PD.NodeSelector,
					),
					Containers: []v1.Container{
						{
							Name:  "pd",
							Image: cluster.Spec.PD.Image,
							Command: []string{
								"/pd-server",
								"--data-dir=/var/lib/tidb/pd",
								"--name=$(MY_POD_NAME)",
								"--config=/etc/tidb/pd.toml",
								"--advertise-peer-urls=http://$(MY_POD_IP):2380",
								"--peer-urls=http://$(MY_POD_IP):2380",
								"--client-urls=http://$(MY_POD_IP):2379",
								"--advertise-client-urls=http://$(MY_POD_IP):2379",
							},
							Ports: []v1.ContainerPort{
								{
									Name:          "server",
									ContainerPort: int32(2380),
									Protocol:      v1.ProtocolTCP,
								},
								{
									Name:          "client",
									ContainerPort: int32(2379),
									Protocol:      v1.ProtocolTCP,
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{Name: "data", MountPath: "/var/lib/tidb"},
								{Name: "timezone", ReadOnly: true, MountPath: "/etc/localtime"},
								{Name: "config", ReadOnly: true, MountPath: "/etc/tidb"},
							},
							Resources: util.ResourceRequirement(cluster.Spec.PD.MemberSpec),
							Env: []v1.EnvVar{
								{
									Name: "MY_POD_NAME",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name: "MY_POD_IP",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyAlways,
					Volumes: []v1.Volume{
						{Name: "data", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
						{Name: "timezone", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/etc/localtime"}}},
						{Name: "config", VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: fmt.Sprintf("%s-config", clusterName)},
								Items: []v1.KeyToPath{
									{Key: "pd-config", Path: "pd.toml"},
								},
							},
						}},
					},
				},
			},
		},
	}
	tikvset := &tsapi.TidbSet{
		Metadata: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-tikvset", clusterName),
			Namespace:       ns,
			Labels:          label.New().Cluster(clusterName).App("tikv"),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Annotations:     map[string]string{},
		},
		Spec: tsapi.TidbSetSpec{
			Replicas: int32(cluster.Spec.TiKV.Size),
			Selector: label.New().Cluster(clusterName).App("tikv").Selector(),
			Template: &v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Affinity: util.AffinityForNodeSelector(
						ns,
						label.New().Cluster(clusterName).App("tikv"),
						cluster.Spec.TiKV.NodeSelector,
					),
					Containers: []v1.Container{
						{
							Name:  "tikv",
							Image: cluster.Spec.TiKV.Image,
							Command: []string{
								"/tikv-server",
								fmt.Sprintf("--pd=%s-pd:2379", clusterName),
								"--addr=0.0.0.0:20160",
								"--advertise-addr=$(MY_POD_IP):20160",
								"--store=/var/lib/tidb/tikv",
								"--config=/etc/tidb/tikv.toml",
								tikvCapacityArgs(cluster.Spec.TiKV.Limits),
							},
							Ports: []v1.ContainerPort{
								{
									Name:          "server",
									ContainerPort: int32(20160),
									Protocol:      v1.ProtocolTCP,
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{Name: "data", MountPath: "/var/lib/tidb"},
								{Name: "timezone", ReadOnly: true, MountPath: "/etc/localtime"},
								{Name: "config", ReadOnly: true, MountPath: "/etc/tidb"},
							},
							Resources: util.ResourceRequirement(cluster.Spec.TiKV.MemberSpec),
							Env: []v1.EnvVar{
								{
									Name: "MY_POD_IP",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyAlways,
					Volumes: []v1.Volume{
						{Name: "timezone", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/etc/localtime"}}},
						{Name: "data", VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: "PVC-NAME"}}},
						{Name: "config", VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: fmt.Sprintf("%s-config", clusterName)},
								Items:                []v1.KeyToPath{{Key: "tikv-config", Path: "tikv.toml"}},
							},
						}},
					},
				},
			},
		},
	}
	tidbset := &tsapi.TidbSet{
		Metadata: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-tidbset", clusterName),
			Namespace:       ns,
			Labels:          label.New().Cluster(clusterName).App("tidb"),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Annotations:     map[string]string{},
		},
		Spec: tsapi.TidbSetSpec{
			Replicas: int32(cluster.Spec.TiDB.Size),
			Selector: label.New().Cluster(clusterName).App("tidb").Selector(),
			Template: &v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Affinity: util.AffinityForNodeSelector(
						ns,
						nil,
						cluster.Spec.TiDB.NodeSelector,
					),
					Containers: []v1.Container{
						{
							Name:  "tidb",
							Image: cluster.Spec.TiDB.Image,
							Command: []string{
								"/tidb-server",
								"--store=tikv",
								fmt.Sprintf("--path=%s-pd:2379", clusterName),
								"--lease=$(TIDB_LEASE)",
								"--metrics-addr=$(TIDB_METRICS_ADDR)",
							},
							Ports: []v1.ContainerPort{
								{
									Name:          "server",
									ContainerPort: int32(4000),
									Protocol:      v1.ProtocolTCP,
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{Name: "timezone", ReadOnly: true, MountPath: "/etc/localtime"},
							},
							Resources: util.ResourceRequirement(cluster.Spec.TiDB.MemberSpec),
							Env: []v1.EnvVar{
								{
									Name: "MY_POD_IP",
									ValueFrom: &v1.EnvVarSource{
										FieldRef: &v1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
								{
									Name: "TIDB_LEASE",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: fmt.Sprintf("%s-config", clusterName)},
											Key:                  "tidb.lease",
										},
									},
								},
								{
									Name: "TIDB_METRICS_ADDR",
									ValueFrom: &v1.EnvVarSource{
										ConfigMapKeyRef: &v1.ConfigMapKeySelector{
											LocalObjectReference: v1.LocalObjectReference{Name: fmt.Sprintf("%s-config", clusterName)},
											Key:                  "tidb.metrics-addr",
										},
									},
								},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyAlways,
					Volumes: []v1.Volume{
						{Name: "timezone", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/etc/localtime"}}},
					},
				},
			},
		},
	}
	if err := tcc.updateTidbSet(pdset); err != nil {
		log.Errorf("failed to update tidbset pd: %v", err)
		return err
	}
	if err := tcc.updateTidbSet(tikvset); err != nil {
		log.Errorf("failed to update tidbset tikv: %v", err)
		return err
	}
	if err := tcc.updateTidbSet(tidbset); err != nil {
		log.Errorf("failed to update tidbset tidb: %v", err)
		return err
	}
	return nil
}

func (tcc *TiDBClusterController) updateTidbSet(tidbset *tsapi.TidbSet) error {
	name := tidbset.GetObjectMeta().GetName()
	ns := tidbset.GetObjectMeta().GetNamespace()
	ts, err := tcc.cli.PingcapV1().TidbSets(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = tcc.cli.PingcapV1().TidbSets(ns).Create(tidbset)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("Creating TidbSet: %s/%s", ns, name)
			return nil
		}
		return errors.Trace(err)
	}
	if reflect.DeepEqual(ts.Spec, tidbset.Spec) {
		return nil
	}
	ts.Spec = tidbset.Spec
	// tidbset.Metadata.ResourceVersion = ts.Metadata.ResourceVersion
	_, err = tcc.cli.PingcapV1().TidbSets(ns).Update(ts)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("Updating TidbSet: %s/%s", ns, name)
	return nil
}
