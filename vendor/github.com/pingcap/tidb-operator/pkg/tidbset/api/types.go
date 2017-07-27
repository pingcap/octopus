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

package api

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

const (
	// AnnotationPVCName is a pvcName annotation key
	AnnotationPVCName string = "volume.pingcap.com/pvcName"

	// AnnotationStorageSize is a storage size annotation key
	AnnotationStorageSize string = "storage.pingcap.com/size"

	// TiKVVolumeName is volume name for tikv volume
	TiKVVolumeName string = "tikv-volume-hostpath"

	// AnnotationStorageProvisioner is storage provisioner annotation key
	AnnotationStorageProvisioner string = "volume.beta.kubernetes.io/storage-provisioner"

	// TiDBStorageProvisioner is tidb's storage provisioner name
	TiDBStorageProvisioner string = "tidb-volume-provisioner"

	// TidbSetOwnerReference is the TidbSet kind of OwnerReference
	TidbSetOwnerReference string = "TidbSet"

	// TidbClusterOwnerReference is the TidbCluster kind of OwnerReference
	TidbClusterOwnerReference string = "TidbCluster"
)

// TidbSet represents the configuration of a single TidbSet.
// A TidbSet may be a PD/TiDB/TiKV instance.
type TidbSet struct {
	metav1.TypeMeta `json:",inline"`
	Metadata        metav1.ObjectMeta `json:"metadata"`

	// Spec is a structure defining the expected behavior of a TidbSet.
	Spec TidbSetSpec `json:"spec,omitempty"`

	// Status is a structure describing current status of a TidbSet.
	Status TidbSetStatus `json:"status,omitempty"`
}

// TidbSetList is a collection of TidbSets.
type TidbSetList struct {
	metav1.TypeMeta `json:",inline"`
	Metadata        metav1.ListMeta `json:"metadata"`

	Items []TidbSet `json:"items"`
}

// TidbSetSpec describes how the TidbSet will look like.
type TidbSetSpec struct {
	// Replicas represents the desired TidbSets count
	Replicas int32 `json:"replicas,omitempty"`

	// Selector is a label query over pods
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// Template is the object that describes the pod that will be created
	Template *v1.PodTemplateSpec `json:"template,omitempty"`
}

// TidbSetStatus represents the current state of a TidbSet.
type TidbSetStatus struct {
	// Replicas is the number of actual replicas. unreachable pods are not included
	Replicas int32 `json:"replicas"`

	// The number of running replicas.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	PDStatus   map[string]PDStatus   `json:"pdStatus,omitempty"`
	TiKVStatus map[string]TiKVStatus `json:"tikvStatus,omitempty"`
	TiDBStatus map[string]TiDBStatus `json:"tidbStatus,omitempty"`
}

// PDStatus is PD status
type PDStatus struct {
	Name string `json:"name"`
	// member id is actually a uint64, but apimachinery's json only treats numbers as int64/float64
	// so uint64 may overflow int64 and thus convert to float64
	ID string `json:"id"`
	IP string `json:"ip"`
}

// TiDBStatus is TiDB status
type TiDBStatus struct {
	IP string `json:"ip"`
}

// TiKVStatus is either Up/Down/Offline, namely it's in-cluster status
// when status changed from Offline to Tombstone, we delete it
type TiKVStatus struct {
	// store id is also uint64, due to the same reason as pd id, we store id as string
	ID                string    `json:"id"`
	IP                string    `json:"ip"`
	State             string    `json:"state"`
	LastHeartbeatTime time.Time `json:"lastHeartbeatTime"`
}
