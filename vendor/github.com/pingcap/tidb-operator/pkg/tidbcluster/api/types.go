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
)

// TidbCluster is the controll script's spec
type TidbCluster struct {
	metav1.TypeMeta `json:",inline"`
	Metadata        metav1.ObjectMeta `json:"metadata"`

	// Spec defines the behavior of a tidb cluster
	Spec ClusterSpec `json:"spec"`

	// Most recently observed status of the tidb cluster
	Status ClusterStatus `json:"status"`
}

// TidbClusterList is TidbCluster list
type TidbClusterList struct {
	metav1.TypeMeta `json:",inline"`
	Metadata        metav1.ListMeta `json:"metadata"`

	Items []TidbCluster `json:"items"`
}

// ClusterSpec describes the attributes that a user creates on a tidb cluster
type ClusterSpec struct {
	PD   PDSpec   `json:"pd,omitempty"`
	TiDB TiDBSpec `json:"tidb,omitempty"`
	TiKV TiKVSpec `json:"tikv,omitempty"`

	// Service type for tidb: ClusterIP | NodePort | LoadBalancer, default: ClusterIP
	Service string            `json:"service,omitempty"`
	Config  map[string]string `json:"config,omitempty"`
	Paused  bool              `json:"paused,omitempty"`
}

// ClusterStatus represents the current status of a tidb cluster.
type ClusterStatus struct {
	// Phase is the cluster running phase
	Phase ClusterPhase `json:"phase"`
	// Events keep ten most recent cluster change
	Events []ClusterEvent `json:"events"`
	// Message contains some annotation messages
	Message string `json:"message"`
}

// PDSpec contains details of PD member
type PDSpec struct {
	MemberSpec
}

// TiDBSpec contains details of PD member
type TiDBSpec struct {
	MemberSpec
}

// TiKVSpec contains details of PD member
type TiKVSpec struct {
	MemberSpec
}

// MemberSpec contains common attributes of PD/TiDB/TiKV
type MemberSpec struct {
	Size         int                  `json:"size"`
	Image        string               `json:"image"`
	NodeSelector map[string]string    `json:"nodeSelector,omitempty"`
	Requests     *ResourceRequirement `json:"requests,omitempty"`
	Limits       *ResourceRequirement `json:"limits,omitempty"`
}

// ResourceRequirement is resource requirements for a pod
type ResourceRequirement struct {
	// CPU is how many cores a pod requires
	CPU string `json:"cpu,omitempty"`
	// Memory is how much memory a pod requires
	Memory string `json:"memory,omitempty"`
	// Storage is storage size a pod requires
	Storage string `json:"storage,omitempty"`
}

// ClusterPhase is the type for phase of cluster state
type ClusterPhase string

const (
	// ClusterPhaseNone represents new cluster, nothing to do with it
	ClusterPhaseNone ClusterPhase = ""
	// ClusterPhaseCreating represents clutser is in creating phase
	ClusterPhaseCreating = "Creating"
	// ClusterPhaseAvailable represents cluster can provides service, but some members may not ready
	ClusterPhaseAvailable = "Available"
	// ClusterPhaseReady represents all cluster members is ready
	ClusterPhaseReady = "Ready"
	// ClusterPhaseFailed represents cluster is failed
	ClusterPhaseFailed = "Failed"
)

// ClusterEventType is the type for cluster event
type ClusterEventType string

// ClusterEvent represents the detail event infomation
type ClusterEvent struct {
	Type    ClusterEventType `json:"type"`
	Message string           `json:"message"`
	Time    time.Time        `json:"time"`
}

const (
	// ClusterEventReady represents the cluster is all ok
	ClusterEventReady = "Ready"
	// ClusterEventRemovingDeadMember represents the event that remove dead member
	ClusterEventRemovingDeadMember = "RemovingDeadMember"
	// ClusterEventScalingUp represents the event that scale up
	ClusterEventScalingUp = "ScalingUp"
	// ClusterEventScalingDown represents the event that scale down
	ClusterEventScalingDown = "ScalingDown"
	// ClusterEventUpgrading represents the event that upgrade component
	ClusterEventUpgrading = "Upgrading"
)
