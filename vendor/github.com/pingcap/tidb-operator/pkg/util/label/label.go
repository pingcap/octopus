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

package label

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ClusterLabelKey is cluster label key
	ClusterLabelKey string = "cluster.pingcap.com/tidbCluster"
	// AppLabelKey is app label key
	AppLabelKey string = "cluster.pingcap.com/app"
	// OwnerLabelKey is owner label key
	OwnerLabelKey string = "cluster.pingcap.com/owner"

	// PDLabelVal is PD label value
	PDLabelVal string = "pd"
	// TiDBLabelVal is TiDB label value
	TiDBLabelVal string = "tidb"
	// TiKVLabelVal is TiKV label value
	TiKVLabelVal string = "tikv"
	// ClusterLabelVal is cluster label value
	ClusterLabelVal string = "tidbCluster"
)

// Label is the label field in metadata
type Label map[string]string

// New initialize a new Label
func New() Label {
	return map[string]string{OwnerLabelKey: ClusterLabelVal}
}

// Cluster adds cluster kv pair to label
func (l Label) Cluster(name string) Label {
	l[ClusterLabelKey] = name
	return l
}

// App adds app kv pair to label
func (l Label) App(name string) Label {
	l[AppLabelKey] = name
	return l
}

// PD assigns pd to app key in label
func (l Label) PD() Label {
	l.App(PDLabelVal)
	return l
}

// TiDB assigns tidb to app key in label
func (l Label) TiDB() Label {
	l.App(TiDBLabelVal)
	return l
}

// TiKV assigns tikv to app key in label
func (l Label) TiKV() Label {
	l.App(TiKVLabelVal)
	return l
}

// Selector gets LabelSelector from label
func (l Label) Selector() *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: l}
}

// Labels converts label to map[string]string
func (l Label) Labels() map[string]string {
	return l
}
