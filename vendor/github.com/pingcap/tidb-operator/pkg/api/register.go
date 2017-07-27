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

package api

import (
	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/pkg/api"
)

var (
	// GroupVersion is our TPR group and version
	GroupVersion = schema.GroupVersion{
		Group:   "pingcap.com",
		Version: "v1",
	}

	unversioned = schema.GroupVersion{Group: "", Version: "v1"}
)

func init() {
	schemeBuilder := runtime.NewSchemeBuilder(
		func(scheme *runtime.Scheme) error {
			scheme.AddKnownTypes(
				GroupVersion,
				&tcapi.TidbCluster{},
				&tcapi.TidbClusterList{},
				&tsapi.TidbSet{},
				&tsapi.TidbSetList{},
			)

			scheme.AddUnversionedTypes(
				unversioned,
				&metav1.ListOptions{},
				&metav1.GetOptions{},
				&metav1.DeleteOptions{},
			)

			return nil
		},
	)

	schemeBuilder.AddToScheme(api.Scheme)
}
