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

package tpr

import (
	"time"

	"github.com/juju/errors"
	tapi "github.com/pingcap/tidb-operator/pkg/api"
	"github.com/pingcap/tidb-operator/pkg/client"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

var (
	tidbClusterGK = tapi.GroupVersion.WithKind("tidb-cluster").GroupKind()
	tidbSetGK     = tapi.GroupVersion.WithKind("tidb-set").GroupKind()
)

// EnsureTPRExist ensures tidb-cluster and tidb-set TPR exist
func EnsureTPRExist(cli client.Interface) error {
	err := createTidbClusterTPR(cli)
	if err != nil {
		return errors.Trace(err)
	}

	err = createTidbSetTPR(cli)
	if err != nil {
		return errors.Trace(err)
	}

	return waitTPRReady(cli)
}

func createTidbClusterTPR(cli client.Interface) error {
	return createTPR(cli, (&tidbClusterGK).String(), tapi.GroupVersion.Version, "TiDB Cluster")
}

func createTidbSetTPR(cli client.Interface) error {
	return createTPR(cli, (&tidbSetGK).String(), tapi.GroupVersion.Version, "TiDB component")
}

func createTPR(cli client.Interface, name, version, description string) error {
	tpr := &v1beta1.ThirdPartyResource{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Versions: []v1beta1.APIVersion{
			{Name: version},
		},
		Description: description,
	}

	_, err := cli.ExtensionsV1beta1().ThirdPartyResources().Create(tpr)
	if err != nil && apierrors.IsAlreadyExists(err) {
		return nil
	}

	return errors.Trace(err)
}

func waitTPRReady(cli client.Interface) error {
	return wait.Poll(1*time.Second, 60*time.Second, func() (bool, error) {
		_, err := cli.PingcapV1().TidbClusters(metav1.NamespaceDefault).List(metav1.ListOptions{})
		if err != nil {
			return false, nil
		}

		_, err = cli.PingcapV1().TidbSets(metav1.NamespaceDefault).List(metav1.ListOptions{})
		if err != nil {
			return false, nil
		}

		return true, nil
	})
}
