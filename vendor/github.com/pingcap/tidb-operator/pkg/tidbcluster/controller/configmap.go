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
	"github.com/pingcap/tidb-operator/pkg/util/label"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

func (tcc *TiDBClusterController) syncConfigMapFor(cluster *tcapi.TidbCluster) error {
	clusterName := cluster.Metadata.GetName()
	clusterUID := cluster.Metadata.GetUID()
	ns := cluster.Metadata.GetNamespace()
	name := fmt.Sprintf("%s-config", clusterName)
	if cluster.Spec.Config == nil {
		log.Warnf("cluster: %s/%s, no config provided, use default", ns, clusterName)
		return nil
	}
	controller := true
	ownerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "TidbCluster",
		Name:       clusterName,
		UID:        clusterUID,
		Controller: &controller,
	}
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			Labels:          label.New().Cluster(clusterName).App("config").Labels(),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Annotations:     map[string]string{},
		},
		Data: cluster.Spec.Config,
	}
	c, err := tcc.cli.CoreV1().ConfigMaps(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = tcc.cli.CoreV1().ConfigMaps(ns).Create(cm)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("Creating ConfigMap: %s/%s", ns, name)
			return nil
		}
		return errors.Trace(err)
	}
	if reflect.DeepEqual(c.Data, cm.Data) {
		return nil
	}
	cm.ResourceVersion = c.ResourceVersion
	_, err = tcc.cli.CoreV1().ConfigMaps(ns).Update(cm)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("Updating ConfigMap: %s/%s", ns, name)
	return nil
}
