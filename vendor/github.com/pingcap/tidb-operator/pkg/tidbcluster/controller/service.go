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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/pkg/api/v1"
)

func (tcc *TiDBClusterController) syncServiceFor(cluster *tcapi.TidbCluster) error {
	log.Infof("Syncing service")
	clusterName := cluster.Metadata.GetName()
	uid := cluster.Metadata.GetUID()
	ns := cluster.Metadata.GetNamespace()
	controller := true
	ownerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "TidbCluster",
		Name:       clusterName,
		UID:        uid,
		Controller: &controller,
	}
	pdLabel := label.New().Cluster(clusterName).PD().Labels()
	tidbLabel := label.New().Cluster(clusterName).TiDB().Labels()
	pdSvc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-pd", clusterName),
			Namespace:       ns,
			Labels:          pdLabel,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Annotations:     map[string]string{},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name:       "client",
					Port:       2379,
					TargetPort: intstr.FromInt(2379),
					Protocol:   v1.ProtocolTCP,
				},
			},
			Selector: pdLabel,
		},
	}
	tidbSvc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-tidb", clusterName),
			Namespace:       ns,
			Labels:          tidbLabel,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Annotations:     map[string]string{},
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceType(cluster.Spec.Service),
			Ports: []v1.ServicePort{
				{
					Name:       "mysql-client",
					Port:       4000,
					TargetPort: intstr.FromInt(4000),
					Protocol:   v1.ProtocolTCP,
				},
			},
			Selector: tidbLabel,
		},
	}
	if err := tcc.updateService(pdSvc); err != nil {
		log.Errorf("failed to update pd service: %v", err)
		return err
	}
	if err := tcc.updateService(tidbSvc); err != nil {
		log.Errorf("failed to update tidb service: %v", err)
		return err
	}
	return nil
}

func (tcc *TiDBClusterController) updateService(svc *v1.Service) error {
	name := svc.GetName()
	ns := svc.GetNamespace()
	s, err := tcc.cli.CoreV1().Services(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = tcc.cli.CoreV1().Services(ns).Create(svc)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("Creating Service: %s/%s", ns, name)
			return nil
		}
		return errors.Trace(err)
	}
	if s.Spec.Type == svc.Spec.Type {
		return nil
	}
	svc.ResourceVersion = s.ResourceVersion
	svc.Spec.ClusterIP = s.Spec.ClusterIP
	_, err = tcc.cli.CoreV1().Services(ns).Update(svc)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("Updating Service: %s/%s", s.Namespace, s.Name)
	return nil
}
