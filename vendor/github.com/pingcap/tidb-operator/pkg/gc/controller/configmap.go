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

package controller

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

func (gcc *GCController) syncCm(key string) error {
	startTime := time.Now()
	defer func() {
		log.Infof("Finished syncing ConfigMap: %q (%v)", key, time.Now().Sub(startTime))
	}()

	obj, exists, err := gcc.cmStore.GetByKey(key)
	if err != nil {
		return errors.Trace(err)
	}

	if !exists {
		return nil
	}

	cm, ok := obj.(*v1.ConfigMap)
	if !ok {
		return errors.Trace(fmt.Errorf("object %v is not a v1.ConfigMap", obj))
	}

	var ownerRef *metav1.OwnerReference

	for _, or := range cm.ObjectMeta.OwnerReferences {
		if or.Kind == tsapi.TidbClusterOwnerReference {
			ownerRef = &or
		}
	}

	if ownerRef == nil {
		return errors.Errorf("configMap: %s/%s don't have a OwnerReference, skipping", cm.Namespace, cm.Name)
	}

	tsName := ownerRef.Name

	_, err = gcc.cli.PingcapV1().TidbClusters(cm.Namespace).Get(tsName, metav1.GetOptions{})
	if err != nil && apierrs.IsNotFound(err) {
		err := gcc.cli.CoreV1().ConfigMaps(cm.Namespace).Delete(cm.Name, nil)
		if err == nil {
			log.Infof("configMap: %s/%s is successfully deleted.", cm.Namespace, cm.Name)
			return nil
		}

		return errors.Trace(err)
	}

	return nil
}
