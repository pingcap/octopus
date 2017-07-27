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
	"github.com/pingcap/tidb-operator/pkg/scheduler"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

func (gcc *GCController) syncPVC(key string) error {
	startTime := time.Now()
	defer func() {
		log.Infof("Finished syncing PVC: %q (%v)", key, time.Now().Sub(startTime))
	}()

	obj, exists, err := gcc.pvcStore.GetByKey(key)
	if err != nil {
		return errors.Trace(err)
	}

	if !exists {
		return nil
	}

	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		return errors.Trace(fmt.Errorf("object %v is not a *v1.PersistentVolumeClaim", obj))
	}

	podName, ok := pvc.Annotations[scheduler.LabelPodName]
	if !ok {
		return nil
	}

	_, err = gcc.cli.CoreV1().Pods(pvc.Namespace).Get(podName, metav1.GetOptions{})
	if err != nil && apierrs.IsNotFound(err) {
		err := gcc.cli.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(pvc.Name, nil)
		if err == nil {
			log.Infof("PVC: %s/%s is successfully deleted.", pvc.Namespace, pvc.Name)
			return nil
		}

		return errors.Trace(err)
	}

	return nil
}
