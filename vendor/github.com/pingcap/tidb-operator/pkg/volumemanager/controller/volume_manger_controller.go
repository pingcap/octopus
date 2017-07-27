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

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/scheduler"
	"github.com/pingcap/tidb-operator/pkg/volumemanager"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

var keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc

// VolumeManagerController is a controller for managing tidb's hostPath PV.
type VolumeManagerController struct {
	cli        kubernetes.Interface
	controller cache.Controller
	store      cache.Store
	recover    volumemanager.VolumeRecover
	queue      *workqueue.Type
	hostIP     string
}

// NewVolumeManagerController returns a VolumeManagerController
func NewVolumeManagerController(hostIP string, cli kubernetes.Interface) *VolumeManagerController {
	vmc := &VolumeManagerController{
		cli:     cli,
		recover: volumemanager.NewVolumeRecover(),
		queue:   workqueue.New(),
		hostIP:  hostIP,
	}
	opts := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", scheduler.LabelNodeName, vmc.hostIP)}

	vmc.store, vmc.controller = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(options metav1.ListOptions) (runtime.Object, error) {
				return vmc.cli.CoreV1().PersistentVolumes().List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(options metav1.ListOptions) (watch.Interface, error) {
				return vmc.cli.CoreV1().PersistentVolumes().Watch(opts)
			}),
		},
		&v1.PersistentVolume{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc: vmc.enqueuePV,
			UpdateFunc: func(old, cur interface{}) {
				vmc.enqueuePV(cur)
			},
			DeleteFunc: vmc.enqueuePV,
		},
	)

	return vmc
}

// Run start the controller.
func (vmc *VolumeManagerController) Run(stopCh <-chan struct{}) {
	go vmc.controller.Run(stopCh)
	go wait.Until(vmc.worker, time.Second, stopCh)

	<-stopCh
	vmc.queue.ShutDown()
}

func (vmc *VolumeManagerController) enqueuePV(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}

	vmc.queue.Add(key)
}

func (vmc *VolumeManagerController) worker() {
	for {
		func() {
			key, quit := vmc.queue.Get()
			if quit {
				return
			}
			defer vmc.queue.Done(key)
			vmc.syncPV(key.(string))
		}()
	}
}

func (vmc *VolumeManagerController) syncPV(key string) {
	obj, exists, err := vmc.store.GetByKey(key)
	if err != nil {
		log.Errorf("try to get obj: %s return error: %v", key, err)
		return
	}
	if !exists {
		log.Warningf("obj: %s is not exist in local cache", key)
		return
	}

	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		log.Errorf("obj: %v is not a *v1.PersistentVolume", obj)
		return
	}

	if provisioner, ok := pv.Annotations[scheduler.AnnotationProvisioner]; ok && provisioner == scheduler.ProvisionerName {
		if hostIP, ok := pv.Labels[scheduler.LabelNodeName]; ok && hostIP == vmc.hostIP {
			pvName := pv.Name
			podName, ok := pv.Labels[scheduler.LabelPodName]
			if !ok {
				log.Errorf("PV: %s should have label: %s", pvName, scheduler.LabelPodName)
				return
			}
			podNamespace, ok := pv.Labels[scheduler.LabelPodNamespace]
			if !ok {
				log.Errorf("PV: %s should have label: %s", pvName, scheduler.LabelPodNamespace)
				return
			}
			pvcName, ok := pv.Annotations[scheduler.AnnotationPVCName]
			if !ok {
				log.Errorf("PV: %s should have label: %s", pvName, scheduler.AnnotationPVCName)
				return
			}

			if pv.Status.Phase == v1.VolumeReleased {
				log.Infof("PV: %s released by pod: %s/%s, recovering", pvName, podNamespace, podName)
				startTime := time.Now()

				err := vmc.recover.Recover(pv)
				if err != nil {
					log.Errorf("PV: %s, recover failed: %v", pvName, err)
					return
				}
				err = vmc.cli.CoreV1().PersistentVolumes().Delete(pvName, metav1.NewDeleteOptions(0))
				if err != nil {
					log.Errorf("PV: %s, delete failed: %v", pvName, err)
					return
				}
				log.Infof("PV: %s recovered succeed, took: %v", pvName, time.Now().Sub(startTime))

				return
			}

			// pod may be deleted by other components or operators, and PVC become orphan
			_, err := vmc.cli.CoreV1().Pods(podNamespace).Get(podName, metav1.GetOptions{})
			if err != nil && apierrors.IsNotFound(err) {
				err := vmc.cli.CoreV1().PersistentVolumeClaims(podNamespace).Delete(pvcName, metav1.NewDeleteOptions(0))
				if err != nil && !apierrors.IsNotFound(err) {
					log.Errorf("try to delete PVC %s/%s failed: %v", podNamespace, pvcName, err)
				}
			}
		}
	}
}
