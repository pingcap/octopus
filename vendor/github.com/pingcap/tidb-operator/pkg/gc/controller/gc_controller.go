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
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/client"
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// GCController is the controller of garbage collector controller
type GCController struct {
	cli client.Interface

	podController cache.Controller
	podStore      cache.Store
	podQueue      *workqueue.Type

	pvcController cache.Controller
	pvcStore      cache.Store
	pvcQueue      *workqueue.Type

	tsController cache.Controller
	tsStore      cache.Store
	tsQueue      *workqueue.Type

	svcController cache.Controller
	svcStore      cache.Store
	svcQueue      *workqueue.Type

	cmController cache.Controller
	cmStore      cache.Store
	cmQueue      *workqueue.Type
}

// NewGCController creates a new GCController
func NewGCController(cli client.Interface) *GCController {
	gcc := &GCController{
		cli:      cli,
		podQueue: workqueue.New(),
		pvcQueue: workqueue.New(),
		tsQueue:  workqueue.New(),
		svcQueue: workqueue.New(),
		cmQueue:  workqueue.New(),
	}

	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(
			label.New().Labels(),
		).String(),
	}

	gcc.podStore, gcc.podController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(_ metav1.ListOptions) (runtime.Object, error) {
				return gcc.cli.CoreV1().Pods(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(_ metav1.ListOptions) (watch.Interface, error) {
				return gcc.cli.CoreV1().Pods(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&v1.Pod{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc: gcc.enqueuePod,
			UpdateFunc: func(old, cur interface{}) {
				gcc.enqueuePod(cur)
			},
			DeleteFunc: gcc.enqueuePod,
		},
	)

	gcc.pvcStore, gcc.pvcController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(_ metav1.ListOptions) (runtime.Object, error) {
				return gcc.cli.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(_ metav1.ListOptions) (watch.Interface, error) {
				return gcc.cli.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&v1.PersistentVolumeClaim{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc: gcc.enqueuePVC,
			UpdateFunc: func(old, cur interface{}) {
				gcc.enqueuePVC(cur)
			},
			DeleteFunc: gcc.enqueuePVC,
		},
	)

	gcc.tsStore, gcc.tsController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(options metav1.ListOptions) (runtime.Object, error) {
				return gcc.cli.PingcapV1().TidbSets(metav1.NamespaceAll).List(options)
			}),
			WatchFunc: cache.WatchFunc(func(options metav1.ListOptions) (watch.Interface, error) {
				return gcc.cli.PingcapV1().TidbSets(metav1.NamespaceAll).Watch(options)
			}),
		},
		&tsapi.TidbSet{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc: gcc.enqueueTs,
			UpdateFunc: func(old, cur interface{}) {
				gcc.enqueueTs(cur)
			},
			DeleteFunc: gcc.enqueueTs,
		},
	)

	gcc.svcStore, gcc.svcController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(options metav1.ListOptions) (runtime.Object, error) {
				return gcc.cli.CoreV1().Services(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(options metav1.ListOptions) (watch.Interface, error) {
				return gcc.cli.CoreV1().Services(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&v1.Service{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc: gcc.enqueueSvc,
			UpdateFunc: func(old, cur interface{}) {
				gcc.enqueueSvc(cur)
			},
			DeleteFunc: gcc.enqueueSvc,
		},
	)

	gcc.cmStore, gcc.cmController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(options metav1.ListOptions) (runtime.Object, error) {
				return gcc.cli.CoreV1().ConfigMaps(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(options metav1.ListOptions) (watch.Interface, error) {
				return gcc.cli.CoreV1().ConfigMaps(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&v1.ConfigMap{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc: gcc.enqueueCm,
			UpdateFunc: func(old, cur interface{}) {
				gcc.enqueueCm(cur)
			},
			DeleteFunc: gcc.enqueueCm,
		},
	)

	return gcc
}

// Run method runs GC controller
func (gcc *GCController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	defer gcc.podQueue.ShutDown()
	log.Infof("Starting GC Pod controller")
	go gcc.podController.Run(stopCh)
	go wait.Until(gcc.podWorker, time.Second, stopCh)

	defer gcc.pvcQueue.ShutDown()
	log.Infof("Starting GC PVC controller")
	go gcc.pvcController.Run(stopCh)
	go wait.Until(gcc.pvcWorker, time.Second, stopCh)

	defer gcc.tsQueue.ShutDown()
	log.Infof("Starting GC TidbSet controller")
	go gcc.tsController.Run(stopCh)
	go wait.Until(gcc.tsWorker, time.Second, stopCh)

	defer gcc.svcQueue.ShutDown()
	log.Infof("Starting GC PD and Tidb service controller")
	go gcc.svcController.Run(stopCh)
	go wait.Until(gcc.svcWorker, time.Second, stopCh)

	defer gcc.cmQueue.ShutDown()
	log.Infof("Starting GC ConfigMap controller")
	go gcc.cmController.Run(stopCh)
	go wait.Until(gcc.cmWorker, time.Second, stopCh)

	<-stopCh
	log.Infof("Shutting down GCController")
}

func (gcc *GCController) podWorker() {
	for {
		func() {
			key, quit := gcc.podQueue.Get()
			if quit {
				return
			}
			defer gcc.podQueue.Done(key)

			if err := gcc.syncPod(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (gcc *GCController) enqueuePod(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}

	gcc.podQueue.Add(key)
}

func (gcc *GCController) pvcWorker() {
	for {
		func() {
			key, quit := gcc.pvcQueue.Get()
			if quit {
				return
			}
			defer gcc.pvcQueue.Done(key)

			if err := gcc.syncPVC(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (gcc *GCController) enqueuePVC(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}

	gcc.pvcQueue.Add(key)
}

func (gcc *GCController) tsWorker() {
	for {
		func() {
			key, quit := gcc.tsQueue.Get()
			if quit {
				return
			}
			defer gcc.tsQueue.Done(key)

			if err := gcc.syncTs(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (gcc *GCController) enqueueTs(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}

	gcc.tsQueue.Add(key)
}

func (gcc *GCController) svcWorker() {
	for {
		func() {
			key, quit := gcc.svcQueue.Get()
			if quit {
				return
			}
			defer gcc.svcQueue.Done(key)

			if err := gcc.syncSvc(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (gcc *GCController) enqueueSvc(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}

	gcc.svcQueue.Add(key)
}

func (gcc *GCController) cmWorker() {
	for {
		func() {
			key, quit := gcc.cmQueue.Get()
			if quit {
				return
			}
			defer gcc.cmQueue.Done(key)

			if err := gcc.syncCm(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (gcc *GCController) enqueueCm(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}

	gcc.cmQueue.Add(key)
}
