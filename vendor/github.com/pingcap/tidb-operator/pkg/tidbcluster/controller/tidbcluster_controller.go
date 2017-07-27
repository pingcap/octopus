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
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/client"
	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

var keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc

// TiDBClusterController is a controller for managing TidbCluster.
// It manages tidb/pd service, tidbcluster config and tidbcluster itself
type TiDBClusterController struct {
	cli        client.Interface
	controller cache.Controller
	store      cache.Store
	queue      *workqueue.Type
}

// NewTiDBClusterController returns a *TiDBClusterController
func NewTiDBClusterController(cli client.Interface) *TiDBClusterController {
	tcc := &TiDBClusterController{
		cli:   cli,
		queue: workqueue.New(),
	}
	tcc.store, tcc.controller = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(opts metav1.ListOptions) (runtime.Object, error) {
				return tcc.cli.PingcapV1().TidbClusters(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(opts metav1.ListOptions) (watch.Interface, error) {
				return tcc.cli.PingcapV1().TidbClusters(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&tcapi.TidbCluster{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    tcc.enqueueTidbCluster,
			UpdateFunc: tcc.updateTidbCluster,
			DeleteFunc: tcc.enqueueTidbCluster,
		})
	return tcc
}

// Run start the controller.
func (tcc *TiDBClusterController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer tcc.queue.ShutDown()
	log.Infof("Starting TidbCluster controller")
	go tcc.controller.Run(wait.NeverStop)
	go wait.Until(tcc.worker, time.Second, stopCh)
	<-stopCh
	log.Infof("Shutting down TidbCluster controller")
}

func (tcc *TiDBClusterController) enqueueTidbCluster(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}

	tcc.queue.Add(key)
}

func (tcc *TiDBClusterController) worker() {
	for {
		func() {
			key, quit := tcc.queue.Get()
			if quit {
				return
			}
			defer tcc.queue.Done(key)
			if err := tcc.syncTidbCluster(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (tcc *TiDBClusterController) syncTidbCluster(key string) error {
	startTime := time.Now()
	defer func() {
		log.Infof("Finished syncing tidb cluster %q (%v)", key, time.Now().Sub(startTime))
	}()

	obj, exists, err := tcc.store.GetByKey(key)
	if err != nil {
		log.Errorf("try to get obj: %s return error: %v", key, err)
		return err
	}
	if !exists {
		log.Infof("TidbCluster has been deleted %v", key)
		ns, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			log.Error(err)
			return err
		}
		_, err = tcc.cli.PingcapV1().TidbClusters(ns).Get(name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) { // make sure tidbcluster is actually deleted
				return tcc.deleteTidbClusterResource(ns, name)
			}
			return err
		}
		return nil
	}

	tc, ok := obj.(*tcapi.TidbCluster)
	if !ok {
		log.Errorf("obj: %v is not a *TidbCluster", obj)
		return fmt.Errorf("object %+v is not a *TidbCluster", obj)
	}
	if err := tcc.syncConfigMapFor(tc); err != nil {
		log.Errorf("failed to sync configmap for %s: %v", tc.Metadata.Name, err)
	}
	if err := tcc.syncServiceFor(tc); err != nil {
		log.Errorf("failed to sync service for %s: %v", tc.Metadata.Name, err)
	}
	if err := tcc.syncTidbSetFor(tc); err != nil {
		log.Errorf("failed to sync tidbset for %s: %v", tc.Metadata.Name, err)
	}
	return nil
}

// TODO: determine whether it's necessary to update tidbcluster
func (tcc *TiDBClusterController) updateTidbCluster(old, cur interface{}) {
	tcc.enqueueTidbCluster(cur)
}

func (tcc *TiDBClusterController) deleteTidbClusterResource(ns, name string) error {
	pdSetName := fmt.Sprintf("%s-pdset", name)
	tikvSetName := fmt.Sprintf("%s-tikvset", name)
	tidbSetName := fmt.Sprintf("%s-tidbset", name)
	configName := fmt.Sprintf("%s-config", name)
	pdSvcName := fmt.Sprintf("%s-pd", name)
	tidbSvcName := fmt.Sprintf("%s-tidb", name)
	if err := tcc.cli.PingcapV1().TidbSets(ns).Delete(tidbSetName, nil); err != nil {
		log.Error(err)
	}
	if err := tcc.cli.PingcapV1().TidbSets(ns).Delete(tikvSetName, nil); err != nil {
		log.Error(err)
	}
	if err := tcc.cli.PingcapV1().TidbSets(ns).Delete(pdSetName, nil); err != nil {
		log.Error(err)
	}
	if err := tcc.cli.CoreV1().ConfigMaps(ns).Delete(configName, nil); err != nil {
		log.Error(err)
	}
	if err := tcc.cli.CoreV1().Services(ns).Delete(pdSvcName, nil); err != nil {
		log.Error(err)
	}
	if err := tcc.cli.CoreV1().Services(ns).Delete(tidbSvcName, nil); err != nil {
		log.Error(err)
	}
	return nil
}
