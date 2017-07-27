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

package tidb

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/client"
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	"github.com/pingcap/tidb-operator/pkg/util"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

var keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc

// Controller is the controller of tidbset for TiDB
type Controller struct {
	cli        client.Interface
	controller cache.Controller
	store      cache.Store
	queue      *workqueue.Type
}

// NewController creates a new TiDB Controller
func NewController(cli client.Interface) *Controller {
	tc := &Controller{
		cli:   cli,
		queue: workqueue.New(),
	}
	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(
			label.New().TiDB().Labels(),
		).String(),
	}
	tc.store, tc.controller = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(_ metav1.ListOptions) (runtime.Object, error) {
				return tc.cli.PingcapV1().TidbSets(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(_ metav1.ListOptions) (watch.Interface, error) {
				return tc.cli.PingcapV1().TidbSets(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&tsapi.TidbSet{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    tc.enqueueTiDB,
			UpdateFunc: tc.updateTiDB,
			DeleteFunc: tc.enqueueTiDB,
		})
	return tc
}

// Run method runs TiDB Controller
func (tc *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer tc.queue.ShutDown()
	log.Infof("Starting TiDB controller")

	go tc.controller.Run(wait.NeverStop)
	go wait.Until(tc.worker, time.Second, stopCh)
	<-stopCh
	log.Infof("Shutting down TiDB controller")
}

func (tc *Controller) enqueueTiDB(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}
	tc.queue.Add(key)
}

func (tc *Controller) worker() {
	for {
		func() {
			key, quit := tc.queue.Get()
			if quit {
				return
			}
			defer tc.queue.Done(key)
			if err := tc.syncTiDB(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (tc *Controller) syncTiDB(key string) error {
	startTime := time.Now()
	defer func() {
		log.Infof("Finished syncing Pd %q (%v)", key, time.Now().Sub(startTime))
	}()
	obj, exists, err := tc.store.GetByKey(key)
	if err != nil {
		log.Errorf("try to get obj: %s return error: %v", key, err)
		return err
	}
	if !exists {
		log.Infof("TidbSet has been deleted %v", key)
		ns, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			log.Error(err)
			return err
		}
		_, err = tc.cli.PingcapV1().TidbSets(ns).Get(name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) { // make sure tidbset is actually deleted
				opts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(
					label.New().Cluster(strings.TrimSuffix(name, "-tidbset")).TiDB().Labels(),
				).String()}
				return tc.deleteTiDBPods(ns, opts)
			}
			return err
		}
		return nil
	}
	ts, ok := obj.(*tsapi.TidbSet)
	if !ok {
		log.Errorf("obj: %v is not a *TidbSet", obj)
		return fmt.Errorf("object %+v is not a *TidbSet", obj)
	}
	defer tc.syncTiDBStatus(ts)
	if err := tc.syncTiDBPodFor(ts); err != nil {
		log.Errorf("failed to sync TiDB pod: %v", err)
		return err
	}
	return nil
}

func (tc *Controller) updateTiDB(old, cur interface{}) {
	tc.enqueueTiDB(cur)
}

func (tc *Controller) syncTiDBPodFor(tidbset *tsapi.TidbSet) error {
	clusterName := tidbset.Metadata.OwnerReferences[0].Name
	ns := tidbset.Metadata.Namespace
	// Check if TiKV is started, TiDB can only be started after TiKV is started
	opts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(label.New().Cluster(clusterName).TiKV().Labels()).String()}
	pds, err := tc.cli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		log.Errorf("failed to list TiKV pods: %v", err)
		return err
	}
	var tikvStarted bool
	for _, pod := range pds.Items {
		if pod.Status.Phase == v1.PodRunning {
			tikvStarted = true
			break
		}
	}
	if !tikvStarted {
		log.Infof("waiting for TiKV to start")
		return nil
	}

	uid := tidbset.Metadata.UID
	controller := true
	ownerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       tsapi.TidbSetOwnerReference,
		Name:       tidbset.Metadata.Name,
		UID:        uid,
		Controller: &controller,
	}

	obj, err := conversion.NewCloner().DeepCopy(tidbset.Spec.Template.Spec)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	podSpec := obj.(v1.PodSpec)
	opts = metav1.ListOptions{LabelSelector: labels.SelectorFromSet(tidbset.Spec.Selector.MatchLabels).String()}
	podList, err := tc.cli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		return errors.Trace(err)
	}
	var podCount int
	for _, pod := range podList.Items {
		if pod.Status.Reason == util.NodeUnreachablePodReason {
			// We should skip unreachable TiDB pod and this causes controller adding a new TiDB pod.
			// When node is reachable again, it will be deleted by kubelet
			continue
		}
		if pod.Status.Phase == v1.PodPending || pod.Status.Phase == v1.PodRunning {
			podCount++
		}
	}
	if podCount < int(tidbset.Spec.Replicas) {
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName:    fmt.Sprintf("%s-tidb-", clusterName),
				Namespace:       ns,
				Labels:          tidbset.Spec.Selector.MatchLabels,
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
			Spec: podSpec,
		}
		for i := 0; i < int(tidbset.Spec.Replicas)-podCount; i++ {
			if _, err := tc.cli.CoreV1().Pods(ns).Create(pod); err != nil {
				return err
			}
		}
		return err
	} else if podCount == int(tidbset.Spec.Replicas) {
		log.Infof("already satisfied")
		return nil
	} else {
		// here we select the first pod to delete
		podName := podList.Items[0].Name
		err = tc.deleteTiDBPod(ns, podName)
		log.Infof("need to delete TiDB pod")
	}
	return nil
}

func (tc *Controller) deleteTiDBPods(ns string, opts metav1.ListOptions) error {
	podList, err := tc.cli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		return err
	}
	for _, pod := range podList.Items {
		if err := tc.cli.CoreV1().Pods(ns).Delete(pod.Name, nil); err != nil {
			log.Error(err)
		}
	}
	return nil
}

func (tc *Controller) deleteTiDBPod(ns, name string) error {
	err := tc.cli.CoreV1().Pods(ns).Delete(name, nil)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

func (tc *Controller) syncTiDBStatus(tidbset *tsapi.TidbSet) {
	ns := tidbset.Metadata.Namespace
	name := tidbset.Metadata.Name
	opts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(tidbset.Spec.Selector.MatchLabels).String()}
	podList, err := tc.cli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		log.Errorf("list TiDB pod failed: %v", err)
		return
	}
	var replicas int32
	var readyReplicas int32

	latestTidbset, err := tc.cli.PingcapV1().TidbSets(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("get TidbSet failed: %v", err)
		return
	}
	if latestTidbset.Status.TiDBStatus == nil {
		latestTidbset.Status.TiDBStatus = make(map[string]tsapi.TiDBStatus)
	}
	pods := make(map[string]string)
	tidbStatus := make(map[string]tsapi.TiDBStatus)
	for _, pod := range podList.Items {
		if pod.Status.Reason == util.NodeUnreachablePodReason {
			continue
		}
		podIP := pod.Status.PodIP
		pods[pod.Name] = podIP
		replicas++
		if pod.Status.Phase == v1.PodRunning && pod.Status.Reason != util.NodeUnreachablePodReason {
			readyReplicas++
		}
		podName := pod.Name
		status := tsapi.TiDBStatus{
			IP: podIP,
		}
		tidbStatus[podName] = status
	}
	if latestTidbset.Status.Replicas != replicas || latestTidbset.Status.ReadyReplicas != readyReplicas || !reflect.DeepEqual(tidbStatus, latestTidbset.Status.TiDBStatus) {
		latestTidbset.Status.Replicas = replicas
		latestTidbset.Status.ReadyReplicas = readyReplicas
		latestTidbset.Status.TiDBStatus = tidbStatus
		_, err = tc.cli.PingcapV1().TidbSets(ns).Update(latestTidbset)
		if err != nil {
			log.Errorf("update TidbSet status failed: %v", err)
			return
		}
	}
}
