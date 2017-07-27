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

package tikv

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pborman/uuid"
	"github.com/pingcap/tidb-operator/pkg/client"
	"github.com/pingcap/tidb-operator/pkg/scheduler"
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	"github.com/pingcap/tidb-operator/pkg/util"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	"github.com/pingcap/tidb-operator/pkg/util/pdutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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

const (
	storeUpState        = "Up"
	storeOfflineState   = "Offline"
	storeDownState      = "Down"
	storeTombstoneState = "Tombstone"
)

// defaultTiKVRequestSize is tikv default storage size
var defaultTiKVRequestSize = resource.MustParse("50Gi")

const (
	// TiDBScheduler is the name of our extended scheduler
	TiDBScheduler string = "tidb-scheduler"
	timeout              = 2 * time.Second
)

// Controller is the controller of tidbset for TiKV
type Controller struct {
	cli        client.Interface
	controller cache.Controller
	store      cache.Store
	queue      *workqueue.Type
}

// NewController creates a new TiKV Controller
func NewController(cli client.Interface) *Controller {
	tkc := &Controller{
		cli:   cli,
		queue: workqueue.New(),
	}
	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(
			label.New().TiKV().Labels(),
		).String(),
	}

	tkc.store, tkc.controller = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(_ metav1.ListOptions) (runtime.Object, error) {
				return tkc.cli.PingcapV1().TidbSets(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(_ metav1.ListOptions) (watch.Interface, error) {
				return tkc.cli.PingcapV1().TidbSets(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&tsapi.TidbSet{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc: tkc.enqueueTiKV,
			UpdateFunc: func(old, cur interface{}) {
				tkc.enqueueTiKV(cur)
			},
			DeleteFunc: tkc.enqueueTiKV,
		},
	)

	return tkc
}

// Run method runs TiKV Controller
func (tkc *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer tkc.queue.ShutDown()
	log.Infof("Starting TiKV controller")

	go tkc.controller.Run(stopCh)
	go wait.Until(tkc.worker, time.Second, stopCh)

	<-stopCh
	log.Infof("Shutting down TiKV controller")
}

func (tkc *Controller) worker() {
	for {
		func() {
			key, quit := tkc.queue.Get()
			if quit {
				return
			}
			defer tkc.queue.Done(key)

			if err := tkc.syncTiKVSet(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (tkc *Controller) enqueueTiKV(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}

	tkc.queue.Add(key)
}

func (tkc *Controller) syncTiKVSet(key string) error {
	startTime := time.Now()
	defer func() {
		log.Infof("Finished syncing TiKVSet: %q (%v)", key, time.Now().Sub(startTime))
	}()

	obj, exists, err := tkc.store.GetByKey(key)
	if err != nil {
		return errors.Trace(err)
	}

	if !exists {
		return tkc.deleteTiKVSet(key)
	}

	ts, ok := obj.(*tsapi.TidbSet)
	if !ok {
		return errors.Trace(fmt.Errorf("object %v is not a *TidbSet", obj))
	}

	defer func() {
		tkc.syncTiKVStatus(ts)
	}()

	return tkc.syncTiKVPodFor(ts)
}

func (tkc *Controller) syncTiKVReplicasStatus(tidbset *tsapi.TidbSet) error {
	ns := tidbset.Metadata.Namespace
	return wait.Poll(5*time.Second, 30*time.Second, func() (bool, error) {
		podList, err := tkc.cli.CoreV1().Pods(ns).List(
			metav1.ListOptions{LabelSelector: labels.SelectorFromSet(tidbset.Spec.Selector.MatchLabels).String()},
		)
		if err != nil {
			log.Error(err)
			return false, nil
		}

		var replicas int32
		var readyReplicas int32

		for _, pod := range podList.Items {
			if pod.Status.Reason == util.NodeUnreachablePodReason {
				continue
			}
			replicas++
			if pod.Status.Phase == v1.PodRunning {
				readyReplicas++
			}
		}

		latestTidbset, err := tkc.cli.PingcapV1().TidbSets(ns).Get(tidbset.Metadata.Name, metav1.GetOptions{})
		if err != nil {
			log.Errorf("get tikv set failed: %v", err)
			return false, nil
		}

		if latestTidbset.Status.Replicas != replicas || latestTidbset.Status.ReadyReplicas != readyReplicas {
			latestTidbset.Status.Replicas = replicas
			latestTidbset.Status.ReadyReplicas = readyReplicas
			_, err := tkc.cli.PingcapV1().TidbSets(tidbset.Metadata.Namespace).Update(latestTidbset)
			if err != nil {
				log.Errorf("update tikv set status failed: %v", err)
				return false, nil
			}
		}

		return true, nil
	})
}

func (tkc *Controller) syncTiKVStatus(tidbset *tsapi.TidbSet) error {
	ns := tidbset.Metadata.Namespace
	return wait.Poll(5*time.Second, 30*time.Second, func() (bool, error) {
		podList, err := tkc.cli.CoreV1().Pods(ns).List(
			metav1.ListOptions{LabelSelector: labels.SelectorFromSet(tidbset.Spec.Selector.MatchLabels).String()},
		)
		if err != nil {
			log.Error(err)
			return false, nil
		}

		pods := make(map[string]v1.Pod)

		for _, pod := range podList.Items {
			podIP := pod.Status.PodIP
			pods[podIP] = pod
		}

		latestTidbset, err := tkc.cli.PingcapV1().TidbSets(ns).Get(tidbset.Metadata.Name, metav1.GetOptions{})
		if err != nil {
			log.Errorf("get tikv set failed: %v", err)
			return false, nil
		}

		if latestTidbset.Status.TiKVStatus == nil {
			latestTidbset.Status.TiKVStatus = make(map[string]tsapi.TiKVStatus)
		}

		clusterName := tidbset.Metadata.OwnerReferences[0].Name
		pdCli := pdutil.New(fmt.Sprintf("http://%s-pd.%s:2379", clusterName, ns), timeout)
		storesInfo, err := pdCli.GetStores() // This only returns Up/Down/Offline stores
		if err != nil {
			log.Error(err)
			return false, nil
		}
		tombstoneStoresInfo, err := pdCli.GetTombStoneStores()
		if err != nil {
			log.Error(err)
			return false, nil
		}
		tikvStatus := make(map[string]tsapi.TiKVStatus)

		if storesInfo != nil {
			stores := make(map[string]*pdutil.MetaStore)
			for _, store := range storesInfo.Stores {
				id := store.Store.GetId()
				storeID := fmt.Sprintf("%d", id)
				stores[storeID] = store.Store
				addr := store.Store.GetAddress()
				ip := strings.Split(addr, ":")[0]
				status := tsapi.TiKVStatus{
					ID:                storeID,
					IP:                ip,
					State:             store.Store.StateName,
					LastHeartbeatTime: store.Status.LastHeartbeatTS,
				}
				pod, exist := pods[ip]
				var podName string
				if exist {
					podName = pod.Name
				} else { // no pod is found for this store, happens when user deletes TiKV pod accidentally
					podName = fmt.Sprintf("tikv-%s", storeID)
				}
				if !exist || pod.Status.Reason == util.NodeUnreachablePodReason { // pod is deleted or NodeLost
					if err := pdCli.DeleteStore(id); err == nil {
						status.State = storeOfflineState
					} else {
						log.Errorf("failed to delete tikv(%d) from cluster: %v", id, err)
					}
				}
				tikvStatus[podName] = status
			}
			for _, store := range tombstoneStoresInfo.Stores {
				addr := store.Store.GetAddress()
				ip := strings.Split(addr, ":")[0]
				if pod, ok := pods[ip]; ok { // Tombstone stores has this IP
					if _, exist := tikvStatus[pod.Name]; exist { // This pod is in cluster
						continue
					}
					tikvStatus[pod.Name] = tsapi.TiKVStatus{
						ID:                fmt.Sprintf("%d", store.Store.GetId()),
						IP:                ip,
						State:             store.Store.StateName,
						LastHeartbeatTime: store.Status.LastHeartbeatTS,
					}
				}
			}
			for _, pod := range pods {
				if _, exist := tikvStatus[pod.Name]; !exist { // store hasn't been registered in PD
					tikvStatus[pod.Name] = tsapi.TiKVStatus{
						ID: "0",
						IP: pod.Status.PodIP,
					}
				}
			}
		}

		if !reflect.DeepEqual(tikvStatus, latestTidbset.Status.TiKVStatus) {
			latestTidbset.Status.TiKVStatus = tikvStatus
			_, err := tkc.cli.PingcapV1().TidbSets(tidbset.Metadata.Namespace).Update(latestTidbset)
			if err != nil {
				log.Errorf("update tikv set status failed: %v", err)
				return false, nil
			}
		}

		return true, nil
	})
}

func (tkc *Controller) deleteTiKVSet(key string) error {
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = tkc.cli.PingcapV1().TidbSets(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return tkc.deleteTiKVPods(ns, name)
		}

		return err
	}

	log.Warnf("TiKVSet: %s is existing, skipping...", key)
	return nil
}

func (tkc *Controller) createPVCForTiKVPod(tidbset *tsapi.TidbSet) (*v1.PersistentVolumeClaim, error) {
	pvcName := fmt.Sprintf("pvc-%s-%s", tidbset.Metadata.Name, uuid.New())
	storage := defaultTiKVRequestSize

	if tidbset.Metadata.Annotations != nil {
		storageFromAnnotation, ok := tidbset.Metadata.Annotations[tsapi.AnnotationStorageSize]
		if ok {
			q, err := resource.ParseQuantity(storageFromAnnotation)
			if err != nil {
				return nil, errors.Errorf(
					"can't parse storage from: [%s] when creating PVC: %s for TiKVSet: %s, %v",
					storageFromAnnotation,
					pvcName,
					tidbset.Metadata.Name,
					err,
				)
			}
			storage = q
		}
	}

	clusterName := tidbset.Metadata.OwnerReferences[0].Name
	storageClassName := tsapi.TiDBStorageProvisioner
	newPVC := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: tidbset.Metadata.Namespace,
			Labels:    label.New().Cluster(clusterName).Labels(),
			Annotations: map[string]string{
				tsapi.AnnotationStorageProvisioner: tsapi.TiDBStorageProvisioner,
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceStorage: storage},
			},
			StorageClassName: &storageClassName,
		},
	}

	return tkc.cli.CoreV1().PersistentVolumeClaims(tidbset.Metadata.Namespace).Create(newPVC)
}

func (tkc *Controller) createTiKVInstanceFor(tidbset *tsapi.TidbSet) error {
	pvc, err := tkc.createPVCForTiKVPod(tidbset)
	if err != nil {
		return errors.Trace(err)
	}

	clusterName := tidbset.Metadata.OwnerReferences[0].Name
	uid := tidbset.Metadata.UID
	ns := tidbset.Metadata.Namespace
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
		return errors.Trace(err)
	}

	podSpec := obj.(v1.PodSpec)

	for _, vol := range podSpec.Volumes {
		if vol.Name == "data" && vol.VolumeSource.PersistentVolumeClaim != nil {
			vol.VolumeSource.PersistentVolumeClaim.ClaimName = pvc.Name
		}
	}

	podSpec.SchedulerName = TiDBScheduler

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName:    fmt.Sprintf("%s-tikv-", clusterName),
			Namespace:       ns,
			Labels:          tidbset.Spec.Selector.MatchLabels,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Spec: podSpec,
	}
	podR, err := tkc.cli.CoreV1().Pods(ns).Create(pod)

	defer func() {
		wait.Poll(5*time.Second, 30*time.Second, func() (bool, error) {
			pvcP, err := tkc.cli.CoreV1().PersistentVolumeClaims(tidbset.Metadata.Namespace).Get(pvc.Name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}

			pvcP.Annotations[scheduler.LabelPodName] = podR.Name
			_, err = tkc.cli.CoreV1().PersistentVolumeClaims(tidbset.Metadata.Namespace).Update(pvcP)
			if err != nil {
				log.Errorf("################: %v", err)
				return false, nil
			}

			return true, nil
		})
	}()

	return err
}

func (tkc *Controller) syncTiKVPodFor(tidbset *tsapi.TidbSet) error {
	err := tkc.syncTiKVReplicasStatus(tidbset)
	if err != nil {
		return errors.Trace(err)
	}

	clusterName := tidbset.Metadata.OwnerReferences[0].Name
	ns := tidbset.Metadata.Namespace
	// Check if PD is started, TiKV can only be started after PD is started
	opts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(label.New().Cluster(clusterName).PD().Labels()).String()}
	pds, err := tkc.cli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		log.Errorf("failed to list pd pods: %v", err)
		return err
	}
	var pdStarted bool
	for _, pod := range pds.Items {
		if pod.Status.Phase == v1.PodRunning {
			pdStarted = true
			break
		}
	}
	if !pdStarted {
		log.Infof("waiting for pd to start")
		return nil
	}

	var upStores []tsapi.TiKVStatus
	var offlineStores []tsapi.TiKVStatus
	var downStores []tsapi.TiKVStatus
	for _, status := range tidbset.Status.TiKVStatus {
		switch status.State {
		case storeUpState:
			upStores = append(upStores, status)
		case storeDownState:
			downStores = append(downStores, status)
		case storeOfflineState:
			offlineStores = append(offlineStores, status)
		default:
			log.Warnf("unknown store state for TiKVStatus: %v", status)
		}
	}
	pdCli := pdutil.New(fmt.Sprintf("http://%s-pd.%s:2379", clusterName, ns), timeout)

	if tidbset.Status.Replicas == tidbset.Spec.Replicas {
		return nil
	}

	if tidbset.Status.Replicas > tidbset.Spec.Replicas {
		// we can only offline one tikv at a time
		if len(upStores) > int(tidbset.Spec.Replicas) && (offlineStores == nil && downStores == nil) {
			return tkc.deleteOneTiKV(pdCli)
		}
		return tkc.deleteTombstonePods(tidbset)
	}

	for i := 0; i < int(tidbset.Spec.Replicas-tidbset.Status.Replicas); i++ {
		err := tkc.createTiKVInstanceFor(tidbset)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return tkc.syncTiKVReplicasStatus(tidbset)
}

func (tkc *Controller) deleteOneTiKV(cli *pdutil.Client) error {
	storesInfo, err := cli.GetStores()
	if err != nil {
		return err
	}
	id := storesInfo.Stores[0].Store.GetId()
	regions := storesInfo.Stores[0].Status.RegionCount
	// find tikv with the smallest region count
	for _, store := range storesInfo.Stores {
		if store.Status.RegionCount < regions {
			id = store.Store.GetId()
			regions = store.Status.RegionCount
		}
	}
	return cli.DeleteStore(id)
}

func (tkc *Controller) deleteTombstonePods(tidbset *tsapi.TidbSet) error {
	ns := tidbset.Metadata.Namespace
	opts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(tidbset.Spec.Selector.MatchLabels).String()}
	podList, err := tkc.cli.CoreV1().Pods(ns).List(opts)
	for _, pod := range podList.Items {
		podName := pod.Name
		if status, found := tidbset.Status.TiKVStatus[podName]; found && status.State == storeTombstoneState {
			err = tkc.releasePVCFor(&pod)
			if err != nil {
				log.Errorf("failed to release PVC for pod %s: %v", podName, err)
				return err
			}
			err = tkc.cli.CoreV1().Pods(ns).Delete(podName, nil)
			if err != nil {
				log.Errorf("failed to delete tombstone pod %s: %v", podName, err)
				return err
			}
		}
	}
	return nil
}

func (tkc *Controller) deleteTiKVPods(ns, name string) error {
	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(
			label.New().Cluster(strings.TrimSuffix(name, "-tikvset")).TiKV().Labels(),
		).String(),
	}

	return wait.Poll(5*time.Second, 10*time.Minute, func() (bool, error) {
		podList, err := tkc.cli.CoreV1().Pods(ns).List(opts)
		if err != nil {
			return false, nil
		}

		for _, pod := range podList.Items {
			err := tkc.releasePVCFor(&pod)
			if err != nil {
				return false, nil
			}

			err = tkc.cli.CoreV1().Pods(ns).Delete(pod.Name, nil)
			if err != nil {
				return false, nil
			}
		}

		return true, nil
	})
}

func (tkc *Controller) releasePVCFor(pod *v1.Pod) error {
	for _, vol := range pod.Spec.Volumes {
		if vol.Name == tsapi.TiKVVolumeName && vol.PersistentVolumeClaim != nil {
			err := tkc.cli.CoreV1().PersistentVolumeClaims(pod.Namespace).
				Delete(vol.PersistentVolumeClaim.ClaimName, nil)
			if err != nil && !apierrors.IsNotFound(err) {
				return errors.Trace(err)
			}

			break
		}
	}

	return nil
}
