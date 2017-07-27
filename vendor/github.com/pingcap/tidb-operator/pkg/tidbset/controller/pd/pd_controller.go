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

package pd

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb-operator/pkg/client"
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	"github.com/pingcap/tidb-operator/pkg/util"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	"github.com/pingcap/tidb-operator/pkg/util/pdutil"
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

const timeout = 2 * time.Second

// Controller is the controller of tidbset for PD
type Controller struct {
	cli        client.Interface
	controller cache.Controller
	store      cache.Store
	queue      *workqueue.Type
}

// NewController creates a new PD Controller
func NewController(cli client.Interface) *Controller {
	pc := &Controller{
		cli:   cli,
		queue: workqueue.New(),
	}
	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(
			label.New().PD().Labels(),
		).String(),
	}
	pc.store, pc.controller = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(_ metav1.ListOptions) (runtime.Object, error) {
				return pc.cli.PingcapV1().TidbSets(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(_ metav1.ListOptions) (watch.Interface, error) {
				return pc.cli.PingcapV1().TidbSets(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&tsapi.TidbSet{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    pc.enqueuePD,
			UpdateFunc: pc.updatePD,
			DeleteFunc: pc.enqueuePD,
		})
	return pc
}

// Run method runs PD Controller
func (pc *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer pc.queue.ShutDown()
	log.Infof("Starting PD controller")

	go pc.controller.Run(stopCh)
	go wait.Until(pc.worker, time.Second, stopCh)
	<-stopCh
	log.Infof("Shutting down PD controller")
}

func (pc *Controller) enqueuePD(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		log.Errorf("can't get key for obj: %v, error: %v", obj, err)
		return
	}
	pc.queue.Add(key)
}

func (pc *Controller) worker() {
	for {
		func() {
			key, quit := pc.queue.Get()
			if quit {
				return
			}
			defer pc.queue.Done(key)
			if err := pc.syncPDSet(key.(string)); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (pc *Controller) syncPDSet(key string) error {
	startTime := time.Now()
	defer func() {
		log.Infof("Finished syncing PD %q (%v)", key, time.Now().Sub(startTime))
	}()
	obj, exists, err := pc.store.GetByKey(key)
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
		_, err = pc.cli.PingcapV1().TidbSets(ns).Get(name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) { // make sure tidbset is actually deleted
				opts := metav1.ListOptions{
					LabelSelector: labels.SelectorFromSet(
						label.New().Cluster(strings.TrimSuffix(name, "-pdset")).PD().Labels(),
					).String(),
				}
				return pc.deletePDPods(ns, opts)
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
	defer pc.syncPDStatus(ts)
	if err := pc.syncPDPodFor(ts); err != nil {
		log.Errorf("failed to sync PD pod: %v", err)
		return err
	}
	return nil
}

func (pc *Controller) updatePD(old, cur interface{}) {
	pc.enqueuePD(cur)
}

func (pc *Controller) syncPDPodFor(tidbset *tsapi.TidbSet) error {
	err := pc.syncPDReplicasStatus(tidbset)
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
		log.Error(err)
		return err
	}
	podSpec := obj.(v1.PodSpec)
	commands := podSpec.Containers[0].Command
	opts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(tidbset.Spec.Selector.MatchLabels).String()}
	podList, err := pc.cli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		return errors.Trace(err)
	}
	if len(podList.Items) == 0 {
		podSpec.Containers[0].Command = append(commands, "--initial-cluster=$(MY_POD_NAME)=http://$(MY_POD_IP):2380")
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName:    fmt.Sprintf("%s-pd-", clusterName),
				Namespace:       ns,
				Labels:          tidbset.Spec.Selector.MatchLabels,
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
			Spec: podSpec,
		}
		_, err := pc.cli.CoreV1().Pods(ns).Create(pod)
		return err
	}
	podCounts := map[string]int{
		"running": 0,
		"pending": 0,
	}
	for _, pod := range podList.Items {
		if pod.Status.Reason == util.NodeUnreachablePodReason {
			// We should remove unreachable PD from cluster, because after node
			// is reachable again, the pod will be deleted by kubelet
			if err := pc.deletePD(ns, clusterName, pod.Name); err != nil {
				log.Errorf("failed to remove unreachable PD")
				return err
			}
			continue
		}
		switch pod.Status.Phase {
		case v1.PodPending:
			podCounts["pending"]++
		case v1.PodRunning:
			podCounts["running"]++
		default:
			log.Infof("PD pod(%s) state: %s", pod.Name, pod.Status.Phase)
		}
	}
	if podCounts["running"] == 0 {
		log.Infof("waiting for the first PD start")
		return nil
	}
	podsTotal := podCounts["running"] + podCounts["pending"]
	if podsTotal < int(tidbset.Spec.Replicas) {
		podSpec.Containers[0].Command = append(commands, fmt.Sprintf("--join=%s-pd:2379", clusterName))
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName:    fmt.Sprintf("%s-pd-", clusterName),
				Namespace:       ns,
				Labels:          tidbset.Spec.Selector.MatchLabels,
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
			Spec: podSpec,
		}
		_, err := pc.cli.CoreV1().Pods(ns).Create(pod)
		return err
	} else if podsTotal == int(tidbset.Spec.Replicas) {
		log.Infof("already satisfied")
		return nil
	} else {
		// here we select the first pd to remove
		pdName := podList.Items[0].Name
		if err := pc.deletePD(ns, clusterName, pdName); err != nil {
			return err
		}
		if err := pc.deletePDPod(ns, pdName); err != nil {
			return err
		}
		log.Infof("need to delete PD pod")
	}
	return pc.syncPDReplicasStatus(tidbset)
}

func (pc *Controller) deletePDPods(ns string, opts metav1.ListOptions) error {
	podList, err := pc.cli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		return err
	}
	for _, pod := range podList.Items {
		if err := pc.cli.CoreV1().Pods(ns).Delete(pod.Name, nil); err != nil {
			log.Error(err)
		}
	}
	return nil
}

// deregister pd from pd cluster
func (pc *Controller) deletePD(namespace, clusterName, pdName string) error {
	pdCli := pdutil.New(fmt.Sprintf("http://%s-pd.%s:2379", clusterName, namespace), timeout)
	return pdCli.DeleteMember(pdName)
}

func (pc *Controller) deletePDPod(ns, name string) error {
	err := pc.cli.CoreV1().Pods(ns).Delete(name, nil)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

func (pc *Controller) syncPDReplicasStatus(tidbset *tsapi.TidbSet) error {
	ns := tidbset.Metadata.Namespace
	name := tidbset.Metadata.Name
	opts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(tidbset.Spec.Selector.MatchLabels).String()}
	return wait.Poll(5*time.Second, 30*time.Second, func() (bool, error) {
		podList, err := pc.cli.CoreV1().Pods(ns).List(opts)
		if err != nil {
			log.Error(err)
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
		latestTidbset, err := pc.cli.PingcapV1().TidbSets(ns).Get(name, metav1.GetOptions{})
		if err != nil {
			log.Errorf("get pd set failed: %v", err)
			return false, nil
		}
		if latestTidbset.Status.Replicas != replicas || latestTidbset.Status.ReadyReplicas != readyReplicas {
			latestTidbset.Status.Replicas = replicas
			latestTidbset.Status.ReadyReplicas = readyReplicas
			_, err := pc.cli.PingcapV1().TidbSets(ns).Update(latestTidbset)
			if err != nil {
				log.Errorf("update pd set status failed: %v", err)
				return false, nil
			}
		}
		return true, nil
	})
}

func (pc *Controller) syncPDStatus(tidbset *tsapi.TidbSet) error {
	ns := tidbset.Metadata.Namespace
	name := tidbset.Metadata.Name
	opts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(tidbset.Spec.Selector.MatchLabels).String()}
	return wait.Poll(5*time.Second, 30*time.Second, func() (bool, error) {
		podList, err := pc.cli.CoreV1().Pods(ns).List(opts)
		if err != nil {
			log.Error(err)
			return false, nil
		}
		pods := make(map[string]v1.Pod)
		for _, pod := range podList.Items {
			pods[pod.Name] = pod
		}
		latestTidbset, err := pc.cli.PingcapV1().TidbSets(ns).Get(name, metav1.GetOptions{})
		if err != nil {
			log.Errorf("get PdSet failed: %v", err)
			return false, nil
		}
		if latestTidbset.Status.PDStatus == nil {
			latestTidbset.Status.PDStatus = make(map[string]tsapi.PDStatus)
		}
		clusterName := strings.TrimSuffix(tidbset.Metadata.OwnerReferences[0].Name, "-pdset")
		pdCli := pdutil.New(fmt.Sprintf("http://%s-pd.%s:2379", clusterName, ns), timeout)
		membersInfo, err := pdCli.GetMembers()
		if err != nil {
			log.Errorf("failed to list PD members: %v", err)
			return false, nil
		}
		pdStatus := make(map[string]tsapi.PDStatus)
		if membersInfo != nil {
			members := make(map[string]*pdpb.Member)
			for _, member := range membersInfo["members"] {
				id := member.GetMemberId()
				name := member.GetName()
				members[name] = member
				u, err := url.Parse(member.PeerUrls[0])
				if err != nil {
					log.Error(err)
					return false, nil
				}
				ip := strings.Split(u.Host, ":")[0]
				status := tsapi.PDStatus{
					Name: name,
					ID:   fmt.Sprintf("%d", id),
					IP:   ip,
				}
				pod, exist := pods[name]
				// pod was deleted accidentally or unreachable
				if !exist || pod.Status.Reason == util.NodeUnreachablePodReason {
					err = pdCli.DeleteMember(name)
					if err != nil {
						log.Errorf("failed to delete PD(%s) from cluster: %v", name, err)
						return false, nil
					}
					continue
				}
				pdStatus[name] = status
			}
			if !reflect.DeepEqual(pdStatus, latestTidbset.Status.PDStatus) {
				latestTidbset.Status.PDStatus = pdStatus
				_, err := pc.cli.PingcapV1().TidbSets(ns).Update(latestTidbset)
				if err != nil {
					log.Errorf("update PdSet status failed: %v", err)
					return false, nil
				}
			}
		}
		return true, nil
	})
}
