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

package volumemanager

import (
	"os"
	"path"

	"k8s.io/client-go/pkg/api/v1"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/scheduler"
	"github.com/pingcap/tidb-operator/pkg/util"
)

// VolumeRecover is an interface for managing tidb-operator's PV.
type VolumeRecover interface {
	// Recover recover the input PV, remove all directories and files under hostPath/tikv
	Recover(*v1.PersistentVolume) error
}

type volumeRecover struct{}

// NewVolumeRecover returns a VolumeRecover instance.
func NewVolumeRecover() VolumeRecover {
	return &volumeRecover{}
}

func (vr *volumeRecover) Recover(pv *v1.PersistentVolume) error {
	if hostPath, ok := pv.Annotations[scheduler.AnnotationHostPath]; ok {
		podName, _ := pv.Labels[scheduler.LabelPodName]
		podNamespace, _ := pv.Labels[scheduler.LabelPodNamespace]

		log.Infof("tidb-volume-manager: deleting directory: %s of PV: %s for pod: %s/%s", hostPath, pv.Name, podNamespace, podName)

		// TODO TiKVStoreDir may be many directories
		err := os.RemoveAll(path.Join(hostPath, util.TiKVStoreDir))
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	return errors.Errorf("can't find %s annotation in PV: %s", scheduler.AnnotationHostPath, pv.Name)
}
