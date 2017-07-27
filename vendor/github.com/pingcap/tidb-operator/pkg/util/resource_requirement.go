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

package util

import (
	"github.com/ngaut/log"
	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/pkg/api/v1"
)

// ResourceRequirement creates ResourceRequirements for MemberSpec
func ResourceRequirement(spec tcapi.MemberSpec) v1.ResourceRequirements {
	rr := v1.ResourceRequirements{}
	if spec.Requests != nil {
		rr.Requests = make(map[v1.ResourceName]resource.Quantity)
		if q, err := resource.ParseQuantity(spec.Requests.CPU); err != nil {
			log.Errorf("failed to parse CPU resource %s to quantity: %v", spec.Requests.CPU, err)
		} else {
			rr.Requests[v1.ResourceCPU] = q
		}
		if q, err := resource.ParseQuantity(spec.Requests.Memory); err != nil {
			log.Errorf("failed to parse memory resource %s to quantity: %v", spec.Requests.Memory, err)
		} else {
			rr.Requests[v1.ResourceMemory] = q
		}
	}
	if spec.Limits != nil {
		rr.Limits = make(map[v1.ResourceName]resource.Quantity)
		if q, err := resource.ParseQuantity(spec.Limits.CPU); err != nil {
			log.Errorf("failed to parse CPU resource %s to quantity: %v", spec.Limits.CPU, err)
		} else {
			rr.Limits[v1.ResourceCPU] = q
		}
		if q, err := resource.ParseQuantity(spec.Limits.Memory); err != nil {
			log.Errorf("failed to parse memory resource %s to quantity: %v", spec.Limits.Memory, err)
		} else {
			rr.Limits[v1.ResourceMemory] = q
		}
	}
	return rr
}
