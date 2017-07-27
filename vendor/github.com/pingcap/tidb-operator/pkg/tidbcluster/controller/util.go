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
	"math"

	"github.com/ngaut/log"
	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
	"k8s.io/apimachinery/pkg/api/resource"
)

// tikv uses GB, TB as unit suffix, but it actually means GiB, TiB
func tikvCapacityArgs(limits *tcapi.ResourceRequirement) string {
	defaultArgs := "--capacity=0"
	if limits == nil {
		return defaultArgs
	}
	q, err := resource.ParseQuantity(limits.Storage)
	if err != nil {
		log.Warnf("failed to parse quantity %s: %v", limits.Storage, err)
		return defaultArgs
	}
	i, b := q.AsInt64()
	if !b {
		log.Warnf("quantity %s can't be converted to int64", q.String())
		return defaultArgs
	}
	return fmt.Sprintf("--capacity=%dGB", int(float64(i)/math.Pow(2, 30)))
}
