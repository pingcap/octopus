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
	"fmt"
	"regexp"

	"k8s.io/client-go/pkg/api/v1"
)

const (
	// TiKVStoreDir is the sub directory under hostPath for tikv
	TiKVStoreDir = "tikv"
	// NodeUnreachablePodReason is defined in k8s.io/kubernetes/pkg/util/node
	// but not in client-go and apimachinery, so we define it here
	NodeUnreachablePodReason = "NodeLost"
)

var rxDotOrSlash = regexp.MustCompile("[./]")

// GetPodNames returns the pods' name
func GetPodNames(pods []*v1.Pod) (names []string) {
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return
}

// PVName return a PV name for given node and its hostPath.
func PVName(nodeName, pathName string) string {
	return rxDotOrSlash.ReplaceAllString(fmt.Sprintf("pv-%s-%s", nodeName, pathName), "-")
}

// ContainsString checks if a given slice of strings contains the provided string.
func ContainsString(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}
