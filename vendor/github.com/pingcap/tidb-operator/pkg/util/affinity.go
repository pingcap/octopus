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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

var (
	// weight is in range 1-100
	topologySchedulingWeight = map[string]int32{
		"region": 10,
		"zone":   20,
		"rack":   40,
	}
)

// AntiAffinityForPod creates a PodAntiAffinity with antiLabels
func AntiAffinityForPod(namespace string, antiLabels map[string]string) *v1.PodAntiAffinity {
	terms := []v1.WeightedPodAffinityTerm{}
	for key, weight := range topologySchedulingWeight {
		term := v1.WeightedPodAffinityTerm{
			Weight: weight,
			PodAffinityTerm: v1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{MatchLabels: antiLabels},
				TopologyKey:   key,
				Namespaces:    []string{namespace},
			},
		}
		terms = append(terms, term)
	}
	return &v1.PodAntiAffinity{
		// pod should not be scheduled on the same host
		RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
			{
				LabelSelector: &metav1.LabelSelector{MatchLabels: antiLabels},
				TopologyKey:   metav1.LabelHostname,
				Namespaces:    []string{namespace},
			},
		},
		// pod should prefer not be scheduled on same rack, then zone and then region
		PreferredDuringSchedulingIgnoredDuringExecution: terms,
	}
}

// AffinityForNodeSelector creates an Affinity for NodeSelector
// Externally we use NodeSelector for simplicity,
// while internally we convert it to affinity which can express complex scheduling rules
func AffinityForNodeSelector(namespace string, antiLabels, selector map[string]string) *v1.Affinity {
	if selector == nil {
		return nil
	}
	requiredTerms := []v1.NodeSelectorTerm{}
	preferredTerms := []v1.PreferredSchedulingTerm{}
	for key, val := range selector {
		// region,zone,rack are preferred labels, others are must match labels
		if weight, ok := topologySchedulingWeight[key]; ok {
			if val == "" {
				continue
			}
			values := strings.Split(val, ",")
			t := v1.PreferredSchedulingTerm{
				Weight: weight,
				Preference: v1.NodeSelectorTerm{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      key,
							Operator: v1.NodeSelectorOpIn,
							Values:   values,
						},
					},
				},
			}
			preferredTerms = append(preferredTerms, t)
		} else {
			t := v1.NodeSelectorTerm{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{
						Key:      key,
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{val},
					},
				},
			}
			requiredTerms = append(requiredTerms, t)
		}
	}

	affinity := &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: requiredTerms,
			},
			PreferredDuringSchedulingIgnoredDuringExecution: preferredTerms,
		},
	}
	if antiLabels != nil {
		affinity.PodAntiAffinity = AntiAffinityForPod(namespace, antiLabels)
	}
	return affinity
}
