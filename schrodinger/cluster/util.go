package cluster

import (
	"fmt"
	"strings"

	"k8s.io/client-go/pkg/api/v1"
)

func getVersion(name string) string {
	s := strings.Split(name, ":")
	if len(s) < 2 {
		return defaultVersion
	}
	return s[len(s)-1]
}

func getName(clusterName, app string) string {
	return fmt.Sprintf("%s-%s", clusterName, app)
}

func affinityForNodeSelector(selector map[string]string) *v1.Affinity {
	affinity := &v1.Affinity{}
	if len(selector) == 0 {
		return affinity
	}
	affinity.NodeAffinity = &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "kind",
							Operator: v1.NodeSelectorOpIn,
							Values:   []string{selector["kind"]},
						},
					},
				},
			},
		},
	}
	return affinity
}
