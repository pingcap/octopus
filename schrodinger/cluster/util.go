package cluster

import (
	"strings"
)

func getVersion(name string) string {
	s := strings.Split(name, ":")
	if len(s) < 2 {
		return defaultVersion
	}
	return s[len(s)-1]
}
