package cluster

import (
	"html/template"

	"github.com/pingcap/tidb-operator/pkg/client"
)

type Manager struct {
	RepoPrefix  string
	ServiceType string

	cli            client.Interface
	tikvConfigTmpl *template.Template
	pdConfigTmpl   *template.Template
}

func NewClusterManager() *Manager {
	return &Manager{}
}
