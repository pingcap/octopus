package cluster

import (
	"errors"
	"time"

	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
)

const (
	urlPrefix         = "/apis/pingcap.com/v1"
	tprName           = "tidbclusters"
	promRetentionDays = 30 // prometheus default retention days
	// default version for docker images
	defaultVersion            = "latest" // default version for pd/tidb/tikv
	grafanaVersion            = "4.2.0"
	pushgatewayVersion        = "v0.3.1"
	prometheusVersion         = "v1.5.2"
	dashboardInstallerVersion = "v0.1.0"
	maxRetries                = 10
)

var (
	ClusterNotFound = errors.New("tidb cluster not found")
)

type Cluster struct {
	Name               string            `yaml:"name" json:"name"`
	PD                 *PodSpec          `yaml:"name" json:"pd"`
	TiKV               *PodSpec          `yaml:"tikv" json:"tikv"`
	TiDB               *PodSpec          `yaml:"tidb" json:"tidb"`
	Monitor            *PodSpec          `yaml:"monitor" json:"monitor,omitempty"`
	ServiceType        string            `yaml:"service_type" json:"service_type,omitempty"` // default service type is set at manager startup
	TiDBLease          int               `yaml:"tidb_lease" json:"tidb_lease,omitempty"`     // this should be an advanced option
	MonitorReserveDays int               `yaml:"monitor_reserve_days" json:"monitor_reserve_days,omitempty"`
	Labels             map[string]string `yaml:"labels" json:"labels,omitempty"` // store cluster level meta info

	// response info
	CreatedAt         time.Time    `yaml:"created_at" json:"created_at,omitempty"`
	TiDBService       Service      `yaml:"tidb_service" json:"tidb_service,omitempty"`
	PrometheusService Service      `yaml:"prometheus" json:"prometheus_service,omitempty"`
	GrafanaService    Service      `yaml:"grafana_service" json:"grafana_service,omitempty"`
	PDStatus          []*PodStatus `yaml:"pd_status" json:"pd_status,omitempty"`
	TiDBStatus        []*PodStatus `yaml:"tidb_status" json:"tidb_status,omitempty"`
	TiKVStatus        []*PodStatus `yaml;:"tikv_status" json:"tikv_status,omitempty"`
}

func (c *Cluster) Valid() bool {
	return true
}

type clusterConfig struct {
	MetricsAddr string
}

type PodStatus struct {
	Name   string `yaml:"name" json:"name"`
	PodIP  string `yaml:"pod_ip" json:"pod_ip"`
	NodeIP string `yaml:"node_ip" json:"node_ip"`
	Status string `yaml:"status" json:"status"`
}

type PodSpec struct {
	Size         int                        `yaml:"size" json:"size"`
	Version      string                     `yaml:"version" json:"version,omitempty"`
	Requests     *tcapi.ResourceRequirement `yaml:"requests" json:"requests,omitempty"`
	Limits       *tcapi.ResourceRequirement `yaml:"limits" json:"limits,omitempty"`
	NodeSelector map[string]string          `yaml:"node_selector" json:"node_selector,omitempty"`
}

//type Job struct {
//Name   string `json:"name"`
//Status string `json:"status"`
//}

//type clusterConfig struct {
//MetricsAddr string
//}
