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
	Name               string            `json:"name"`
	PD                 *PodSpec          `json:"pd"`
	TiKV               *PodSpec          `json:"tikv"`
	TiDB               *PodSpec          `json:"tidb"`
	Monitor            *PodSpec          `json:"monitor,omitempty"`
	ServiceType        string            `json:"service_type,omitempty"` // default service type is set at manager startup
	TiDBLease          int               `json:"tidb_lease,omitempty"`   // this should be an advanced option
	MonitorReserveDays int               `json:"monitor_reserve_days,omitempty"`
	RootPassword       string            `json:"root_password,omitempty"`
	Labels             map[string]string `json:"labels,omitempty"` // store cluster level meta info

	// response info
	CreatedAt         time.Time    `json:"created_at,omitempty"`
	TiDBService       Service      `json:"tidb_service,omitempty"`
	PrometheusService Service      `json:"prometheus_service,omitempty"`
	GrafanaService    Service      `json:"grafana_service,omitempty"`
	PDStatus          []*PodStatus `json:"pd_status,omitempty"`
	TiDBStatus        []*PodStatus `json:"tidb_status,omitempty"`
	TiKVStatus        []*PodStatus `json:"tikv_status,omitempty"`
}

func (c *Cluster) Valid() bool {
	return true
}

type clusterConfig struct {
	MetricsAddr string
}

type PodStatus struct {
	Name   string `json:"name"`
	PodIP  string `json:"pod_ip"`
	NodeIP string `json:"node_ip"`
	Status string `json:"status"`
}

type PodSpec struct {
	Size         int                        `json:"size"`
	Version      string                     `json:"version,omitempty"`
	Requests     *tcapi.ResourceRequirement `json:"requests,omitempty"`
	Limits       *tcapi.ResourceRequirement `json:"limits,omitempty"`
	NodeSelector map[string]string          `json:"node_selector,omitempty"`
}

//type Job struct {
//Name   string `json:"name"`
//Status string `json:"status"`
//}

//type clusterConfig struct {
//MetricsAddr string
//}
