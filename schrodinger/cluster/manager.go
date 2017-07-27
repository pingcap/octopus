package cluster

import (
	"bytes"
	"fmt"
	"html/template"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/schrodinger/config"
	"github.com/pingcap/tidb-operator/pkg/client"
	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/pkg/api/v1"
)

const (
	pdTemplateFile   = "pd.toml.tmpl"
	tikvTemplateFile = "tikv.toml.tmpl"
)

type Manager struct {
	RepoPrefix  string
	ServiceType string

	cli            client.Interface
	tikvConfigTmpl *template.Template
	pdConfigTmpl   *template.Template
	nodesInfo      NodesInfo
}

type NodesInfo struct {
	Nodes     []string
	UpdatedAt time.Time
}

type Service struct {
	NodeIP       []string `json:"node_ip,omitempty"` // if ServiceType is NodePort or LoadBalancer, NodeIP is all nodes' IP
	NodePort     int      `json:"node_port,omitempty"`
	ClusterIP    string   `json:"cluster_ip,omitempty"`
	ClusterPort  int      `json:"cluster_port,omitempty"`
	ExternalIP   string   `json:"external_ip,omitempty"`   // LoadBalancer IP
	ExternalPort int      `json:"external_port,omitempty"` // LoadBalancer Port
}

func NewClusterManager(cfg *config.Config) *Manager {
	cli, err := client.New(cfg.KubeConfig)
	if err != nil {
		log.Fatal("failed to initialize kube client")
	}
	m := &Manager{
		RepoPrefix:  cfg.RepoPrefix,
		ServiceType: cfg.ServiceType,
		cli:         cli,
	}
	err = m.loadTemplateFiles(cfg.TemplateDir)
	if err != nil {
	}
	return m
}

func (m *Manager) loadTemplateFiles(path string) error {
	if _, err := os.Stat(path); err != nil {
		return errors.Trace(err)
	}
	pdTmpl, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", path, pdTemplateFile))
	if err != nil {
		return errors.Trace(err)
	}
	m.pdConfigTmpl = template.Must(template.New("pd-config").Parse(string(pdTmpl)))

	tikvTmpl, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", path, tikvTemplateFile))
	if err != nil {
		return errors.Trace(err)
	}
	m.tikvConfigTmpl = template.Must(template.New("tikv-config").Parse(string(tikvTmpl)))
	return nil
}

func (m *Manager) GetCluster(namespace, clusterName string) (*Cluster, error) {
	cluster, err := m.cli.PingcapV1().TidbClusters(namespace).Get(clusterName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	c := &Cluster{
		Name:      cluster.Metadata.Namespace,
		CreatedAt: cluster.Metadata.CreationTimestamp.Time,
		PD:        &PodSpec{Size: cluster.Spec.PD.Size, Version: getVersion(cluster.Spec.PD.Image)},
		TiDB:      &PodSpec{Size: cluster.Spec.TiDB.Size, Version: getVersion(cluster.Spec.TiDB.Image)},
		TiKV:      &PodSpec{Size: cluster.Spec.TiKV.Size, Version: getVersion(cluster.Spec.TiKV.Image)},
	}
	option := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"tidb_cluster": clusterName,
			"owner":        "tidb-cluster",
		}).String(),
	}
	pods, err := m.cli.CoreV1().Pods(namespace).List(option)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, pod := range pods.Items {
		if pod.ObjectMeta.Labels != nil {
			switch pod.ObjectMeta.Labels["app"] {
			case "pd":
				c.PDStatus = append(c.PDStatus, &PodStatus{
					Name:   pod.ObjectMeta.GetName(),
					PodIP:  pod.Status.PodIP,
					NodeIP: pod.Status.HostIP,
					Status: string(pod.Status.Phase),
				})
			case "tikv":
				c.TiKVStatus = append(c.TiKVStatus, &PodStatus{
					Name:   pod.ObjectMeta.GetName(),
					PodIP:  pod.Status.PodIP,
					NodeIP: pod.Status.HostIP,
					Status: string(pod.Status.Phase),
				})
			case "tidb":
				c.TiDBStatus = append(c.TiDBStatus, &PodStatus{
					Name:   pod.ObjectMeta.GetName(),
					PodIP:  pod.Status.PodIP,
					NodeIP: pod.Status.HostIP,
					Status: string(pod.Status.Phase),
				})
			case "tidb-monitor", "configure-grafana":
				log.Infof("tidb monitor: %s", pod.ObjectMeta.Labels["app"])
			default:
				log.Warnf("unexpected pod type %s", pod.ObjectMeta.Labels["app"])
			}
		}
	}
	serviceNames := map[string]string{
		"grafana":    "tidb-monitor-grafana",
		"prometheus": "tidb-monitor-prometheus",
		"tidb":       "tidb",
	}
	for kind, name := range serviceNames {
		svc, err := m.cli.CoreV1().Services(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			log.Errorf("failed to get service %s: %v", name, err)
			continue
		}
		service := m.getService(svc)
		switch kind {
		case "grafana":
			c.GrafanaService = service
		case "prometheus":
			c.PrometheusService = service
		case "tidb":
			c.TiDBService = service
		default:
			log.Warnf("unexpected service kind %s", kind)
		}
	}
	return c, nil
}

func (m *Manager) CreateCluster(c *Cluster) error {
	//create namespace with cluster name
	err := m.CreateNamespace(c.Name)
	if err != nil {
		return errors.Trace(err)
	}
	enableMonitor := c.Monitor != nil
	config, err := m.genConfig(enableMonitor, c.TiDBLease)
	if err != nil {
		return errors.Trace(err)
	}
	if c.ServiceType == "" {
		c.ServiceType = m.ServiceType
	}
	s := &tcapi.TidbCluster{
		Metadata: metav1.ObjectMeta{
			Name:   c.Name,
			Labels: c.Labels,
		},
		Spec: tcapi.ClusterSpec{
			PD: tcapi.PDSpec{
				tcapi.MemberSpec{
					Size:         c.PD.Size,
					Image:        m.getImage("pd", c.PD.Version),
					Limits:       c.PD.Limits,
					Requests:     c.PD.Requests,
					NodeSelector: c.PD.NodeSelector,
				},
			},
			TiDB: tcapi.TiDBSpec{
				tcapi.MemberSpec{
					Size:         c.TiDB.Size,
					Image:        m.getImage("tidb", c.TiDB.Version),
					Limits:       c.TiDB.Limits,
					Requests:     c.TiDB.Requests,
					NodeSelector: c.TiDB.NodeSelector,
				},
			},
			TiKV: tcapi.TiKVSpec{
				tcapi.MemberSpec{
					Size:         c.TiKV.Size,
					Image:        m.getImage("tikv", c.TiKV.Version),
					Limits:       c.TiKV.Limits,
					Requests:     c.TiKV.Requests,
					NodeSelector: c.TiKV.NodeSelector,
				},
			},
			Paused:  false,
			Config:  config,
			Service: c.ServiceType,
		},
	}
	_, err = m.cli.PingcapV1().TidbClusters(c.Name).Create(s)
	if err != nil {
		log.Errorf("failed to create tidbcluster %s: %v", c.Name, err)
		return errors.Trace(err)
	}
	return nil
}

//func (m *Manager) DeleteCluster(namespace, clusterName string, interval, timeout time.Duration) {
//}

//func (m *Manager) deleteJobs(namespace, clusterName string) {
//}

func (m *Manager) createNamespace(namespace string) error {
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	_, err := m.cli.CoreV1().Namespaces().Create(ns)
	return errors.Trace(err)
}

func (m *Manager) getImage(kind string, version string) string {
	if version == "" {
		return fmt.Sprintf("%s/%s:%s", m.RepoPrefix, kind, defaultVersion)
	}
	return fmt.Sprintf("%s/%s:%s", m.RepoPrefix, kind, version)
}

func (m *Manager) getService(svc *v1.Service) Service {
	service := Service{
		ClusterIP:   svc.Spec.ClusterIP,
		ClusterPort: int(svc.Spec.Ports[0].Port),
	}
	if svc.Spec.Type == v1.ServiceTypeNodePort || svc.Spec.Type == v1.ServiceTypeLoadBalancer {
		service.NodePort = int(svc.Spec.Ports[0].NodePort)
		service.NodeIP = m.getLatestNodes()
	}
	if svc.Spec.Type == v1.ServiceTypeLoadBalancer {
		service.ExternalIP = svc.Status.LoadBalancer.Ingress[0].IP
	}
	return service
}

func (m *Manager) getLatestNodes() []string {
	if int(time.Now().Sub(m.nodesInfo.UpdatedAt).Seconds()) < 30 {
		return m.nodesInfo.Nodes
	}
	nodes, err := m.cli.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		log.Errorf("failed to retrieve nodes: %v", nodes)
		return m.nodesInfo.Nodes
	}
	m.nodesInfo.Nodes = []string{}
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable {
			continue
		}
		for _, a := range node.Status.Addresses {
			if a.Type == v1.NodeInternalIP {
				m.nodesInfo.Nodes = append(m.nodesInfo.Nodes, a.Address)
			}
		}
	}
	m.nodesInfo.UpdatedAt = time.Now()
	return m.nodesInfo.Nodes
}

func (m Manager) genConfig(enableMonitor bool, lease int) (map[string]string, error) {
	pdConfig := bytes.NewBuffer([]byte{})
	tikvConfig := bytes.NewBuffer([]byte{})
	metricsAddr := ""
	if enableMonitor {
		metricsAddr = "tidb-cluster-pushgateway:9091"
	}
	c := &clusterConfig{
		MetricsAddr: metricsAddr,
	}
	err := m.pdConfigTmpl.Execute(pdConfig, c)
	if err != nil {
		return nil, err
	}
	err = m.tikvConfigTmpl.Execute(tikvConfig, c)
	if err != nil {
		return nil, err
	}
	config := map[string]string{
		"pd-config":         pdConfig.String(),
		"tikv-config":       tikvConfig.String(),
		"tidb.lease":        strconv.Itoa(lease),
		"tidb.metrics-addr": metricsAddr,
	}
	if enableMonitor {
		config["prometheus-config"] = `global:
  scrape_interval: 15s
  evaluation_interval: 15s
  labels:
    monitor: 'prometheus'
scrape_configs:
  - job_name: 'tidb-cluster'
    scrape_interval: 5s
    honor_labels: true
    static_configs:
      - targets: ['127.0.0.1:9091']
        labels:
          cluster: 'tidb-cluster'
`
	}
	return config, nil
}

//func (m *Manager) ListCluster() {
//}

//func (m *Manager) DestroyCluster() {
//}
