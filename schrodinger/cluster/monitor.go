package cluster

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/util/label"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/pkg/api/v1"
	batchv1 "k8s.io/client-go/pkg/apis/batch/v1"
	extensionsv1beta1 "k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

func (m *Manager) createMonitorService(namespace, clusterName, kind string, port int) error {
	l := label.New().Cluster(clusterName).App("tidb-monitor").Labels()
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   getName(clusterName, kind),
			Labels: l,
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name:       kind,
					Port:       int32(port),
					TargetPort: intstr.FromInt(port),
					Protocol:   v1.ProtocolTCP,
				},
			},
			Selector: l,
		},
	}
	if kind == "prometheus" || kind == "grafana" {
		svc.Spec.Type = v1.ServiceTypeNodePort
	}
	if _, err := m.cli.CoreV1().Services(namespace).Create(svc); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Trace(err)
	}
	return nil
}

func (m *Manager) createMonitorServices(namespace, clusterName string) error {
	services := map[string]int{
		"pushgateway": 9091,
		"prometheus":  9090,
		"grafana":     3000,
	}
	for kind, port := range services {
		if err := m.createMonitorService(namespace, clusterName, kind, port); err != nil {
			log.Errorf("failed to create monitor service %s: %v", kind, err)
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *Manager) createMonitorPods(cluster *Cluster, namespace string) error {
	l := label.New().Cluster(cluster.Name).App("tidb-monitor")
	reserveDays := promRetentionDays
	if cluster.MonitorReserveDays > 0 {
		reserveDays = cluster.MonitorReserveDays
	}
	versions := map[string]string{"grafana": grafanaVersion, "prometheus": prometheusVersion, "pustgateway": pushgatewayVersion}
	if values := strings.Split(cluster.Monitor.Version, ","); len(values) == 3 {
		versions = map[string]string{
			"grafana":            values[0],
			"prometheus":         values[1],
			"pushgatewayVersion": values[2],
		}
	}
	deploy := &extensionsv1beta1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   getName(cluster.Name, "monitor"),
			Labels: l,
		},
		Spec: extensionsv1beta1.DeploymentSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   getName(cluster.Name, "monitor"),
					Labels: l,
				},
				Spec: v1.PodSpec{
					Affinity: affinityForNodeSelector(cluster.Monitor.NodeSelector),
					Containers: []v1.Container{
						{
							Name: "pushgateway", Image: fmt.Sprintf("%s/pushgateway: %s", m.RepoPrefix, versions["pushgateway"]),
							Ports: []v1.ContainerPort{
								{
									Name:          "pushgateway",
									ContainerPort: int32(9091),
									Protocol:      v1.ProtocolTCP,
								},
							},
						},
						{
							Name:  "prometheus",
							Image: fmt.Sprintf("%s/prometheus:%s", m.RepoPrefix, versions["prometheus"]),
							Command: []string{
								"/bin/prometheus",
								"-config.file=/etc/prometheus/prometheus.yml",
								fmt.Sprintf("-storage.local.retention=%dh0m0s", 24*reserveDays),
								"-storage.local.path=/prometheus",
							},
							Ports: []v1.ContainerPort{
								{
									Name:          "pushgateway",
									ContainerPort: int32(9090),
									Protocol:      v1.ProtocolTCP,
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{Name: "timezone", ReadOnly: true, MountPath: "etc/localtime"},
								{Name: "prometheus-config", ReadOnly: true, MountPath: "/etc/prometheus"},
								{Name: "prometheus-data", MountPath: "/prometheus"},
							},
						},
						{
							Name:  "grafana",
							Image: fmt.Sprintf("%s/grafana:%s", m.RepoPrefix, versions["grafana"]),
							Ports: []v1.ContainerPort{
								{
									Name:          "grafana",
									ContainerPort: int32(3000),
									Protocol:      v1.ProtocolTCP,
								},
							},
							VolumeMounts: []v1.VolumeMount{
								{Name: "timezone", ReadOnly: true, MountPath: "/etc/locate.rc"},
								{Name: "grafana-data", MountPath: "/data"},
							},
							Env: []v1.EnvVar{
								{Name: "GF_PATHS_DATA", Value: "/data"},
							},
						},
					},
					Volumes: []v1.Volume{
						{Name: "timezone", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/etc/localtime"}}},
						{Name: "prometheus-data", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
						{Name: "grafana-data", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
						{Name: "prometheus-config", VolumeSource: v1.VolumeSource{
							ConfigMap: &v1.ConfigMapVolumeSource{
								LocalObjectReference: v1.LocalObjectReference{Name: getName(cluster.Name, "config")},
								Items:                []v1.KeyToPath{{Key: "prometheus-config", Path: "prometheus.yml"}},
							},
						}},
					},
				},
			},
		},
	}
	deploy.Spec.Template.Spec.Containers[1].Resources = *getResourceRequirements(cluster.Monitor)
	if _, err := m.cli.ExtensionsV1beta1().Deployments(namespace).Create(deploy); err != nil {
		log.Errorf("error creating tidb-monitor deployment: %v", err)
		return errors.Trace(err)
	}
	return nil
}

func getResourceRequirements(sp *PodSpec) *v1.ResourceRequirements {
	rr := v1.ResourceRequirements{}
	if sp.Requests != nil {
		rr.Requests = make(map[v1.ResourceName]resource.Quantity)
		if q, err := resource.ParseQuantity(sp.Requests.CPU); err != nil {
			log.Errorf("failed to parse CPU resource %s to quantity: %v", sp.Requests.CPU, err)
		} else {
			rr.Requests[v1.ResourceCPU] = q
		}
		if q, err := resource.ParseQuantity(sp.Requests.Memory); err != nil {
			log.Errorf("failed to parse memory resource %s to quantity: %v", sp.Requests.Memory, err)
		} else {
			rr.Requests[v1.ResourceMemory] = q
		}
	}
	if sp.Limits != nil {
		rr.Limits = make(map[v1.ResourceName]resource.Quantity)
		if q, err := resource.ParseQuantity(sp.Limits.CPU); err != nil {
			log.Errorf("failed to parse CPU resource %s to quantity: %v", sp.Limits.CPU, err)
		} else {
			rr.Limits[v1.ResourceCPU] = q
		}
		if q, err := resource.ParseQuantity(sp.Limits.Memory); err != nil {
			log.Errorf("failed to parse memory resource %s to quantity: %v", sp.Limits.Memory, err)
		} else {
			rr.Limits[v1.ResourceMemory] = q
		}
	}
	return &rr
}

func (m *Manager) configGrafana(namespace, clusterName string) error {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:   getName(clusterName, "configure-grafana"),
			Labels: label.New().Cluster(clusterName).App("configure-grafana").Labels(),
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: getName(clusterName, "configure-grafana"),
				},
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyOnFailure,
					Containers: []v1.Container{
						{
							Name:  "configure-grafana",
							Image: fmt.Sprintf("%s/tidb-dashboard-installer:%s", m.RepoPrefix, dashboardInstallerVersion),
							Args:  []string{fmt.Sprintf("%s:3000", getName(clusterName, "grafana"))},
						},
					},
				},
			},
		},
	}
	if _, err := m.cli.BatchV1().Jobs(namespace).Create(job); err != nil {
		log.Errorf("error creating configure grafana job: %+v", err)
		return err
	}
	return nil
}

func (m *Manager) createTidbMonitor(cluster *Cluster, namespace string) error {
	var err error
	if err = m.createMonitorServices(namespace, cluster.Name); err != nil {
		log.Errorf("failed to create monitor services: %v", err)
		return err
	}
	if err = m.createMonitorPods(cluster, namespace); err != nil {
		log.Errorf("failed to create monitor pods: %v", err)
		return err
	}
	if err = m.configGrafana(namespace, cluster.Name); err != nil {
		log.Errorf("failed to config grafana: %v", err)
		return err
	}
	return nil
}

func (m *Manager) deleteTidbMonitor(namespace, clusterName string) error {
	var err error
	services := []string{
		getName(clusterName, "grafana"),
		getName(clusterName, "pushgateway"),
		getName(clusterName, "prometheus"),
	}
	for _, svc := range services {
		if err = m.cli.CoreV1().Services(namespace).Delete(svc, nil); err != nil {
			log.Errorf("error deleting service %s: %+v", svc, err)
		}
	}
	if err = m.cli.CoreV1().ConfigMaps(namespace).Delete(getName(clusterName, "config"), nil); err != nil {
		log.Errorf("error deleting configmap %s: %+v", getName(clusterName, "config"), err)
	}
	if err = m.cli.ExtensionsV1beta1().Deployments(namespace).Delete(getName(clusterName, "monitor"), nil); err != nil {
		log.Errorf("error deleting deployment %s: %+v", getName(clusterName, "monitor"), err)
	}
	l := label.New().Cluster(clusterName).App("tidb-monitor").Labels()
	option := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(l).String()}
	rss, err := m.cli.ExtensionsV1beta1().ReplicaSets(namespace).List(option)
	if err == nil {
		log.Debugf("tidb-monitor replicaset count: %d", len(rss.Items))
		for _, rs := range rss.Items {
			log.Debugf("deleting replicaset %s", rs.ObjectMeta.Name)
			err = m.cli.ExtensionsV1beta1().ReplicaSets(namespace).Delete(rs.ObjectMeta.Name, nil)
			if err != nil {
				log.Errorf("error deleting replicaset %s: %v", rs.ObjectMeta.Name, err)
			}
		}
	}
	if err = m.cli.BatchV1().Jobs(namespace).Delete(getName(clusterName, "configure-grafana"), nil); err != nil {
		log.Errorf("error deleting configure-grafana job %s: %+v", getName(clusterName, "configure-grafana"), err)
	}
	return nil
}
