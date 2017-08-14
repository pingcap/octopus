package cluster

import (
	"database/sql"
	"fmt"
	"sync"

	"golang.org/x/net/context"
)

type Cluster interface {
	Prepare() error // Prepare resources.
	Deploy() error  // Deploy resources.
	Start() error   // Start cluster.
	Stop() error    // Stop cluster.
	Close() error   // Close cluster.
	Reset() error   // Cleanup cluster data.
	Destory() error // Cleanup Cluster data and resources.
	Accessor() *sql.DB
}

type clusterMeta struct {
	pd   *BinPackage
	tikv *BinPackage
	tidb *BinPackage
	dsn  *ClusterDSN
}

type clusterResource struct {
	free bool
	name string
	meta *clusterMeta
}

func (c *clusterResource) require(pd, tikv, tidb *BinPackage) {
	c.free = false
	c.meta.pd = pd
	c.meta.tikv = tikv
	c.meta.tidb = tidb
}

func (c *clusterResource) release() {
	c.free = true
	c.meta.pd = nil
	c.meta.tikv = nil
	c.meta.tidb = nil
}

type clusterWrapper struct {
	Cluster
	cm  *ClusterManager
	res *clusterResource
}

func (cw *clusterWrapper) Close() error {
	err := cw.Cluster.Close()
	cw.cm.ReleaseCluster(cw.res.name)
	return err
}

type ClusterManager struct {
	mux      sync.RWMutex
	ctx      context.Context
	clusters []*clusterResource
}

func NewClusterManager(ctx context.Context, cfg *AnsibleConfig) (*ClusterManager, error) {
	if err := initAnsibleEnv(cfg); err != nil {
		return nil, err
	}

	cm := &ClusterManager{ctx: ctx}
	for _, dsn := range cfg.Clusters {
		res := &clusterResource{
			free: true,
			name: dsn.Name,
			meta: &clusterMeta{dsn: dsn},
		}
		cm.clusters = append(cm.clusters, res)
	}

	return cm, nil
}

func (cm *ClusterManager) RequireCluster(pd, tikv, tidb *BinPackage) (Cluster, error) {
	cm.mux.Lock()
	defer cm.mux.Unlock()

	for _, res := range cm.clusters {
		if !res.free {
			continue
		}
		res.require(pd, tikv, tidb)

		cluster := &clusterWrapper{
			Cluster: NewAnsibleCluster(cm.ctx, res.meta),
			cm:      cm,
			res:     res,
		}
		return cluster, nil
	}

	return nil, fmt.Errorf("no cluster available")
}

func (cm *ClusterManager) ReleaseCluster(name string) {
	cm.mux.Lock()
	defer cm.mux.Unlock()

	for _, res := range cm.clusters {
		if res.name == name {
			res.release()
			return
		}
	}
}
