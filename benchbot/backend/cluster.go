package backend

import (
	"fmt"
	"sync"

	"database/sql"
	"golang.org/x/net/context"
)

type Cluster interface {
	Prepare() error
	Deploy() error
	Run() error
	Stop() error
	Reset() error
	Destory() error
	Ping() error
	Accessor() *sql.DB
	Close()
}

type clusterDSN struct {
	name     string
	host     string
	port     uint16
	db       string
	user     string
	password string
}

type clusterMeta struct {
	tidb *BinPackage
	tikv *BinPackage
	pd   *BinPackage
	dsn  *clusterDSN
}

type clusterResouce struct {
	free bool
	name string
	meta *clusterMeta
}

type ClusterManager struct {
	mux      sync.RWMutex
	ctx      context.Context
	clusters []*clusterResouce
}

var clusterManager = new(ClusterManager)

type clusterWrapper struct {
	c     Cluster
	cname string
	mgr   *ClusterManager
}

func (cw *clusterWrapper) Prepare() error { return cw.c.Prepare() }

func (cw *clusterWrapper) Deploy() error { return cw.c.Deploy() }

func (cw *clusterWrapper) Run() error { return cw.c.Run() }

func (cw *clusterWrapper) Stop() error { return cw.c.Stop() }

func (cw *clusterWrapper) Reset() error { return cw.c.Reset() }

func (cw *clusterWrapper) Destory() error { return cw.c.Destory() }

func (cw *clusterWrapper) Ping() error { return cw.c.Ping() }

func (cw *clusterWrapper) Accessor() *sql.DB { return cw.c.Accessor() }

func (cw *clusterWrapper) Close() {
	cw.c.Close()
	cw.mgr.recycle(cw.cname)
}

func (crs *clusterResouce) refresh() {
	crs.free = true
	crs.meta.tidb = nil
	crs.meta.tikv = nil
	crs.meta.pd = nil
}

func initClusterManager(cfg *ServerConfig, ctx context.Context) error {
	clusterManager.mux.Lock()
	defer clusterManager.mux.Unlock()

	num := len(cfg.Ansible.Clusters)
	clusterManager.clusters = make([]*clusterResouce, 0, num)
	clusterManager.ctx = ctx

	for i := 0; i < num; i++ {
		dsn := cfg.Ansible.Clusters[i]
		cmeta := &clusterMeta{
			dsn: &clusterDSN{
				name:     dsn.Name,
				host:     dsn.Host,
				port:     dsn.Port,
				db:       dsn.DB,
				user:     dsn.AuthUser,
				password: dsn.AuthPassword,
			},
		}

		cname := fmt.Sprintf("%d-%s", i, dsn.Name) // ps : avoid name confliction
		c := &clusterResouce{
			free: true,
			name: cname,
			meta: cmeta,
		}

		clusterManager.clusters = append(clusterManager.clusters, c)
	}

	return nil
}

func (cmgr *ClusterManager) applyAnsibleCluster(pd *BinPackage, tikv *BinPackage, tidb *BinPackage) Cluster {
	cmgr.mux.Lock()
	defer cmgr.mux.Unlock()

	for _, c := range cmgr.clusters {
		if !c.free {
			continue
		}

		c.free = false
		c.meta.tidb = tidb
		c.meta.tikv = tikv
		c.meta.pd = pd

		return &clusterWrapper{
			c:     newAnsibleCluster(c.meta, cmgr.ctx),
			cname: c.name,
			mgr:   cmgr,
		}
	}

	return nil
}

func (cmgr *ClusterManager) recycle(cname string) bool {
	cmgr.mux.Lock()
	defer cmgr.mux.Unlock()

	for _, c := range cmgr.clusters {
		if c.name == cname {
			// TODO ... assert 'c.free == false'
			c.refresh()
			return true
		}
	}

	return false
}
