// Copyright 2016 PingCAP, Inc.
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
// limitations under the License.

package server

import (
	"math/rand"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/pkg/types"
	"github.com/juju/errors"
	"github.com/ngaut/systimemon"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/etcdutil"
	"google.golang.org/grpc"
)

const (
	etcdTimeout = time.Second * 3
	// pdRootPath for all pd servers.
	pdRootPath      = "/pd"
	pdAPIPrefix     = "/pd/"
	pdRPCPrefix     = "/pd/rpc"
	pdClusterIDPath = "/pd/cluster_id"
)

// Server is the pd server.
type Server struct {
	cfg         *Config
	scheduleOpt *scheduleOption

	etcd *embed.Etcd

	client *clientv3.Client

	clusterID uint64

	rootPath string

	isLeaderValue int64
	// leader value saved in etcd leader key.
	// Every write will use this to check leader validation.
	leaderValue string

	wg sync.WaitGroup

	closed int64

	// for tso
	ts            atomic.Value
	lastSavedTime time.Time

	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAlloc *idAllocator

	// for kv operation.
	kv *kv

	// for API operation.
	handler *Handler

	// for raft cluster
	clusterLock sync.RWMutex
	cluster     *RaftCluster

	msgID uint64

	id uint64
}

// NewServer creates the pd server with given configuration.
func NewServer(cfg *Config) (*Server, error) {
	s := CreateServer(cfg)
	if err := s.StartEtcd(nil); err != nil {
		s.Close()
		return nil, errors.Trace(err)
	}

	go systimemon.StartMonitor(time.Now, func() {
		log.Errorf("system time jumps backward")
		timeJumpBackCounter.Inc()
	})
	return s, nil
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(cfg *Config) *Server {
	log.Infof("PD config - %v", cfg)
	rand.Seed(time.Now().UnixNano())

	s := &Server{
		cfg:           cfg,
		scheduleOpt:   newScheduleOption(cfg),
		isLeaderValue: 0,
		closed:        1,
	}

	s.handler = newHandler(s)
	return s
}

// StartEtcd starts an embed etcd server with an user handler.
func (s *Server) StartEtcd(apiHandler http.Handler) error {
	etcdCfg, err := s.cfg.genEmbedEtcdConfig()
	if err != nil {
		return errors.Trace(err)
	}
	if apiHandler != nil {
		etcdCfg.UserHandlers = map[string]http.Handler{
			pdAPIPrefix: apiHandler,
		}
	}
	etcdCfg.ServiceRegister = func(gs *grpc.Server) { pdpb.RegisterPDServer(gs, s) }

	log.Info("start embed etcd")

	etcd, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return errors.Trace(err)
	}

	// Check cluster ID
	urlmap, err := types.NewURLsMap(s.cfg.InitialCluster)
	if err != nil {
		return errors.Trace(err)
	}
	if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlmap); err != nil {
		return errors.Trace(err)
	}

	endpoints := []string{etcdCfg.ACUrls[0].String()}

	log.Infof("create etcd v3 client with endpoints %v", endpoints)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return errors.Trace(err)
	}

	if err = etcdutil.WaitEtcdStart(client, endpoints[0]); err != nil {
		// See https://github.com/coreos/etcd/issues/6067
		// Here may return "not capable" error because we don't start
		// all etcds in initial_cluster at same time, so here just log
		// an error.
		// Note that pd can not work correctly if we don't start all etcds.
		log.Errorf("etcd start failed, err %v", err)
	}

	s.etcd = etcd
	s.client = client
	s.id = uint64(etcd.Server.ID())

	// update advertise peer urls.
	etcdMembers, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return errors.Trace(err)
	}
	for _, m := range etcdMembers.Members {
		if s.ID() == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
				log.Infof("update advertise peer urls from %s to %s", s.cfg.AdvertisePeerUrls, etcdPeerURLs)
				s.cfg.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}

	if err = s.initClusterID(); err != nil {
		return errors.Trace(err)
	}
	log.Infof("init cluster id %v", s.clusterID)

	s.rootPath = path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
	s.idAlloc = &idAllocator{s: s}
	s.kv = newKV(s)
	s.cluster = newRaftCluster(s, s.clusterID)

	// Server has started.
	atomic.StoreInt64(&s.closed, 0)
	return nil
}

func (s *Server) initClusterID() error {
	// Get any cluster key to parse the cluster ID.
	resp, err := kvGet(s.client, pdRootPath, clientv3.WithFirstCreate()...)
	if err != nil {
		return errors.Trace(err)
	}

	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		s.clusterID, err = initOrGetClusterID(s.client, pdClusterIDPath)
		return errors.Trace(err)
	}

	key := string(resp.Kvs[0].Key)

	// If the key is "pdClusterIDPath", parse the cluster ID from it.
	if key == pdClusterIDPath {
		s.clusterID, err = bytesToUint64(resp.Kvs[0].Value)
		return errors.Trace(err)
	}

	// Parse the cluster ID from any other keys for compatibility.
	elems := strings.Split(key, "/")
	if len(elems) < 3 {
		return errors.Errorf("invalid cluster key %v", key)
	}
	s.clusterID, err = strconv.ParseUint(elems[2], 10, 64)

	log.Infof("init and load cluster id: %d", s.clusterID)
	return errors.Trace(err)
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.closed, 0, 1) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.enableLeader(false)

	if s.client != nil {
		s.client.Close()
	}

	if s.etcd != nil {
		s.etcd.Close()
	}

	s.wg.Wait()

	log.Info("close server")
}

// isClosed checks whether server is closed or not.
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.closed) == 1
}

// Run runs the pd server.
func (s *Server) Run() {
	// We use "127.0.0.1:0" for test and will set correct listening
	// address before run, so we set leader value here.
	s.leaderValue = s.marshalLeader()

	s.wg.Add(1)
	s.leaderLoop()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
}

// GetHandler returns the handler for API.
func (s *Server) GetHandler() *Handler {
	return s.handler
}

// GetEndpoints returns the etcd endpoints for outer use.
func (s *Server) GetEndpoints() []string {
	return s.client.Endpoints()
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (s *Server) ID() uint64 {
	return s.id
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// txn returns an etcd client transaction wrapper.
// The wrapper will set a request timeout to the context and log slow transactions.
func (s *Server) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

// leaderTxn returns txn() with a leader comparison to guarantee that
// the transaction can be executed only if the server is leader.
func (s *Server) leaderTxn(cs ...clientv3.Cmp) clientv3.Txn {
	return s.txn().If(append(cs, s.leaderCmp())...)
}
