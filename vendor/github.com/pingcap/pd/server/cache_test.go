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
	"sync/atomic"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testStoresInfoSuite{})

type testStoresInfoSuite struct{}

func checkStaleRegion(origin *metapb.Region, region *metapb.Region) error {
	o := origin.GetRegionEpoch()
	e := region.GetRegionEpoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return errors.Trace(errRegionIsStale(region, origin))
	}

	return nil
}

// Create n stores (0..n).
func newTestStores(n uint64) []*storeInfo {
	stores := make([]*storeInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		store := &metapb.Store{
			Id: i,
		}
		stores = append(stores, newStoreInfo(store))
	}
	return stores
}

func (s *testStoresInfoSuite) TestStores(c *C) {
	n := uint64(10)
	cache := newStoresInfo()
	stores := newTestStores(n)

	for i := uint64(0); i < n; i++ {
		c.Assert(cache.getStore(i), IsNil)
		c.Assert(cache.blockStore(i), NotNil)
		cache.setStore(stores[i])
		c.Assert(cache.getStore(i), DeepEquals, stores[i])
		c.Assert(cache.getStoreCount(), Equals, int(i+1))
		c.Assert(cache.blockStore(i), IsNil)
		c.Assert(cache.getStore(i).isBlocked(), IsTrue)
		c.Assert(cache.blockStore(i), NotNil)
		cache.unblockStore(i)
		c.Assert(cache.getStore(i).isBlocked(), IsFalse)
	}
	c.Assert(cache.getStoreCount(), Equals, int(n))

	for _, store := range cache.getStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	for _, store := range cache.getMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()].Store)
	}

	c.Assert(cache.getStoreCount(), Equals, int(n))
}

var _ = Suite(&testRegionsInfoSuite{})

type testRegionsInfoSuite struct{}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, np uint64) []*RegionInfo {
	regions := make([]*RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:       i,
			Peers:    peers,
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		regions = append(regions, newRegionInfo(region, peers[0]))
	}
	return regions
}

func (s *testRegionsInfoSuite) Test(c *C) {
	n, np := uint64(10), uint64(3)
	cache := newRegionsInfo()
	regions := newTestRegions(n, np)

	for i := uint64(0); i < n; i++ {
		region := regions[i]
		regionKey := []byte{byte(i)}

		c.Assert(cache.getRegion(i), IsNil)
		c.Assert(cache.searchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		cache.addRegion(region)
		checkRegion(c, cache.getRegion(i), region)
		checkRegion(c, cache.searchRegion(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])

		// Update leader to peer np-1.
		region.Leader = region.Peers[np-1]
		cache.setRegion(region)
		checkRegion(c, cache.getRegion(i), region)
		checkRegion(c, cache.searchRegion(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])

		cache.removeRegion(region)
		c.Assert(cache.getRegion(i), IsNil)
		c.Assert(cache.searchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		// Reset leader to peer 0.
		region.Leader = region.Peers[0]
		cache.addRegion(region)
		checkRegion(c, cache.getRegion(i), region)
		checkRegions(c, cache, regions[0:(i+1)])
		checkRegion(c, cache.searchRegion(regionKey), region)
	}

	for i := uint64(0); i < n; i++ {
		region := cache.randLeaderRegion(i)
		c.Assert(region.Leader.GetStoreId(), Equals, i)

		region = cache.randFollowerRegion(i)
		c.Assert(region.Leader.GetStoreId(), Not(Equals), i)

		c.Assert(region.GetStorePeer(i), NotNil)
	}

	// All regions will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.getStoreLeaderCount(i); j++ {
			region := cache.randLeaderRegion(i)
			region.PendingPeers = region.Peers
			cache.setRegion(region)
		}
		c.Assert(cache.randLeaderRegion(i), IsNil)
	}
	for i := uint64(0); i < n; i++ {
		c.Assert(cache.randFollowerRegion(i), IsNil)
	}
}

func checkRegion(c *C, a *RegionInfo, b *RegionInfo) {
	c.Assert(a.Region, DeepEquals, b.Region)
	c.Assert(a.Leader, DeepEquals, b.Leader)
	c.Assert(a.Peers, DeepEquals, b.Peers)
	if len(a.DownPeers) > 0 || len(b.DownPeers) > 0 {
		c.Assert(a.DownPeers, DeepEquals, b.DownPeers)
	}
	if len(a.PendingPeers) > 0 || len(b.PendingPeers) > 0 {
		c.Assert(a.PendingPeers, DeepEquals, b.PendingPeers)
	}
}

func checkRegionsKV(c *C, kv *kv, regions []*RegionInfo) {
	if kv != nil {
		for _, region := range regions {
			var meta metapb.Region
			ok, err := kv.loadRegion(region.GetId(), &meta)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(&meta, DeepEquals, region.Region)
		}
	}
}

func checkRegions(c *C, cache *regionsInfo, regions []*RegionInfo) {
	regionCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.Peers {
			regionCount[peer.StoreId]++
			if peer.Id == region.Leader.Id {
				leaderCount[peer.StoreId]++
				checkRegion(c, cache.leaders[peer.StoreId].Get(region.Id), region)
			} else {
				followerCount[peer.StoreId]++
				checkRegion(c, cache.followers[peer.StoreId].Get(region.Id), region)
			}
		}
	}

	c.Assert(cache.getRegionCount(), Equals, len(regions))
	for id, count := range regionCount {
		c.Assert(cache.getStoreRegionCount(id), Equals, count)
	}
	for id, count := range leaderCount {
		c.Assert(cache.getStoreLeaderCount(id), Equals, count)
	}
	for id, count := range followerCount {
		c.Assert(cache.getStoreFollowerCount(id), Equals, count)
	}

	for _, region := range cache.getRegions() {
		checkRegion(c, region, regions[region.GetId()])
	}
	for _, region := range cache.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].Region)
	}
}

var _ = Suite(&testClusterInfoSuite{})

type testClusterInfoSuite struct{}

func (s *testClusterInfoSuite) Test(c *C) {
	var tests []func(*C, *clusterInfo)
	tests = append(tests, s.testStoreHeartbeat)
	tests = append(tests, s.testRegionHeartbeat)
	tests = append(tests, s.testRegionSplitAndMerge)

	// Test without kv.
	{
		for _, test := range tests {
			cluster := newClusterInfo(newMockIDAllocator())
			test(c, cluster)
		}
	}

	// Test with kv.
	{
		for _, test := range tests {
			server, cleanup := mustRunTestServer(c)
			defer cleanup()
			cluster := newClusterInfo(server.idAlloc)
			cluster.kv = server.kv
			test(c, cluster)
		}
	}
}

func (s *testClusterInfoSuite) TestLoadClusterInfo(c *C) {
	server, cleanup := mustRunTestServer(c)
	defer cleanup()

	kv := server.kv

	// Cluster is not bootstrapped.
	cluster, err := loadClusterInfo(server.idAlloc, kv)
	c.Assert(err, IsNil)
	c.Assert(cluster, IsNil)

	// Save meta, stores and regions.
	n := 10
	meta := &metapb.Cluster{Id: 123}
	c.Assert(kv.saveMeta(meta), IsNil)
	stores := mustSaveStores(c, kv, n)
	regions := mustSaveRegions(c, kv, n)

	cluster, err = loadClusterInfo(server.idAlloc, kv)
	c.Assert(err, IsNil)
	c.Assert(cluster, NotNil)

	// Check meta, stores, and regions.
	c.Assert(cluster.getMeta(), DeepEquals, meta)
	c.Assert(cluster.getStoreCount(), Equals, n)
	for _, store := range cluster.getMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	c.Assert(cluster.getRegionCount(), Equals, n)
	for _, region := range cluster.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
}

func (s *testClusterInfoSuite) testStoreHeartbeat(c *C, cache *clusterInfo) {
	n, np := uint64(3), uint64(3)
	stores := newTestStores(n)
	regions := newTestRegions(n, np)

	for _, region := range regions {
		c.Assert(cache.putRegion(region), IsNil)
	}
	c.Assert(cache.getRegionCount(), Equals, int(n))

	for i, store := range stores {
		storeStats := &pdpb.StoreStats{StoreId: store.GetId()}
		c.Assert(cache.handleStoreHeartbeat(storeStats), NotNil)

		c.Assert(cache.putStore(store), IsNil)
		c.Assert(cache.getStoreCount(), Equals, int(i+1))

		stats := store.status
		c.Assert(stats.LastHeartbeatTS.IsZero(), IsTrue)

		c.Assert(cache.handleStoreHeartbeat(storeStats), IsNil)

		stats = cache.getStore(store.GetId()).status
		c.Assert(stats.LastHeartbeatTS.IsZero(), IsFalse)
	}

	c.Assert(cache.getStoreCount(), Equals, int(n))

	// Test with kv.
	if kv := cache.kv; kv != nil {
		for _, store := range stores {
			tmp := &metapb.Store{}
			ok, err := kv.loadStore(store.GetId(), tmp)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(tmp, DeepEquals, store.Store)
		}
	}
}

func (s *testClusterInfoSuite) testRegionHeartbeat(c *C, cache *clusterInfo) {
	n, np := uint64(3), uint64(3)

	stores := newTestStores(3)
	regions := newTestRegions(n, np)

	for _, store := range stores {
		cache.putStore(store)
	}

	for i, region := range regions {
		// region does not exist.
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// region is the same, not updated.
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		epoch := region.clone().GetRegionEpoch()

		// region is updated.
		region.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
		}
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// region is stale (Version).
		stale := region.clone()
		stale.RegionEpoch = &metapb.RegionEpoch{
			ConfVer: epoch.GetConfVer() + 1,
		}
		c.Assert(cache.handleRegionHeartbeat(stale), NotNil)
		checkRegions(c, cache.regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// region is updated.
		region.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
			ConfVer: epoch.GetConfVer() + 1,
		}
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// region is stale (ConfVer).
		stale = region.clone()
		stale.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
		}
		c.Assert(cache.handleRegionHeartbeat(stale), NotNil)
		checkRegions(c, cache.regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// Add a down peer.
		region.DownPeers = []*pdpb.PeerStats{
			{
				Peer:        region.Peers[rand.Intn(len(region.Peers))],
				DownSeconds: 42,
			},
		}
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.regions, regions[:i+1])

		// Add a pending peer.
		region.PendingPeers = []*metapb.Peer{region.Peers[rand.Intn(len(region.Peers))]}
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.regions, regions[:i+1])

		// Clear down peers.
		region.DownPeers = nil
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.regions, regions[:i+1])

		// Clear pending peers.
		region.PendingPeers = nil
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.regions, regions[:i+1])
	}

	regionCounts := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCounts[peer.GetStoreId()]++
		}
	}
	for id, count := range regionCounts {
		c.Assert(cache.getStoreRegionCount(id), Equals, count)
	}

	for _, region := range cache.getRegions() {
		checkRegion(c, region, regions[region.GetId()])
	}
	for _, region := range cache.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].Region)
	}

	for _, region := range regions {
		for _, store := range cache.getRegionStores(region) {
			c.Assert(region.GetStorePeer(store.GetId()), NotNil)
		}
		for _, store := range cache.getFollowerStores(region) {
			peer := region.GetStorePeer(store.GetId())
			c.Assert(peer.GetId(), Not(Equals), region.Leader.GetId())
		}
	}

	for _, store := range cache.stores.getStores() {
		c.Assert(store.status.LeaderCount, Equals, cache.regions.getStoreLeaderCount(store.GetId()))
		c.Assert(store.status.RegionCount, Equals, cache.regions.getStoreRegionCount(store.GetId()))
	}

	// Test with kv.
	if kv := cache.kv; kv != nil {
		for _, region := range regions {
			tmp := &metapb.Region{}
			ok, err := kv.loadRegion(region.GetId(), tmp)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(tmp, DeepEquals, region.Region)
		}
	}
}

func heartbeatRegions(c *C, cache *clusterInfo, regions []*metapb.Region) {
	// Heartbeat and check region one by one.
	for _, region := range regions {
		r := newRegionInfo(region, nil)

		c.Assert(cache.handleRegionHeartbeat(r), IsNil)

		checkRegion(c, cache.getRegion(r.GetId()), r)
		checkRegion(c, cache.searchRegion(r.StartKey), r)

		if len(r.EndKey) > 0 {
			end := r.EndKey[0]
			checkRegion(c, cache.searchRegion([]byte{end - 1}), r)
		}
	}

	// Check all regions after handling all heartbeats.
	for _, region := range regions {
		r := newRegionInfo(region, nil)

		checkRegion(c, cache.getRegion(r.GetId()), r)
		checkRegion(c, cache.searchRegion(r.StartKey), r)

		if len(r.EndKey) > 0 {
			end := r.EndKey[0]
			checkRegion(c, cache.searchRegion([]byte{end - 1}), r)
			result := cache.searchRegion([]byte{end + 1})
			c.Assert(result.GetId(), Not(Equals), r.GetId())
		}
	}
}

func (s *testClusterInfoSuite) testRegionSplitAndMerge(c *C, cache *clusterInfo) {
	regions := []*metapb.Region{
		{
			Id:          1,
			StartKey:    []byte{},
			EndKey:      []byte{},
			RegionEpoch: &metapb.RegionEpoch{},
		},
	}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		regions = splitRegions(regions)
		heartbeatRegions(c, cache, regions)
	}

	// Merge.
	for i := 0; i < n; i++ {
		regions = mergeRegions(regions)
		heartbeatRegions(c, cache, regions)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			regions = mergeRegions(regions)
		} else {
			regions = splitRegions(regions)
		}
		heartbeatRegions(c, cache, regions)
	}
}

var _ = Suite(&testClusterUtilSuite{})

type testClusterUtilSuite struct{}

func (s *testClusterUtilSuite) TestCheckStaleRegion(c *C) {
	// (0, 0) v.s. (0, 0)
	region := newRegion([]byte{}, []byte{})
	origin := newRegion([]byte{}, []byte{})
	c.Assert(checkStaleRegion(region, origin), IsNil)
	c.Assert(checkStaleRegion(origin, region), IsNil)

	// (1, 0) v.s. (0, 0)
	region.RegionEpoch.Version++
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)

	// (1, 1) v.s. (0, 0)
	region.RegionEpoch.ConfVer++
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)

	// (0, 1) v.s. (0, 0)
	region.RegionEpoch.Version--
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)
}

// mockIDAllocator mocks IDAllocator and it is only used for test.
type mockIDAllocator struct {
	base uint64
}

func newMockIDAllocator() *mockIDAllocator {
	return &mockIDAllocator{base: 0}
}

func (alloc *mockIDAllocator) Alloc() (uint64, error) {
	return atomic.AddUint64(&alloc.base, 1), nil
}

var _ = Suite(&testRegionMapSuite{})

type testRegionMapSuite struct{}

func (s *testRegionMapSuite) TestRegionMap(c *C) {
	var empty *regionMap
	c.Assert(empty.Len(), Equals, 0)
	c.Assert(empty.Get(1), IsNil)

	rm := newRegionMap()
	s.check(c, rm)
	rm.Put(s.regionInfo(1))
	s.check(c, rm, 1)

	rm.Put(s.regionInfo(2))
	rm.Put(s.regionInfo(3))
	s.check(c, rm, 1, 2, 3)

	rm.Put(s.regionInfo(3))
	rm.Delete(4)
	s.check(c, rm, 1, 2, 3)

	rm.Delete(3)
	rm.Delete(1)
	s.check(c, rm, 2)

	rm.Put(s.regionInfo(3))
	s.check(c, rm, 2, 3)
}

func (s *testRegionMapSuite) regionInfo(id uint64) *RegionInfo {
	return &RegionInfo{
		Region: &metapb.Region{
			Id: id,
		},
	}
}

func (s *testRegionMapSuite) check(c *C, rm *regionMap, ids ...uint64) {
	// Check position.
	for _, r := range rm.m {
		c.Assert(rm.ids[r.pos], Equals, r.Id)
	}
	// Check Get.
	for _, id := range ids {
		c.Assert(rm.Get(id).Id, Equals, id)
	}
	// Check Len.
	c.Assert(rm.Len(), Equals, len(ids))
	// Check id set.
	expect := make(map[uint64]struct{})
	for _, id := range ids {
		expect[id] = struct{}{}
	}
	set1 := make(map[uint64]struct{})
	for _, r := range rm.m {
		set1[r.Id] = struct{}{}
	}
	set2 := make(map[uint64]struct{})
	for _, id := range rm.ids {
		set2[id] = struct{}{}
	}
	c.Assert(set1, DeepEquals, expect)
	c.Assert(set2, DeepEquals, expect)
}
