// Copyright 2017 PingCAP, Inc.
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

package api

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testRegionSuite{})

type testRegionSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testRegionSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	httpAddr := mustUnixAddrToHTTPAddr(c, addr)
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", httpAddr, apiPrefix)

	mustBootstrapCluster(c, s.svr)

}

func (s *testRegionSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func newTestRegionInfo(regionID, storeID uint64, start, end []byte) *server.RegionInfo {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: storeID,
	}

	return &server.RegionInfo{
		Region: &metapb.Region{
			Id:       regionID,
			StartKey: start,
			EndKey:   end,
			Peers:    []*metapb.Peer{leader},
		},
		Leader:       leader,
		DownPeers:    make([]*pdpb.PeerStats, 0),
		PendingPeers: make([]*metapb.Peer, 0),
	}
}

func (s *testRegionSuite) TestRegion(c *C) {
	r := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	mustRegionHeartBeat(c, s.svr, r)
	url := fmt.Sprintf("%s/region/id/%d", s.urlPrefix, r.GetId())
	r1 := &server.RegionInfo{}
	err := readJSONWithURL(url, r1)
	c.Assert(err, IsNil)
	c.Assert(r1, DeepEquals, r)

	url = fmt.Sprintf("%s/region/key/%s", s.urlPrefix, "a")
	r2 := &server.RegionInfo{}
	err = readJSONWithURL(url, r2)
	c.Assert(err, IsNil)
	c.Assert(r2, DeepEquals, r)
}
