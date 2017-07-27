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

package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testStoreSuite{})

type testStoreSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
	stores    []*metapb.Store
}

func (s *testStoreSuite) SetUpSuite(c *C) {
	s.stores = []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:      1,
			Address: "localhost:1",
			State:   metapb.StoreState_Up,
		},
		{
			Id:      4,
			Address: "localhost:4",
			State:   metapb.StoreState_Up,
		},
		{
			// metapb.StoreState_Offline == 1
			Id:      6,
			Address: "localhost:6",
			State:   metapb.StoreState_Offline,
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:      7,
			Address: "localhost:7",
			State:   metapb.StoreState_Tombstone,
		},
	}

	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	httpAddr := mustUnixAddrToHTTPAddr(c, addr)
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", httpAddr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	for _, store := range s.stores {
		mustPutStore(c, s.svr, store)
	}
}

func (s *testStoreSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func checkStoresInfo(c *C, ss []*storeInfo, want []*metapb.Store) {
	c.Assert(len(ss), Equals, len(want))
	mapWant := make(map[uint64]*metapb.Store)
	for _, s := range want {
		if _, ok := mapWant[s.Id]; !ok {
			mapWant[s.Id] = s
		}
	}
	for _, s := range ss {
		c.Assert(s.Store.Store, DeepEquals, mapWant[s.Store.Store.Id])
	}
}

func (s *testStoreSuite) TestStoresList(c *C) {
	url := fmt.Sprintf("%s/stores", s.urlPrefix)
	info := new(storesInfo)
	err := readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[:3])

	url = fmt.Sprintf("%s/stores?state=0", s.urlPrefix)
	err = readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[:2])

	url = fmt.Sprintf("%s/stores?state=1", s.urlPrefix)
	err = readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, info.Stores, s.stores[2:3])

}

func (s *testStoreSuite) TestStoreGet(c *C) {
	url := fmt.Sprintf("%s/store/1", s.urlPrefix)
	info := new(storeInfo)
	err := readJSONWithURL(url, info)
	c.Assert(err, IsNil)
	checkStoresInfo(c, []*storeInfo{info}, s.stores[:1])
}

func (s *testStoreSuite) TestStoreDelete(c *C) {
	table := []struct {
		id     int
		status int
	}{
		{
			id:     6,
			status: http.StatusOK,
		},
		{
			id:     7,
			status: http.StatusInternalServerError,
		},
	}
	client := newUnixSocketClient()
	for _, t := range table {
		req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/store/%d", s.urlPrefix, t.id), nil)
		c.Assert(err, IsNil)
		resp, err := client.Do(req)
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, t.status)
	}
}

func (s *testStoreSuite) TestUrlStoreFilter(c *C) {
	table := []struct {
		u    string
		want []*metapb.Store
	}{
		{
			u:    "http://localhost:2379/pd/api/v1/stores",
			want: s.stores[:3],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2",
			want: s.stores[3:],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=0",
			want: s.stores[:2],
		},
		{
			u:    "http://localhost:2379/pd/api/v1/stores?state=2&state=1",
			want: s.stores[2:],
		},
	}

	for _, t := range table {
		uu, err := url.Parse(t.u)
		c.Assert(err, IsNil)
		f, err := newStoreStateFilter(uu)
		c.Assert(err, IsNil)
		c.Assert(f.filter(s.stores), DeepEquals, t.want)
	}

	u, err := url.Parse("http://localhost:2379/pd/api/v1/stores?state=foo")
	c.Assert(err, IsNil)
	_, err = newStoreStateFilter(u)
	c.Assert(err, NotNil)

	u, err = url.Parse("http://localhost:2379/pd/api/v1/stores?state=999999")
	c.Assert(err, IsNil)
	_, err = newStoreStateFilter(u)
	c.Assert(err, NotNil)
}

func (s *testStoreSuite) TestDownState(c *C) {
	status := &server.StoreStatus{
		StoreStats: &pdpb.StoreStats{},
	}
	store := &metapb.Store{
		State: metapb.StoreState_Up,
	}
	status.LastHeartbeatTS = time.Now()
	storeInfo := newStoreInfo(store, status)
	c.Assert(storeInfo.Store.StateName, Equals, metapb.StoreState_Up.String())

	status.LastHeartbeatTS = time.Now().Add(-time.Minute * 2)
	storeInfo = newStoreInfo(store, status)
	c.Assert(storeInfo.Store.StateName, Equals, downStateName)
}
