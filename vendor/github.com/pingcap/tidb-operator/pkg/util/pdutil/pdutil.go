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

package pdutil

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/typeutil"
	"github.com/pingcap/pd/server"
)

var (
	membersPrefix = "pd/api/v1/members"
	storesPrefix  = "pd/api/v1/stores"
	storePrefix   = "pd/api/v1/store"
	configPrefix  = "pd/api/v1/config"
)

// following struct definitions are copied from github.com/pingcap/pd/server/api/store
// these are not exported by that package

// MetaStore is TiKV store status defined in protobuf
type MetaStore struct {
	*metapb.Store
	StateName string `json:"state_name"`
}

// StoreStatus is TiKV store status returned from PD RESTful interface
type StoreStatus struct {
	StoreID            uint64            `json:"store_id"`
	Capacity           typeutil.ByteSize `json:"capacity"`
	Available          typeutil.ByteSize `json:"available"`
	LeaderCount        int               `json:"leader_count"`
	RegionCount        int               `json:"region_count"`
	SendingSnapCount   uint32            `json:"sending_snap_count"`
	ReceivingSnapCount uint32            `json:"receiving_snap_count"`
	ApplyingSnapCount  uint32            `json:"applying_snap_count"`
	IsBusy             bool              `json:"is_busy"`

	StartTS         time.Time         `json:"start_ts"`
	LastHeartbeatTS time.Time         `json:"last_heartbeat_ts"`
	Uptime          typeutil.Duration `json:"uptime"`
}

// StoreInfo is a single store info returned from PD RESTful interface
type StoreInfo struct {
	Store  *MetaStore   `json:"store"`
	Status *StoreStatus `json:"status"`
}

// StoresInfo is stores info returned from PD RESTful interface
type StoresInfo struct {
	Count  int          `json:"count"`
	Stores []*StoreInfo `json:"stores"`
}

// Members is PD members info returned from PD RESTful interface
type Members map[string][]*pdpb.Member

// Client provides infomation of pd client
type Client struct {
	url        string
	httpClient *http.Client
}

// New returns a Client
func New(url string, timeout time.Duration) *Client {
	return &Client{
		url:        url,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// GetConfig returns PD's config
func (cli *Client) GetConfig() (*server.Config, error) {
	apiURL := fmt.Sprintf("%s/%s", cli.url, configPrefix)
	res, err := cli.httpClient.Get(apiURL)
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}
	config := &server.Config{}
	err = json.Unmarshal(body, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// GetMembers returns all PD members from cluster
func (cli *Client) GetMembers() (Members, error) {
	apiURL := fmt.Sprintf("%s/%s", cli.url, membersPrefix)
	res, err := cli.httpClient.Get(apiURL)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}
	members := &Members{}
	err = json.Unmarshal(body, members)
	if err != nil {
		return nil, err
	}
	return *members, nil
}

// GetStores lists all TiKV stores from cluster
func (cli *Client) GetStores() (*StoresInfo, error) {
	apiURL := fmt.Sprintf("%s/%s", cli.url, storesPrefix)
	res, err := cli.httpClient.Get(apiURL)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}
	storesInfo := &StoresInfo{}
	err = json.Unmarshal(body, storesInfo)
	if err != nil {
		return nil, err
	}
	return storesInfo, nil
}

// GetTombStone lists all tombstone stores from cluster
func (cli *Client) GetTombStoneStores() (*StoresInfo, error) {
	apiURL := fmt.Sprintf("%s/%s?state=2", cli.url, storesPrefix)
	res, err := cli.httpClient.Get(apiURL)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}
	storesInfo := &StoresInfo{}
	err = json.Unmarshal(body, storesInfo)
	if err != nil {
		return nil, err
	}
	return storesInfo, nil
}

// GetStore gets a TiKV store for a specific store id from cluster
func (cli *Client) GetStore(id uint64) (*StoreInfo, error) {
	apiURL := fmt.Sprintf("%s/%s/%d", cli.url, storePrefix, id)
	res, err := cli.httpClient.Get(apiURL)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}
	storeInfo := &StoreInfo{}
	err = json.Unmarshal(body, storeInfo)
	if err != nil {
		return nil, err
	}
	return storeInfo, nil
}

// DeleteStore deletes a TiKV store from cluster
func (cli *Client) DeleteStore(id uint64) error {
	apiURL := fmt.Sprintf("%s/%s/%d", cli.url, storePrefix, id)
	req, err := http.NewRequest("DELETE", apiURL, nil)
	if err != nil {
		return err
	}
	res, err := cli.httpClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode == 200 {
		return nil
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return err
	}
	return fmt.Errorf("failed to delete store %d: %v", id, string(body))
}

// DeleteMember delete a PD member from cluster
func (cli *Client) DeleteMember(name string) error {
	apiURL := fmt.Sprintf("%s/%s/%s", cli.url, membersPrefix, name)
	req, err := http.NewRequest("DELETE", apiURL, nil)
	if err != nil {
		return err
	}
	res, err := cli.httpClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode == 200 {
		return nil
	}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return err
	}
	return fmt.Errorf("failed to delete member %s: %v", name, string(body))
}
