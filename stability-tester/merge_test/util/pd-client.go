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

package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	pdClient = &http.Client{}
	PdAddr   = ""

	regionsPrefix    = "pd/api/v1/regions"
	regionIDPrefix   = "pd/api/v1/region/id"
	storePrefix      = "pd/api/v1/store"
	schedulersPrefix = "pd/api/v1/schedulers"
	operatorsPrefix  = "pd/api/v1/operators"
)

type regionsInfo struct {
	Count   int           `json:"count"`
	Regions []*RegionInfo `json:"regions"`
}

type RegionInfo struct {
	ID          uint64              `json:"id"`
	StartKey    string              `json:"start_key"`
	EndKey      string              `json:"end_key"`
	RegionEpoch *metapb.RegionEpoch `json:"epoch,omitempty"`
	Peers       []*metapb.Peer      `json:"peers,omitempty"`

	Leader          *metapb.Peer      `json:"leader,omitempty"`
	DownPeers       []*pdpb.PeerStats `json:"down_peers,omitempty"`
	PendingPeers    []*metapb.Peer    `json:"pending_peers,omitempty"`
	WrittenBytes    uint64            `json:"written_bytes,omitempty"`
	ReadBytes       uint64            `json:"read_bytes,omitempty"`
	ApproximateSize int64             `json:"approximate_size,omitempty"`
}

type metaStore struct {
	*metapb.Store
	StateName string `json:"state_name"`
}
type StoreInfo struct {
	Store *metaStore `json:"store"`
	//Status *storeStatus `json:"status"`
}

func dail(req *http.Request) ([]byte, error) {
	reps, err := pdClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer reps.Body.Close()
	if reps.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("statuscode is not okay")
	}

	r, err := ioutil.ReadAll(reps.Body)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func postJSON(prefix string, input map[string]interface{}) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}

	url := getAddress(prefix)
	r, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		err := fmt.Errorf("statuscode is not okay: ")
		printResponseError(r)
		return err
	}

	return nil
}

func printResponseError(r *http.Response) {
	fmt.Printf("[%d]", r.StatusCode)
	io.Copy(os.Stdout, r.Body)
}

func doRequest(prefix string, method string) ([]byte, error) {
	req, err := getRequest(prefix, method, "", nil)
	if err != nil {
		return nil, err
	}
	return dail(req)
}

func getRequest(prefix string, method string, bodyType string, body io.Reader) (*http.Request, error) {
	if method == "" {
		method = http.MethodGet
	}
	url := getAddress(prefix)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)
	return req, err
}

func getAddress(prefix string) string {
	u, err := url.Parse(PdAddr)
	if err != nil {
		return err.Error()
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	s := fmt.Sprintf("%s/%s", u, prefix)
	return s
}

func GetRegionCount() (int, error) {
	prefix := regionsPrefix

	r, err := doRequest(prefix, http.MethodGet)
	if err != nil {
		return 0, fmt.Errorf("Failed to get region count: %s", err)
	}

	regions := &regionsInfo{}
	err = json.Unmarshal(r, regions)
	if err != nil {
		return 0, fmt.Errorf("Failed to get region count: %s", err)
	}
	return regions.Count, nil
}

func GetRegions() ([]*RegionInfo, error) {
	prefix := regionsPrefix

	r, err := doRequest(prefix, http.MethodGet)
	if err != nil {
		return nil, fmt.Errorf("Failed to get regions: %s", err)
	}

	regions := &regionsInfo{}
	err = json.Unmarshal(r, regions)
	if err != nil {
		return nil, fmt.Errorf("Failed to get regions: %s", err)
	}
	return regions.Regions, nil
}

func GetRegionInfo(regionID uint64) (*RegionInfo, error) {
	prefix := regionIDPrefix + "/" + strconv.Itoa(int(regionID))

	r, err := doRequest(prefix, http.MethodGet)
	if err != nil {
		return nil, fmt.Errorf("Failed to get region %v info: %s", regionID, err)
	}

	region := &RegionInfo{}
	err = json.Unmarshal(r, region)
	if err != nil {
		return nil, fmt.Errorf("Failed to get region %v info: %s", regionID, err)
	}
	return region, nil
}

func StartShuffleRegions() error {
	input := make(map[string]interface{})
	input["name"] = "shuffle-region-scheduler"
	return postJSON(schedulersPrefix, input)
}

func StopShuffleRegions() error {
	path := schedulersPrefix + "/shuffle-region-scheduler"
	_, err := doRequest(path, http.MethodDelete)
	return errors.Trace(err)
}

func GetStoreAddr(storeID uint64) (string, error) {
	prefix := storePrefix + "/" + strconv.Itoa(int(storeID))

	r, err := doRequest(prefix, http.MethodGet)
	if err != nil {
		return "", fmt.Errorf("Failed to get store %v addr: %s", storeID, err)
	}

	store := &StoreInfo{}
	err = json.Unmarshal(r, store)
	if err != nil {
		return "", fmt.Errorf("Failed to get store %v info: %s", storeID, err)
	}
	return store.Store.GetAddress(), nil
}

func MergeRegion(source, target *RegionInfo) error {
	input := make(map[string]interface{})
	input["name"] = "region-merge"
	input["source_region_id"] = source.ID
	input["merge_region_id"] = target.ID
	return postJSON(operatorsPrefix, input)
}
