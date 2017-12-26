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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

func IsSameLocation(one, other []*metapb.Peer) bool {
	set := make(map[uint64]struct{})

	for _, item := range one {
		set[item.StoreId] = struct{}{}
	}

	for _, item := range other {
		delete(set, item.StoreId)
	}

	return len(set) == 0
}

func CreateNewRegion(db *DB) (*RegionInfo, error) {
	initRegions, err := GetRegions()
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = db.CreateTable(1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	nowRegions, err := GetRegions()
	if err != nil {
		return nil, errors.Trace(err)
	}
	region, err := GetNewRegionInfo(initRegions, nowRegions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("new region is %d", region.ID)

	tableRegionInfo, err := FindNewTableRegion(region)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("new table region is %d", tableRegionInfo.ID)

	return tableRegionInfo, nil
}

func FindNewSplitRegion(regions []*RegionInfo, origin *RegionInfo) (*RegionInfo, error) {
	var find string

	for _, region := range regions {
		if region.ID == origin.ID {
			if region.StartKey != origin.StartKey {
				find = origin.StartKey
			} else if region.EndKey != origin.EndKey {
				find = origin.EndKey
			} else {
				return nil, fmt.Errorf("region %d doesn't split", origin.ID)
			}
		}
	}

	for _, item := range regions {
		if item.StartKey == find {
			region, err := GetRegionInfo(item.ID)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return region, nil
		}
	}

	return nil, fmt.Errorf("can not find split reigon")
}

func FindNewTableRegion(region *RegionInfo) (*RegionInfo, error) {
	regions, err := GetRegions()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, item := range regions {
		if item.StartKey == region.EndKey {
			region, err := GetRegionInfo(item.ID)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return region, nil
		}
	}

	return nil, fmt.Errorf("can not find new table reigon")
}

func GetNewRegionInfo(old, new []*RegionInfo) (*RegionInfo, error) {
	set := make(map[uint64]*RegionInfo)
	for _, n := range new {
		set[n.ID] = n
	}
	for _, o := range old {
		delete(set, o.ID)
	}

	if len(set) != 1 {
		return nil, fmt.Errorf("get %v new region, expect only one", len(set))
	}

	for _, s := range set {
		region, err := GetRegionInfo(s.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return region, nil
	}
	return nil, nil
}

func IsRegionIDExist(regions []*RegionInfo, id uint64) bool {
	for _, region := range regions {
		if region.ID == id {
			return true
		}
	}
	return false
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

func Clean() {
	conn.Close()
}

func checkRegionsTableID(tableID int64, one, other uint64) error {
	regions, err := GetRegions()
	if err != nil {
		return errors.Trace(err)
	}

	for _, region := range regions {
		if region.ID == one || region.ID == other {
			continue
		}
		key := Key{}
		fmt.Sscanf("\""+region.StartKey+"\"", "%q", &key)
		if key.TableID() == tableID {
			return fmt.Errorf("table id has changed")
		}
	}

	return nil
}

func GetTableID(region *RegionInfo) int64 {
	key := Key{}
	fmt.Sscanf("\""+region.StartKey+"\"", "%q", &key)
	return key.TableID()
}

func DiffTableRegions(prevRegions, nowRegions []*RegionInfo, tableID int64) (int, int) {
	add := 0

	set := make(map[uint64]struct{})

	for _, region := range prevRegions {
		id := GetTableID(region)
		if id == tableID {
			set[region.ID] = struct{}{}
		}
	}

	for _, region := range nowRegions {
		id := GetTableID(region)
		if _, ok := set[region.ID]; ok {
			delete(set, region.ID)
		} else {
			add++
		}
	}

	return add, len(set)
}
