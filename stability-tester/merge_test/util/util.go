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
	"fmt"
	"time"

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

	return len(set) == 0 && len(one) == len(other)
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

	time.Sleep(time.Second * 60)

	nowRegions, err := GetRegions()
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableRegionInfo, err := GetNewTableRegion(initRegions, nowRegions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("new table region is %d", tableRegionInfo.ID)

	return tableRegionInfo, nil
}

func FindNewSplitRegions(origin *RegionInfo) ([]*RegionInfo, error) {
	regions, err := GetRegions()
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableID := GetTableID(origin)
	var find []*RegionInfo

	for _, region := range regions {
		if GetTableID(region) == tableID {
			find = append(find, region)
		}
	}

	if len(find) == 0 {
		return nil, fmt.Errorf("region %d doesn't split", origin.ID)
	}
	log.Infof("table %d has %d regions", tableID, len(find))
	return find, nil
}

func GetNewTableRegion(old, now []*RegionInfo) (*RegionInfo, error) {
	set := make(map[int64]*RegionInfo)
	for _, n := range now {
		set[GetTableID(n)] = n
	}
	for _, o := range old {
		delete(set, GetTableID(o))
	}

	if len(set) != 1 {
		return nil, fmt.Errorf("get %v new table ids, expect only one", len(set))
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

func CheckRegionsTableID(tableID int64, tableRegions []*RegionInfo) ([]*RegionInfo, error) {
	regions, err := GetRegions()
	if err != nil {
		return tableRegions, errors.Trace(err)
	}

	set := make(map[uint64]struct{})
	for _, region := range tableRegions {
		set[region.ID] = struct{}{}
	}

	for _, region := range regions {
		if _, ok := set[region.ID]; ok {
			continue
		}
		if GetTableID(region) == tableID {
			tableRegions = append(tableRegions, region)
			log.Infof("insert new region %d for table %v", region.ID, tableID)
		}
	}

	return tableRegions, nil
}

func GetTableID(region *RegionInfo) int64 {
	key := Key{}
	fmt.Sscanf("\""+region.StartKey+"\"", "%q", &key)
	return key.TableID()
}
