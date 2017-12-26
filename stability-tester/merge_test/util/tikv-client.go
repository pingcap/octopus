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
	"context"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

func NewTikvClient(addr string) (tikvpb.TikvClient, *grpc.ClientConn, error) {
	var err error
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	tikvClient := tikvpb.NewTikvClient(conn)
	return tikvClient, conn, nil
}

func SplitRegion(region *RegionInfo) (uint64, uint64, error) {
	splitKey := []byte{}
	fmt.Sscanf("\""+region.StartKey+"\"", "%q", &splitKey)
	splitKey[len(splitKey)-1] = splitKey[len(splitKey)-1] + 1
	_, key, err := decodeBytes(splitKey)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	addr, err := GetStoreAddr(region.Leader.StoreId)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	client, conn, err := NewTikvClient(addr)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	defer conn.Close()

	req := &kvrpcpb.SplitRegionRequest{
		Context: &kvrpcpb.Context{
			RegionId:    region.ID,
			RegionEpoch: region.RegionEpoch,
			Peer:        region.Leader,
		},
		SplitKey: key,
	}

	response, err := client.SplitRegion(context.Background(), req)
	if err != nil {
		return 0, 0, err
	}

	regionErr := response.GetRegionError()
	if regionErr != nil {
		return 0, 0, fmt.Errorf("failed to split region: %v, peer is %v", regionErr.GetMessage(), region.Leader)
	}

	return response.Left.Id, response.Right.Id, nil
}
