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

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

var (
	tikvClient tikvpb.TikvClient
	conn       *grpc.ClientConn
)

func InitTikvClient(addr string) error {
	var err error
	conn, err = grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	tikvClient = tikvpb.NewTikvClient(conn)
	return nil
}

func SplitRegion(region *RegionInfo) (uint64, uint64, error) {
	splitKey := []byte{}
	fmt.Sscanf("\""+region.StartKey+"\"", "%q", &splitKey)
	splitKey[len(splitKey)-1] = splitKey[len(splitKey)-1] + 1
	_, key, err := decodeBytes(splitKey)
	if err != nil {
		return 0, 0, err
	}

	req := &kvrpcpb.SplitRegionRequest{
		Context: &kvrpcpb.Context{
			RegionId:    region.ID,
			RegionEpoch: region.RegionEpoch,
			Peer:        region.Leader,
		},
		SplitKey: key,
	}

	response, err := tikvClient.SplitRegion(context.Background(), req)
	if err != nil {
		return 0, 0, err
	}

	regionErr := response.GetRegionError()
	if regionErr != nil {
		return 0, 0, fmt.Errorf("failed to split region: %v", regionErr.GetMessage())
	}

	return response.Left.Id, response.Right.Id, nil
}
