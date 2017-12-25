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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	pdClient = &http.Client{}
	PdAddr   = ""

	regionsPrefix    = "pd/api/v1/regions"
	regionIDPrefix   = "pd/api/v1/region/id"
	schedulersPrefix = "pd/api/v1/schedulers"
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
