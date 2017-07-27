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

package server

import (
	"fmt"
	"net/http"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/scheduler"

	restful "github.com/emicklei/go-restful"
	schedulerapiv1 "k8s.io/kubernetes/plugin/pkg/scheduler/api/v1"
)

var (
	errFailToRead  = restful.NewError(http.StatusBadRequest, "unable to read request body")
	errFailToWrite = restful.NewError(http.StatusInternalServerError, "unable to write response")
)

type server struct {
	scheduler scheduler.Scheduler
}

// StartServer start a kubernetes scheduler extender http apiserver
func StartServer(kubeconfig string, port int) {
	s, err := scheduler.NewSimpleScheduler(kubeconfig)
	if err != nil {
		log.Fatalf("failed to initialize tidb-scheduler: %v", err)
	}
	svr := &server{scheduler: s}

	ws := new(restful.WebService)
	ws.
		Path("/scheduler").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	ws.Route(ws.POST("/filter").To(svr.filterNode).
		Doc("filter nodes").
		Operation("filterNodes").
		Writes(schedulerapiv1.ExtenderFilterResult{}))

	ws.Route(ws.POST("/prioritize").To(svr.prioritizeNode).
		Doc("prioritize nodes").
		Operation("prioritizeNodes").
		Writes(schedulerapiv1.HostPriorityList{}))
	restful.Add(ws)

	log.Infof("start scheduler extender server, listening on 0.0.0.0:%d", port)
	err = http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		panic(err)
	}
}

func (svr *server) filterNode(req *restful.Request, resp *restful.Response) {
	args := &schedulerapiv1.ExtenderArgs{}
	if err := req.ReadEntity(args); err != nil {
		errorResponse(resp, errFailToRead)
		return
	}

	filterResult, err := svr.scheduler.Filter(args)
	if err != nil {
		errorResponse(resp, restful.NewError(http.StatusInternalServerError,
			fmt.Sprintf("unable to filter nodes: %v", err)))
		return
	}

	if err := resp.WriteEntity(filterResult); err != nil {
		errorResponse(resp, errFailToWrite)
	}
}

func (svr *server) prioritizeNode(req *restful.Request, resp *restful.Response) {
	args := &schedulerapiv1.ExtenderArgs{}
	if err := req.ReadEntity(args); err != nil {
		errorResponse(resp, errFailToRead)
		return
	}

	priorityResult, err := svr.scheduler.Priority(args)
	if err != nil {
		errorResponse(resp, restful.NewError(http.StatusInternalServerError,
			fmt.Sprintf("unable to priority nodes: %v", err)))
		return
	}

	if err := resp.WriteEntity(priorityResult); err != nil {
		errorResponse(resp, errFailToWrite)
	}
}

func errorResponse(resp *restful.Response, err restful.ServiceError) {
	log.Error(err.Message)
	if err := resp.WriteServiceError(err.Code, err); err != nil {
		log.Errorf("unable to write error: %v", err)
	}
}
