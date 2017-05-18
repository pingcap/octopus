package api

import (
	"fmt"
	//"bytes"
	//"encoding/json"
	//"io"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	//"golang.org/x/net/context"
	. "github.com/pingcap/octopus/benchbot/backend"
	//. "github.com/pingcap/octopus/pkg/util"
)

type TestingHandler struct {
	rdr *render.Render
	svr *Server
}

func newTestingHandler(svr *Server, rdr *render.Render) *TestingHandler {
	hdl := &TestingHandler{
		rdr: rdr,
		svr: svr,
	}

	return hdl
}

func (hdl *TestingHandler) RunTesting(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, "invalid post data !")
		return
	}

	fmt.Printf("%s\n", string(data))

	job := NewTestJob()
	if err = job.ParseFromJSON(data); err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, "post data json incorrect : "+err.Error())
		return
	}

	if !job.Valid() {
		hdl.rdr.JSON(w, http.StatusBadRequest, "not an valid request, params required !")
		return
	}

	hdl.svr.LaunchTesting(job)

	resp := map[string]string{"job_id": job.Id}
	hdl.rdr.JSON(w, http.StatusOK, resp)
	return
}

func (hdl *TestingHandler) RunReleaseTesting(w http.ResponseWriter, r *http.Request) {
	// TODO
	hdl.rdr.JSON(w, http.StatusOK, nil)
	return
}

func (hdl *TestingHandler) GetGlobalInfo(w http.ResponseWriter, r *http.Request) {
	var info SummaryInfo
	hdl.svr.DumpInfo(&info)

	resp := map[string]interface{}{
		"running_count": info.RunningCount,
		"pending_count": info.PendingCount,
		"history_count": info.HistoryCount,
	}
	hdl.rdr.JSON(w, http.StatusOK, resp)
	return
}

func (hdl *TestingHandler) ShowHistory(w http.ResponseWriter, r *http.Request) {
	his := hdl.svr.GetHistoryJobs(10)
	hdl.rdr.JSON(w, http.StatusOK, his)
	return
}

func (hdl *TestingHandler) QueryTestingJob(w http.ResponseWriter, r *http.Request) {
	jobId := mux.Vars(r)["jobId"]
	if len(jobId) == 0 {
		hdl.rdr.JSON(w, http.StatusBadRequest, "jobId required !")
		return
	}

	if job := hdl.svr.GetJob(jobId); job != nil {
		hdl.rdr.JSON(w, http.StatusOK, job)
	}

	return
}

func (hdl *TestingHandler) AbortTestingJob(w http.ResponseWriter, r *http.Request) {
	jobId := mux.Vars(r)["jobId"]
	if len(jobId) == 0 {
		hdl.rdr.JSON(w, http.StatusBadRequest, "jobId required !")
		return
	}

	success := hdl.svr.AbortTesting(jobId)
	if !success {
		hdl.rdr.JSON(w, http.StatusBadRequest, "cannot abort : "+jobId)
		return
	}

	hdl.rdr.JSON(w, http.StatusOK, "ok")
	return
}
