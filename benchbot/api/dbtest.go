package api

import (
	//"fmt"
	//"bytes"
	//"encoding/json"
	//"io"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	//"golang.org/x/net/context"
	. "github.com/pingcap/octopus/benchbot/backend"
	"github.com/pingcap/octopus/pkg/util"
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

func (hdl *TestingHandler) RunTest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, "invalid post data !")
		return
	}

	job := NewTestingJob()
	if err = job.ParseFromRequstJSON(data); err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, "post data json incorrect : "+err.Error())
		return
	}

	if !job.Valid() {
		hdl.rdr.JSON(w, http.StatusBadRequest, "not an valid request, params required !")
		return
	}

	hdl.svr.ApplyTesting(job)

	resp := map[string]string{"jobid": job.Id}
	hdl.rdr.JSON(w, http.StatusOK, resp)
	return
}

func (hdl *TestingHandler) RunReleaseTest(w http.ResponseWriter, r *http.Request) {
	// TODO
	hdl.rdr.JSON(w, http.StatusOK, nil)
	return
}

func (hdl *TestingHandler) GetGlobalStatus(w http.ResponseWriter, r *http.Request) {
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

func (hdl *TestingHandler) QueryTestJob(w http.ResponseWriter, r *http.Request) {
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

func (hdl *TestingHandler) AbortTestJob(w http.ResponseWriter, r *http.Request) {
	jobId := mux.Vars(r)["jobId"]
	if len(jobId) == 0 {
		hdl.rdr.JSON(w, http.StatusBadRequest, "jobId required !")
		return
	}

	note := ""
	params := make(map[string]string)
	util.ReadJson(r.Body, &params)
	if len(params) > 0 {
		if v, ok := params["note"]; ok {
			note = v
		}
	}

	success := hdl.svr.AbortTesting(jobId, note)
	if !success {
		hdl.rdr.JSON(w, http.StatusBadRequest, "cannot abort : "+jobId)
		return
	}

	hdl.rdr.JSON(w, http.StatusOK, "ok")
	return
}

func (hdl *TestingHandler) QueryTestJobResult(w http.ResponseWriter, r *http.Request) {
	jobId := mux.Vars(r)["jobId"]
	if len(jobId) == 0 {
		hdl.rdr.JSON(w, http.StatusBadRequest, "jobId required !")
		return
	}

	job := hdl.svr.GetJob(jobId)
	if job == nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, "job not found : "+jobId)
		return
	}

	hdl.rdr.JSON(w, http.StatusOK, job.ExportResultInfo())
	return
}
