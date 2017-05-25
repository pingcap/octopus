package api

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	. "github.com/pingcap/octopus/benchbot/backend"
)

const (
	InvalidMsgJobID = "Invalid job id"
)

type BenchmarkHandler struct {
	rdr *render.Render
	svr *Server
}

func newBenchmarkHandler(svr *Server, rdr *render.Render) *BenchmarkHandler {
	hdl := &BenchmarkHandler{
		rdr: rdr,
		svr: svr,
	}

	return hdl
}

func (hdl *BenchmarkHandler) Plan(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, "invalid post data !")
		return
	}

	job := NewBenchmarkJob(hdl.svr)
	if err = job.ParseFromRequstJSON(data); err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, fmt.Sprintf("post data json incorrect : %s", err.Error()))
		return
	}

	if !job.Valid() {
		hdl.rdr.JSON(w, http.StatusBadRequest, "not an valid request, params required !")
		return
	}

	hdl.svr.ApplyBenchmark(job)

	resp := map[string]interface{}{"id": job.ID}
	hdl.rdr.JSON(w, http.StatusOK, resp)
	return
}

func (hdl *BenchmarkHandler) PlanRelease(w http.ResponseWriter, r *http.Request) {
	// TODO
	hdl.rdr.JSON(w, http.StatusBadRequest, "not supported yet !")
	return
}

func (hdl *BenchmarkHandler) GetGlobalStatus(w http.ResponseWriter, r *http.Request) {
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

func (hdl *BenchmarkHandler) ShowHistory(w http.ResponseWriter, r *http.Request) {
	his := hdl.svr.GetHistoryJobs(10)
	hdl.rdr.JSON(w, http.StatusOK, his)
	return
}

func (hdl *BenchmarkHandler) QueryJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := Numeric(mux.Vars(r)["jobid"])
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, InvalidMsgJobID)
		return
	}

	if job := hdl.svr.GetJob(jobID); job != nil {
		hdl.rdr.JSON(w, http.StatusOK, job)
	}

	return
}

func (hdl *BenchmarkHandler) AbortJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := Numeric(mux.Vars(r)["jobid"])
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, InvalidMsgJobID)
		return
	}

	note := ""
	params := make(map[string]string)
	ReadJson(r.Body, &params)
	if len(params) > 0 {
		if v, ok := params["note"]; ok {
			note = v
		}
	}

	success := hdl.svr.AbortBenchmark(jobID, note)
	if !success {
		hdl.rdr.JSON(w, http.StatusBadRequest, fmt.Sprintf("cannot abort : %s", Repr(jobID)))
		return
	}

	hdl.rdr.JSON(w, http.StatusOK, "ok")
	return
}

func (hdl *BenchmarkHandler) QueryResult(w http.ResponseWriter, r *http.Request) {
	jobID, err := Numeric(mux.Vars(r)["jobid"])
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, InvalidMsgJobID)
		return
	}

	job := hdl.svr.GetJob(jobID)
	if job == nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, fmt.Sprintf("job not found : %s", Repr(jobID)))
		return
	}

	hdl.rdr.JSON(w, http.StatusOK, job.ExportResultInfo())
	return
}
