package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/backend"
	. "github.com/pingcap/octopus/benchbot/suite"
)

const (
	InvalidMsgJobID         = "Invalid job id"
	defHistoryShowCount int = 10
)

// data format
type BasicInfo struct {
	ID         int64          `json:"id"`
	CreateTime string         `json:"created_time"`
	Status     string         `json:"status"`
	Meta       *BenchmarkMeta `json:"meta"`
}

type BenchJobInfo struct {
	BasicInfo
	Result *BenchmarkResult `json:"result"`
}

func packSummaryResult(bj *BenchmarkJob) *BenchJobInfo {
	emptySlice := make([]*StatIndex, 0)
	details := make([]*CaseResult, 0, len(bj.Result.Details))
	for _, r := range bj.Result.Details {
		details = append(details, &CaseResult{
			Name:    r.Name,
			Summary: r.Summary,
			Stages:  emptySlice,
		})
	}

	job := &BenchJobInfo{
		BasicInfo: BasicInfo{
			ID:         bj.ID,
			CreateTime: bj.CreateTime,
			Status:     bj.Status,
			Meta:       bj.Meta,
		},
		Result: &BenchmarkResult{
			Cases:   bj.Result.Cases,
			Message: bj.Result.Message,
			Details: details,
		},
	}

	return job
}

// handler
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

	meta, err := NewBenchmarkMetaFromJSON(data)
	if err != nil {
		log.Errorf("invalid json data - %s", data)
		hdl.rdr.JSON(w, http.StatusBadRequest, fmt.Sprintf("post data json incorrect : %s", err.Error()))
		return
	}

	if !meta.Valid() {
		hdl.rdr.JSON(w, http.StatusBadRequest, "not an valid request, params required !")
		return
	}

	job, err := hdl.svr.CreateJob(meta)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, "job creating failed !")
		return
	}

	log.Infof("new bench job : id = %d / user = %s", job.ID, job.Meta.Creator)

	resp := map[string]interface{}{"id": job.ID}
	hdl.rdr.JSON(w, http.StatusOK, resp)

	return
}

func (hdl *BenchmarkHandler) GetGlobalStatus(w http.ResponseWriter, r *http.Request) {
	summary := hdl.svr.DumpSummary()

	resp := map[string]interface{}{
		"running_count": summary.RunningCount,
		"pending_count": summary.PendingCount,
		"history_count": summary.HistoryCount,
	}
	hdl.rdr.JSON(w, http.StatusOK, resp)
	return
}

func (hdl *BenchmarkHandler) ShowHistory(w http.ResponseWriter, r *http.Request) {
	datas := hdl.svr.GetHistoryJobs(defHistoryShowCount)

	his := make([]*BasicInfo, 0, len(datas))
	for _, job := range datas {
		hisJob := &BasicInfo{
			ID:         job.ID,
			Status:     job.Status,
			CreateTime: job.CreateTime,
			Meta:       job.Meta,
		}
		his = append(his, hisJob)
	}

	hdl.rdr.JSON(w, http.StatusOK, his)
	return
}

func (hdl *BenchmarkHandler) QueryJob(w http.ResponseWriter, r *http.Request) {
	inDetail := len(r.URL.Query().Get("detail")) > 0
	jobID, err := ParseInt64(mux.Vars(r)["id"])
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, InvalidMsgJobID)
		return
	}

	job := hdl.svr.GetJob(jobID)
	if job == nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, InvalidMsgJobID)
		return
	}

	var resp interface{} = job
	if !inDetail {
		resp = packSummaryResult(job)
	}
	hdl.rdr.JSON(w, http.StatusOK, resp)

	return
}

func (hdl *BenchmarkHandler) GetJobs(w http.ResponseWriter, r *http.Request) {
	vals := strings.Split(mux.Vars(r)["ids"], ",")
	ids := make([]int, 0, len(vals))
	for _, v := range vals {
		if jobID, err := ParseInt64(v); err != nil {
			continue
		} else {
			ids = append(ids, int(jobID))
		}
	}

	sort.Ints(ids)

	jobs := make([]*BenchmarkJob, 0, len(ids))
	for _, jobID := range ids {
		if job := hdl.svr.GetJob(int64(jobID)); job != nil {
			jobs = append(jobs, job)
		}
	}

	hdl.rdr.JSON(w, http.StatusOK, jobs)
}

func (hdl *BenchmarkHandler) AbortJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := ParseInt64(mux.Vars(r)["id"])
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

	err = hdl.svr.AbortJob(jobID, note)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Infof("bench job aborted : id = %d \n", jobID)

	hdl.rdr.JSON(w, http.StatusOK, "ok")
	return
}
