package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	. "github.com/pingcap/octopus/benchbot/backend"
	. "github.com/pingcap/octopus/benchbot/common"
)

const (
	defaultListJobsSize = 30
)

type BasicInfo struct {
	ID         int64          `json:"id"`
	CreateTime string         `json:"created_time"`
	Status     string         `json:"status"`
	Meta       *BenchmarkMeta `json:"meta"`
}

func NewBasicInfo(b *BenchmarkJob) *BasicInfo {
	return &BasicInfo{
		ID:         b.ID,
		CreateTime: b.CreateTime,
		Status:     b.Status,
		Meta:       b.Meta,
	}
}

type BenchJobInfo struct {
	*BasicInfo
	Result *BenchmarkResult `json:"result"`
}

func NewBenchJobInfo(b *BenchmarkJob) *BenchJobInfo {
	return &BenchJobInfo{
		BasicInfo: NewBasicInfo(b),
		Result:    b.Result,
	}
}

type BenchmarkHandler struct {
	svr *Server
	rdr *render.Render
}

func newBenchmarkHandler(svr *Server, rdr *render.Render) *BenchmarkHandler {
	return &BenchmarkHandler{
		svr: svr,
		rdr: rdr,
	}
}

func (hdl *BenchmarkHandler) Plan(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	meta, err := NewBenchmarkMetaFromJSON(data)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	if !meta.Valid() {
		hdl.rdr.JSON(w, http.StatusBadRequest, "invalid benchmark meta")
		return
	}

	job, err := hdl.svr.AddJob(meta)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Infof("new bench job: id = %d / user = %s", job.ID, job.Meta.Creator)
	resp := map[string]interface{}{"id": job.ID}
	hdl.rdr.JSON(w, http.StatusOK, resp)
}

func (hdl *BenchmarkHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := hdl.svr.ListJobs(defaultListJobsSize)
	resp := make([]*BasicInfo, 0, len(jobs))
	for _, job := range jobs {
		resp = append(resp, NewBasicInfo(job))
	}
	hdl.rdr.JSON(w, http.StatusOK, resp)
}

func (hdl *BenchmarkHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	inDetail := len(r.URL.Query().Get("detail")) > 0

	jobID, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	job := hdl.svr.GetJob(jobID)
	if job == nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, fmt.Sprintf("[job-%d] not found", jobID))
		return
	}

	var resp interface{} = job
	if !inDetail {
		resp = NewBenchJobInfo(job)
	}
	hdl.rdr.JSON(w, http.StatusOK, resp)
}

func (hdl *BenchmarkHandler) AbortJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	note := ""
	params := make(map[string]string)
	ReadJSON(r.Body, &params)
	if len(params) > 0 {
		if v, ok := params["note"]; ok {
			note = v
		}
	}

	if err := hdl.svr.AbortJob(jobID, note); err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	hdl.rdr.JSON(w, http.StatusOK, "OK")
}

func (hdl *BenchmarkHandler) GetGlobalStatus(w http.ResponseWriter, r *http.Request) {
	status := hdl.svr.Status()
	resp := map[string]interface{}{
		"total_count":   status.TotalCount,
		"running_count": status.RunningCount,
		"pending_count": status.PendingCount,
	}
	hdl.rdr.JSON(w, http.StatusOK, resp)
}
