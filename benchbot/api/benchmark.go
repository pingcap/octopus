package api

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	log "github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/backend"
)

const (
	InvalidMsgJobID         = "Invalid job id"
	defHistoryShowCount int = 10
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
		log.Errorf("invalid json data - %s", data)
		hdl.rdr.JSON(w, http.StatusBadRequest, fmt.Sprintf("post data json incorrect : %s", err.Error()))
		return
	}

	if !job.Valid() {
		hdl.rdr.JSON(w, http.StatusBadRequest, "not an valid request, params required !")
		return
	}

	if err = hdl.svr.AddJob(job); err != nil {
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
	his := hdl.svr.GetHistoryJobs(defHistoryShowCount)

	// hisCount := len(objHis)
	// strHis := make([]string, 0, hisCount)
	// for i := 0; i < len(objHis); i++ {
	// 	strHis = append(strHis, objHis[i].DumpToJson())
	// }

	hdl.rdr.JSON(w, http.StatusOK, his)
	return
}

func (hdl *BenchmarkHandler) QueryJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := ParseInt64(mux.Vars(r)["id"])
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

	success := hdl.svr.AbortJob(jobID, note)
	if !success {
		hdl.rdr.JSON(w, http.StatusBadRequest, fmt.Sprintf("cannot abort : %s", FormatInt64(jobID)))
		return
	}

	log.Infof("bench job aborted : id = %d \n", jobID)

	hdl.rdr.JSON(w, http.StatusOK, "ok")
	return
}

/*
func (hdl *BenchmarkHandler) QueryResult(w http.ResponseWriter, r *http.Request) {
	jobID, err := ParseInt64(mux.Vars(r)["id"])
	if err != nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, InvalidMsgJobID)
		return
	}

	job := hdl.svr.GetJob(jobID)
	if job == nil {
		hdl.rdr.JSON(w, http.StatusBadRequest, fmt.Sprintf("job not found : %s", FormatInt64(jobID)))
		return
	}

	hdl.rdr.JSON(w, http.StatusOK, job.ExportResultInfo())
	return
}
*/
