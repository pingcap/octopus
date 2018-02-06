package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/unrolled/render"
	"io"
	"io/ioutil"
	"net/http"
)

//ReadJSON for HTTP
func ReadJSON(hr io.ReadCloser, data interface{}) error {
	defer hr.Close()

	b, err := ioutil.ReadAll(hr)
	if err != nil {
		return errors.Trace(err)
	}
	err = json.Unmarshal(b, data)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

//BuildCallback for callback
func (r *Run) BuildCallback(w http.ResponseWriter, hr *http.Request) {
	btID := mux.Vars(hr)["btID"]
	repo := mux.Vars(hr)["repo"]
	if btID == "" || repo == "" {
		log.Errorf("access url error btID [%s] repo [%s]", btID, repo)
		r.Rdr.Text(w, http.StatusBadRequest, fmt.Sprintf("bad request btID [%s] repo [%s]", btID, repo))
	}
	bt, err := r.GetBenchTestJobByJobID(btID)
	if err != nil {
		r.Rdr.Text(w, http.StatusBadRequest, fmt.Sprintf("not found bench id [%s]", btID))
		return
	}
	log.Infof("get callback info benchid %s and repo %s", btID, repo)

	b := r.InitBuildJob()
	errR := ReadJSON(hr.Body, b)
	if errR != nil {
		log.Errorf("can not read httpPost data body with error %v", err)
		r.Rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	errU := r.UpdateBuildJob(bt, b)
	if errU != nil {
		r.Rdr.Text(w, http.StatusBadRequest, errU.Error())
		return
	}
	r.Rdr.Text(w, http.StatusOK, "ok")
}

//BenchTestCallback for bench test callback
func (r *Run) BenchTestCallback(w http.ResponseWriter, hr *http.Request) {
	btID := mux.Vars(hr)["btID"]
	log.Infof("get benchbot callback id %s", btID)
	bt, err := r.GetBenchTestJobByJobID(btID)
	if err != nil {
		r.Rdr.Text(w, http.StatusBadRequest, fmt.Sprintf("not found bench id [%s]", btID))
		return
	}
	bt.CallBackFlag = true
	r.Rdr.Text(w, http.StatusOK, "ok")
}

//SlackBotBenchTest for bench test
func (r *Run) SlackBotBenchTest(w http.ResponseWriter, hr *http.Request) {
	bt := r.InitBenchTestJob()

	err := ReadJSON(hr.Body, bt.BenchBotJob.Meta)
	if err != nil {
		log.Errorf("can not read httpPost data body with error %v", err)
		r.Rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Infof("get information %v", bt)

	for _, rDetail := range bt.BenchBotJob.Meta.Packages {
		if rDetail.Repo != "" && (rDetail.Branch != "" || rDetail.Tag != "" || rDetail.GitHash != "") {
			bj := r.InitBuildJob()
			bj.Infomation = rDetail
			bj.Callback = fmt.Sprintf("%s/%s/%s", BuildCallbackBaseURL, bt.JobID, bj.Infomation.Repo)

			bjID, err := r.CreateBuildJob(bj)
			if err != nil {
				r.Rdr.JSON(w, http.StatusBadRequest, err.Error())
				return
			}
			bt.BuildJobs[bjID] = bj
		}
	}

	if len(bt.BuildJobs) != 3 {
		log.Errorf("can not read httpPost data body with error %v", err)
		r.Rdr.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	bt.BenchBotJob.Meta.HttpCallback = fmt.Sprintf("%s%s%s", CallbackHTTPURL, BenchCallbackBaseURL, bt.JobID)
	errA := r.AddBenchTestJob(bt)
	if errA != nil {
		log.Errorf("can not add bench job from http api with error %v", errA)
		r.Rdr.Text(w, http.StatusInternalServerError, errA.Error())
		return
	}

	r.Rdr.Text(w, http.StatusOK, "ok")

}

//AllBenchTest get bench test info
func (r *Run) AllBenchTest(w http.ResponseWriter, hr *http.Request) {
	var btList []*BenchTestJob
	for i := 0; i < len(r.BenchTestJobs); i++ {
		bt := <-r.BenchTestJobs
		r.BenchTestJobs <- bt
		btList = append(btList, bt)
	}
	r.Rdr.JSON(w, http.StatusOK, btList)
}

//ForTest for test
func (r *Run) ForTest(w http.ResponseWriter, hr *http.Request) {
	bt := r.InitBenchTestJob()
	bt.BenchBotID = 245
	err := r.HTTPGet(fmt.Sprintf("%s%s%d", BenchTestBaseURL, JobDetailURL, bt.BenchBotID), bt.BenchBotJob)
	if err != nil {
		log.Errorf("can not http get bench test id %d detail with error %v", bt.BenchBotID, err)
	}
	r.AddBenchTestJob(bt)
	oldBtj, _ := r.AchieveBenchTestJobByBenchID(bt.BenchBotID - 1)
	go func() {
		defer func() {
			bt.SlackOutFlag = true
		}()
		r.BenchTestOutMsg(bt.BenchBotJob, oldBtj)
	}()

	r.Rdr.Text(w, http.StatusOK, "ok")

}

// CreateRouter create router
func (r *Run) CreateRouter() *mux.Router {
	buildURL := fmt.Sprintf("%s/%s/%s", BuildCallbackBaseURL, "{btID}", "{repo}")
	benchURL := fmt.Sprintf("%s/%s", BenchCallbackBaseURL, "{btID}")

	m := mux.NewRouter()
	m.HandleFunc(SlackBotBenchTestURL, r.SlackBotBenchTest).Methods("POST")
	m.HandleFunc(buildURL, r.BuildCallback).Methods("POST")
	m.HandleFunc(benchURL, r.BenchTestCallback).Methods("GET", "POST", "PUT")
	m.HandleFunc("/allbenchtest", r.AllBenchTest).Methods("GET")
	m.HandleFunc("/fortest", r.ForTest).Methods("GET")
	return m

}

// CreateRender for render
func (r *Run) CreateRender() {
	r.Rdr = render.New(render.Options{
		IndentJSON: true,
	})
}
