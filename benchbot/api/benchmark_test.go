package api

import (
	"errors"
	"strings"
	"testing"

	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	. "github.com/pingcap/check"
	. "github.com/pingcap/octopus/benchbot/backend"
)

var _ = Suite(&testBenchSuite{})

type testBenchSuite struct{}

type Job struct {
	ID int64 `json:"id"`
}

func (job *Job) parseFromJson(resp *http.Response) error {
	data, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(data, job)
	return err
}

func setup() *httptest.Server {
	cfg := new(ServerConfig)
	cfg.Ansible.Clusters = make([]ClusterDSN, 0) // ps : not any cluster, just testing api.

	svr, _ := NewServer(cfg)
	router := CreateRouter(svr)
	return httptest.NewServer(router)
}

func teardown(s *httptest.Server) {
	s.Close()
}

func loadsJson(resp *http.Response) map[string]interface{} {
	body, _ := ioutil.ReadAll(resp.Body)
	data := make(map[string]interface{})
	json.Unmarshal(body, &data)
	return data
}

func requestNewBenchmark(httpSvr *httptest.Server) (int64, error) {
	var failure error = nil
	var jobID int64

	reqJson := `{
		"user": "pingcap", "note": "simple note msg", "upstream": "unit_test", "callback": "",
	    "packages": [ 
		   {"repo": "tidb", "branch": "tidb/1", "git_hash": "0x10", "platform": "centos",
		    "binary_url": "127.0.0.1:7777/test.tar.gz"},
		   {"repo": "tikv", "branch": "tikv/1", "git_hash": "0x20", "platform": "centos",
		    "binary_url": "127.0.0.1:7777/test.tar.gz"},
		   {"repo": "pd", "branch": "pd/1", "git_hash": "0x30", "platform": "centos",
		    "binary_url": "127.0.0.1:7777/test.tar.gz"}
		]
	}`

	reader := strings.NewReader(reqJson)
	request, err := http.NewRequest("POST", httpSvr.URL+"/bench/plan", reader)

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return -1, err
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		failure = errors.New("new benching plan failed : " + string(errMsg))
	} else {
		job := new(Job)
		job.parseFromJson(resp)
		jobID = job.ID
	}

	return jobID, failure
}

func TestNewTask(t *testing.T) {
	testServer := setup()
	defer teardown(testServer)

	if jobID, err := requestNewBenchmark(testServer); err != nil {
		t.Errorf("%s", err.Error())
	} else if jobID < 0 {
		t.Errorf("job id invalid !")
	}
}

func TestShowSummary(t *testing.T) {
	testServer := setup()
	defer teardown(testServer)

	reader := strings.NewReader("")
	request, err := http.NewRequest("GET", testServer.URL+"/bench/status", reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Errorf(err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("Success expected: %d (%s) \n", resp.StatusCode, string(errMsg))
	}
}

func TestShowHistory(t *testing.T) {
	testServer := setup()
	defer teardown(testServer)

	// prepare
	requestNewBenchmark(testServer)
	requestNewBenchmark(testServer)

	// query
	reader := strings.NewReader("")
	request, err := http.NewRequest("GET", testServer.URL+"/bench/history", reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("Success expected: %d (%s) \n", resp.StatusCode, string(errMsg))
	}
}

func JobQuery(t *testing.T) {
	testServer := setup()
	defer teardown(testServer)

	// prepare
	jobID, _ := requestNewBenchmark(testServer)

	// query
	reader := strings.NewReader("")
	request, err := http.NewRequest("GET", testServer.URL+"/bench/job/"+FormatInt64(jobID), reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("Success expected: %d (%s) \n", resp.StatusCode, string(errMsg))
	}

	job := new(Job)
	job.parseFromJson(resp)
	if job.ID != jobID {
		t.Errorf("job id not match")
	}
}

func TestAbortJob(t *testing.T) {
	testServer := setup()
	defer teardown(testServer)

	// prepare
	jobID, _ := requestNewBenchmark(testServer)

	// query
	reader := strings.NewReader("")
	request, err := http.NewRequest("DELETE", testServer.URL+"/bench/job/"+FormatInt64(jobID), reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("FAIL : %d (%s) \n", resp.StatusCode, string(errMsg))
	}
}
