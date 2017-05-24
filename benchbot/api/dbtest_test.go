package api

import (
	"errors"
	"fmt"
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

func setup() *httptest.Server {
	svr := NewServer()
	router := CreateRouter(svr)
	return httptest.NewServer(router)
}

func teardown(s *httptest.Server) {
	s.Close()
}

func loadsJson(resp *http.Response) map[string]string {
	body, _ := ioutil.ReadAll(resp.Body)
	data := make(map[string]string)
	json.Unmarshal(body, &data)
	return data
}

func requestNewBenchmark(httpSvr *httptest.Server) (string, error) {
	var failure error = nil
	var jobId string = ""

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
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		failure = errors.New("new benching plan failed : " + string(errMsg))
	} else {
		data := loadsJson(resp)
		jobId = data["jobid"]
	}

	return jobId, failure
}

func TestNewTask(t *testing.T) {
	fmt.Printf("[TestNewTask]\n")

	testServer := setup()
	defer teardown(testServer)

	jobId, err := requestNewBenchmark(testServer)
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	if len(jobId) == 0 {
		t.Errorf("jobid empty !")
	}
}

func TestShowSummary(t *testing.T) {
	fmt.Printf("[TestShowSummary]\n")

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
	fmt.Printf("[TestShowHistory]\n")

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
	fmt.Printf("[JobQuery]\n")

	testServer := setup()
	defer teardown(testServer)

	// prepare
	jobId, _ := requestNewBenchmark(testServer)

	// query
	reader := strings.NewReader("")
	request, err := http.NewRequest("GET", testServer.URL+"/bench/job/"+jobId, reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("Success expected: %d (%s) \n", resp.StatusCode, string(errMsg))
	}

	data := loadsJson(resp)
	if data["job_id"] != jobId {
		t.Errorf("NO !!")
	}
}

func TestAbortJob(t *testing.T) {
	fmt.Printf("[TestAbortJob]\n")

	testServer := setup()
	defer teardown(testServer)

	// prepare
	jobId, _ := requestNewBenchmark(testServer)

	// query
	reader := strings.NewReader("")
	request, err := http.NewRequest("DELETE", testServer.URL+"/bench/job/"+jobId, reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("FAIL : %d (%s) \n", resp.StatusCode, string(errMsg))
	}
}
