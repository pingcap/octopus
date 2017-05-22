package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/pingcap/octopus/benchbot/backend"
)

func setup() *httptest.Server {
	svr := NewServer()
	router := CreateRouter(svr)
	return httptest.NewServer(router)
}

func teardown(s *httptest.Server) {
	s.Close()
	fmt.Printf("CLOSE\n")
}

func loadsJson(resp *http.Response) map[string]string {
	body, _ := ioutil.ReadAll(resp.Body)
	data := make(map[string]string)
	json.Unmarshal(body, &data)
	return data
}

func requestNewTesting(httpSvr *httptest.Server) (string, error) {
	var failure error = nil
	var jobId string = ""

	reqJson := `{
		"user": "pingcap", "note": "simple note msg", "upstream": "unit_test",
		"tidb": {"branch": "tidb/1", "commit_hash": "0x10", "url": "127.0.0.1:8808/test.tar.gz"},
		"tikv": {"branch": "tidb/1", "commit_hash": "0x20", "url": "127.0.0.1:8808/test.tar.gz"},
		"pd":   {"branch": "pd/1",   "commit_hash": "0x30", "url": "127.0.0.1:8808/test.tar.gz"}
	}`

	reader := strings.NewReader(reqJson)
	request, err := http.NewRequest("POST", httpSvr.URL+"/test", reader)

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		failure = errors.New("new testing failed : " + string(errMsg))
	} else {
		data := loadsJson(resp)
		jobId = data["job_id"]
	}

	return jobId, failure
}

func TestNewTask(t *testing.T) {
	fmt.Printf("[TestNewTask]\n")

	testServer := setup()
	defer teardown(testServer)

	jobId, err := requestNewTesting(testServer)
	if err != nil {
		t.Errorf("%s", err.Error())
	}

	fmt.Printf("job created : %s\n", jobId)
}

func TestShowSummary(t *testing.T) {
	fmt.Printf("[TestShowSummary]\n")

	testServer := setup()
	defer teardown(testServer)

	reader := strings.NewReader("")
	request, err := http.NewRequest("GET", testServer.URL+"/info", reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Errorf(err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("Success expected: %d (%s) \n", resp.StatusCode, string(errMsg))
	}

	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Printf("info == %s\n", string(body))
}

func TestShowHistory(t *testing.T) {
	fmt.Printf("[TestShowHistory]\n")

	testServer := setup()
	defer teardown(testServer)

	// prepare
	requestNewTesting(testServer)
	requestNewTesting(testServer)

	// query
	reader := strings.NewReader("")
	request, err := http.NewRequest("GET", testServer.URL+"/history", reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("Success expected: %d (%s) \n", resp.StatusCode, string(errMsg))
	}

	//body, _ := ioutil.ReadAll(resp.Body)
	//fmt.Printf("historys == %s\n", string(body))
}

func JobQuery(t *testing.T) {
	fmt.Printf("[JobQuery]\n")

	testServer := setup()
	defer teardown(testServer)

	// prepare
	jobId, _ := requestNewTesting(testServer)

	// query
	reader := strings.NewReader("")
	request, err := http.NewRequest("GET", testServer.URL+"/job/"+jobId, reader)
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
	//body, _ := ioutil.ReadAll(resp.Body)
	//fmt.Printf("job == %s\n", string(body))
}

func TestAbortJob(t *testing.T) {
	fmt.Printf("[TestAbortJob]\n")

	testServer := setup()
	defer teardown(testServer)

	// prepare
	jobId, _ := requestNewTesting(testServer)

	// query
	reader := strings.NewReader("")
	request, err := http.NewRequest("DELETE", testServer.URL+"/job/"+jobId, reader)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != http.StatusOK {
		errMsg, _ := ioutil.ReadAll(resp.Body)
		t.Errorf("FAIL : %d (%s) \n", resp.StatusCode, string(errMsg))
	}

	//body, _ := ioutil.ReadAll(resp.Body)
	//fmt.Printf("abort == %s\n", string(body))
}
