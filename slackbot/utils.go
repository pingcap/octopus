package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"io/ioutil"
	"net/http"
	"time"
)

//HTTPPost for http post
func (r *Run) HTTPPost(url string, pData interface{}, gData interface{}) error {
	body, err := json.Marshal(pData)
	if err != nil {
		return errors.Errorf("can not marshal json data %v with error %v", pData, err)
	}

	var resp *http.Response
	for i := 0; i < 3; i++ {
		rsp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
		if (err != nil || rsp.StatusCode != http.StatusOK) && i == 2 {
			info := fmt.Sprintf("open URL<post> [%s] FAILED", url)
			r.SlackRTM.SendMessage(r.SlackRTM.NewOutgoingMessage(info, ChannelSlackID))
			return errors.New(info)
		} else if rsp.StatusCode == http.StatusOK {
			resp = rsp
			break
		}
	}
	defer resp.Body.Close()
	bodyByte, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(bodyByte, gData)
}

//HTTPGet for http get
func (r *Run) HTTPGet(url string, data interface{}) error {
	var resp *http.Response
	for i := 0; i < 3; i++ {
		rsp, err := http.Get(url)
		if (err != nil || rsp.StatusCode != http.StatusOK) && i == 2 {
			info := fmt.Sprintf("open URL<get> [%s] FAILED", url)
			r.SlackSendMessager(info, ChannelSlackID)
			return errors.New(info)
		} else if rsp.StatusCode == 200 {
			resp = rsp
			break
		}
		log.Errorf("http get url %s time %d", url, i)

		time.Sleep(3 * time.Second)
	}

	defer resp.Body.Close()
	bodyByte, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(bodyByte, data)
}

func handlePercent(now, before float64) float64 {
	if before == 0 {
		return 0
	}
	return (now - before) / before * 100

}
