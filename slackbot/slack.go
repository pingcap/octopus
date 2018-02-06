package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/nlopes/slack"
)

const (
	helpDetail = `@octopus-bot bench_test
pd, type: branch, value: master, platform:centos7
tidb, type: branch, value: master, platform:centos7
tikv, type: branch, value: master, platform:centos7
note: current-master
PS: type options(githash/branch/tag)`
)

//CreateSlackAPI create slack api
func (r *Run) CreateSlackAPI() {
	r.SlackRTM = slack.New(SlackAPIToken).NewRTM()
}

//SlackHandler for slack schedual
func (r *Run) SlackHandler() {
	go r.SlackRTM.ManageConnection()

	for msg := range r.SlackRTM.IncomingEvents {
		// fmt.Print("Event Received: ")
		switch ev := msg.Data.(type) {
		case *slack.HelloEvent:
			// Ignore hello

		case *slack.ConnectedEvent:
			// fmt.Println("Infos:", ev.Info)
			// fmt.Println("Connection counter:", ev.ConnectionCount)
			// // Replace #general with your Channel ID
			// rtm.SendMessage(rtm.NewOutgoingMessage("Hello world", "#general"))

		case *slack.MessageEvent:
			go r.HandleMessageEvent(ev)
			// fmt.Printf("Message: %v\n", ev)

		case *slack.PresenceChangeEvent:
			// fmt.Printf("Presence Change: %v\n", ev)

		case *slack.LatencyReport:
			// logs.Info("Slack Current latency: %v\n", ev.Value)
			// fmt.Printf("Current latency: %v\n", ev.Value)

		case *slack.RTMError:
			log.Infof("Slack Error: %s\n", ev.Error())
			// fmt.Printf("Error: %s\n", ev.Error())

		case *slack.InvalidAuthEvent:
			log.Infof("Slack Invalid credentials")
			return

		default:

			// Ignore other events..
			// fmt.Printf("Unexpected: %v\n", msg.Data)
		}
	}

}

//HandleMessageEvent for slack message event
func (r *Run) HandleMessageEvent(ev *slack.MessageEvent) {
	reg := regexp.MustCompile(SlackCheckkeyWord)
	if ev.Channel == ChannelSlackID && len(reg.FindAllString(ev.Text, -1)) > 0 {
		_createunixtimestr := strings.Split(ev.Timestamp, ".")[0]
		_createunixtime, _ := strconv.ParseInt(_createunixtimestr, 10, 64)
		_createtime := time.Time(time.Unix(_createunixtime, 0)).Local()
		var _userid string
		if ev.User == "" {
			_userid = "<rebot>"
		} else {
			_userid = fmt.Sprintf("<@%s>", ev.User)
		}

		if strings.TrimSpace(strings.Split(strings.Split(ev.Text, "\n")[0], " ")[1]) == "help" {
			r.SlackSendMessager(fmt.Sprintf("=======\nHi,%s  骚年, Time :%s\n O(∩_∩)O SMART BOY;套路在此\n$$$$$$$$$$$\n%s\n$$$$$$$$$$$\n```",
				_userid, _createtime.Format(TimeFormat), helpDetail), ChannelSlackID)

			return
		}

		err := r.HandleBenchTestMessage(ev)
		if err != nil {
			r.SlackSendMessager(fmt.Sprintf("=======\nHi,%s  骚年, Time :%s\nYou provide error message with error\n%v\n", _userid,
				_createtime.Format(TimeFormat), err), ChannelSlackID)
			return

		}
		r.SlackSendMessager(fmt.Sprintf("=======\nHi,%s  骚年, Time :%s\n GOT IT", _userid,
			_createtime.Format(TimeFormat)), ChannelSlackID)
	}

}

//HandleBenchTestMessage for message handler
func (r *Run) HandleBenchTestMessage(sme *slack.MessageEvent) error {
	bt := r.InitBenchTestJob()
	getlines := 4
	ml := strings.Split(sme.Text, "\n")
	if len(ml) >= 5 {
		getlines = 5
	}

	bt.BenchBotJob.Meta.Upstream = "slack"
	userinfo, _ := r.SlackRTM.GetUserInfo(sme.User)
	if userinfo == nil {
		bt.BenchBotJob.Meta.Creator = "robot"
	} else {
		bt.BenchBotJob.Meta.Creator = userinfo.Name
	}

	for _, line := range ml[1:getlines] {
		log.Infof("hanldler line %s", line)
		if strings.HasPrefix(line, "note") {
			bt.BenchBotJob.Meta.Note = strings.SplitAfterN(line, ":", 2)[1]
		} else if strings.HasPrefix(line, "tikv") || strings.HasPrefix(line, "tidb") || strings.HasPrefix(line, "pd") {
			bj, err := r.BuildClusterElement(bt.JobID, line)
			if err != nil {
				return errors.Errorf("can not build %s cluster element with error %v ", line, err)
			}
			bt.BuildJobs[bj.BuildID] = bj
		} else {
			return errors.Errorf("can not find keyword tikv/tidb/pd from line %s", line)
		}

	}
	if len(bt.BuildJobs) != 3 {
		return errors.Errorf("can not get enought cluster element for build %v,now is %d", bt.BuildJobs, len(bt.BuildJobs))
	}

	return r.AddBenchTestJob(bt)
}

//BuildClusterElement for build tikv/tidb/pd
func (r *Run) BuildClusterElement(btID, line string) (*BuildJob, error) {
	bj := r.InitBuildJob()
	_m := strings.Split(line, ",")
	bj.Infomation.Repo = strings.TrimSpace(_m[0])
	var typeV, valueV string
	for _, _uk := range _m[1:] {
		_kv := strings.Split(_uk, ":")
		if strings.Contains(_uk, ":") && strings.Contains(_uk, "type") {
			typeV = strings.TrimSpace(_kv[1])
		} else if strings.Contains(_uk, ":") && strings.Contains(_uk, "value") {
			valueV = strings.TrimSpace(_kv[1])
		} else if strings.Contains(_uk, ":") && strings.Contains(_uk, "platform") {
			bj.Infomation.Platform = strings.TrimSpace(_kv[1])
		}
	}

	if typeV == "branch" && valueV != "" {
		bj.Infomation.Branch = valueV
	} else if typeV == "githash" && valueV != "" {
		bj.Infomation.GitHash = valueV
	} else if typeV == "tag" && valueV != "" {
		bj.Infomation.Tag = valueV
	} else {
		return bj, errors.Errorf("can not find type and value %s", line)
	}

	bj.Callback = fmt.Sprintf("%s%s/%s/%s", CallbackHTTPURL, BuildCallbackBaseURL, btID, bj.Infomation.Repo)

	bjID, err := r.CreateBuildJob(bj)
	if err != nil {
		return bj, err
	}
	bj.BuildID = bjID
	log.Infof("call build successful %+v", bj)
	return bj, nil

}

//SlackSendMessager for send messager
func (r *Run) SlackSendMessager(rsm, ChannelSlackID string) {
	r.SlackRTM.SendMessage(r.SlackRTM.NewOutgoingMessage(rsm, ChannelSlackID))
}
