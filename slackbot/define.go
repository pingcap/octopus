package main

import (
	"github.com/nlopes/slack"
	"github.com/pingcap/octopus/benchbot/backend"
	"github.com/pingcap/octopus/benchbot/cluster"
	"github.com/unrolled/render"
	"golang.org/x/net/context"
	"time"
)

const (
	HTTPPORT = 18082
	//channelSlackID = "C2WPS901Y" // channel sre
	//channel benchbot_beta
	DebugChannelSlackID = "C5NGSNJKB"
	DebugChannelName    = "#benchbot_beta"
	//channel sre-bot
	WorkChannelSlackID = "C5SPWSKTL"
	WorkChannelName    = "#sre-bot"

	SlackAPIToken     = "xoxb-196252456449-cO1LUIv1uYqDslslO9rudnU3"
	SlackCheckkeyWord = "^<@U5S7EDED7>.*bench_test|^<@U5S7EDED7>.*build_binary|^@octopus.*bench_test|^<@U5S7EDED7>.*help|^@octopus-bot.*help"

	CallbackHTTPURL = "http://103.218.243.18:18082"

	BuildBaseURL         = "http://103.218.243.18:28082/v1/build"
	BuildCallbackBaseURL = "/build/callback"

	BenchTestBaseURL     = "http://101.96.133.218:8080/benchbot"
	CreateJobURL         = "/bench/plan"
	JobDetailURL         = "/bench/job/"
	BenchCallbackBaseURL = "/bench/callback"

	SlackBotBenchTestURL = "/v1/benchtest"

	WarningchannelSlackID = "C2WPS901Y"
	WarningchannelName    = "#sre"
	WarningPercent        = 10.1

	TimeFormat = "2006-01-02 15:04:05"

	BenchScheduleTime = time.Minute
	MaxBenchTestJob   = 1000
)

var (
	ChannelSlackID string
	ChannelName    string
)

///////////////////////////////////////////////////////////////////////////
// build  struct
///////////////////////////////////////////////////////////////////////////

////////////////////Build.Status
// pending 任务已创建, 待执行
// failed   任务失败
// running 任务运行中, 正在执行测试
// finished  任务已完成
// aborted 任务被中止 (人为/异常)
////////////////////Build.Status

// BuildJob for build
type BuildJob struct {
	BuildID    string              `json:"build_id"`
	Callback   string              `json:"callback"`
	Infomation *cluster.BinPackage `json:"information"`
	Status     string              `json:"status"`
}

// RepoDetail for build and benchbot
//type RepoDetail struct {
//	BinaryURL string `json:"binary_url"`
//	Branch    string `json:"branch"`
//	GitHash   string `json:"git_hash"`
//	Platform  string `json:"platform"`
//	Repo      string `json:"repo"`
//	Tag       string `json:"tag"`
//}

//BenchTestJob for benchtest
type BenchTestJob struct {
	JobID            string
	BuildJobs        map[string]*BuildJob
	BenchBotJob      *backend.BenchmarkJob
	BenchBotID       int64
	BuildFinishedNum int64
	CallBackFlag     bool
	Ctime            time.Time
	Ftime            time.Time
	SlackOutFlag     bool
}

//Run for init
type Run struct {
	Rdr           *render.Render
	Ctx           context.Context
	Cancel        context.CancelFunc
	SlackRTM      *slack.RTM
	BenchTestJobs chan *BenchTestJob
}
