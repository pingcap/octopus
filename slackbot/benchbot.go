package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/benchbot/backend"
	"github.com/pingcap/octopus/benchbot/cluster"
	"github.com/pingcap/octopus/benchbot/suite"
	"math"
	"time"
)

//InitBenchTestJob is init
func (r *Run) InitBenchTestJob() *BenchTestJob {
	return &BenchTestJob{
		BenchBotJob: &backend.BenchmarkJob{
			Meta: &backend.BenchmarkMeta{
				Packages: make([]*cluster.BinPackage, 0),
			},
			Result: &backend.BenchmarkResult{
				Details: make([]*suite.CaseResult, 0),
			},
		},
		BuildJobs:        make(map[string]*BuildJob),
		Ctime:            time.Now(),
		JobID:            uuid.New().String(),
		BuildFinishedNum: 0,
		BenchBotID:       0,
	}
}

//BenchTestIsFinished return bj is finished
func (r *Run) BenchTestIsFinished(bt *BenchTestJob) bool {
	return bt.BenchBotJob.Status == "finished"
}

//GetBenchTestJobByBenchBotID get bench test job by bench bot id
func (r *Run) GetBenchTestJobByBenchBotID(bbID int64) (*BenchTestJob, error) {
	for i := 0; i < len(r.BenchTestJobs); i++ {
		bt := <-r.BenchTestJobs
		r.BenchTestJobs <- bt
		if bt.BenchBotID == bbID {
			return bt, nil
		}
	}
	return nil, errors.Errorf("can not find bench bot id %d", bbID)
}

//GetBenchTestJobByJobID get bench test job by bench job id
func (r *Run) GetBenchTestJobByJobID(btjID string) (*BenchTestJob, error) {
	for i := 0; i < len(r.BenchTestJobs); i++ {
		bt := <-r.BenchTestJobs
		r.BenchTestJobs <- bt
		if bt.JobID == btjID {
			return bt, nil
		}
	}
	return nil, errors.Errorf("can not find bench job id %s", btjID)
}

//AddBenchTestJob add benchtest job
func (r *Run) AddBenchTestJob(bt *BenchTestJob) error {
	r.BenchTestJobs <- bt
	return nil
}

//CreateBenchTestJob for call bench test
func (r *Run) CreateBenchTestJob(bt *BenchTestJob) {
	if bjNum := len(bt.BuildJobs); bjNum != 3 {
		log.Errorf("can not get build packages number [%d] is not right", bjNum)
		return
	}
	bt.BenchBotJob.Meta.HttpCallback = fmt.Sprintf("%s%s/%s", CallbackHTTPURL, BenchCallbackBaseURL, bt.JobID)
	bt.BenchBotJob.Meta.Packages = bt.BenchBotJob.Meta.Packages[:0]
	for _, bj := range bt.BuildJobs {
		if !r.BuildJobIsFinished(bj) {
			log.Errorf("build id [%d] is not finished", bj.BuildID)
			return
		}
		bt.BenchBotJob.Meta.Packages = append(bt.BenchBotJob.Meta.Packages, bj.Infomation)
	}
	btj := &backend.BenchmarkJob{}
	errP := r.HTTPPost(fmt.Sprintf("%s%s", BenchTestBaseURL, CreateJobURL), bt.BenchBotJob.Meta, btj)
	if errP != nil || btj.ID <= 0 {
		log.Errorf("can not post data to bench test %v with err %v", bt.BenchBotJob.Meta, errP)
		return
	}
	bt.BenchBotID = btj.ID
	log.Infof("create bench test successful with benchbot id [%d]", btj.ID)

}

//UpdateBenchTestJob achieve bench test result by bench test map id
func (r *Run) UpdateBenchTestJob(bt *BenchTestJob) {
	if r.BenchTestIsFinished(bt) {
		return
	}
	for i := 0; i < 3; i++ {
		bj, err := r.AchieveBenchTestJobByBenchID(bt.BenchBotID)
		if err == nil {
			bt.BenchBotJob = bj
			return
		}
		log.Errorf("can not achieve information from benchbot by %d with error %v", bt.BenchBotID, err)
		time.Sleep(3 * time.Second)
	}
}

//AchieveBenchTestJobByBenchID get bench test result by benchID
func (r *Run) AchieveBenchTestJobByBenchID(benchID int64) (*backend.BenchmarkJob, error) {
	bj := &backend.BenchmarkJob{}
	err := r.HTTPGet(fmt.Sprintf("%s%s%d", BenchTestBaseURL, JobDetailURL, benchID), bj)
	if err != nil || bj.ID != benchID {
		return nil, errors.Errorf("can not http get bench test id %d detail with error %v", benchID, err)
	}
	return bj, nil
}

//BenchTestOutMsg bench test out msg
func (r *Run) BenchTestOutMsg(NowBTDetail, BeforeBTDetail *backend.BenchmarkJob) {
	////////////////////////////////////////////////////////////////////////////////////////
	// [ JOB - 11 ]
	// Create :
	//     * UserName @ 2017-01-01 12:00:00
	// Packages:
	//     * tidb / tikv / pd =  eac27696 / 6bd8ce61 / ce8db87a
	// Result ( vs “Job-10” ) :
	//    1. < block-write >
	//        * QPS / BPS / EPS = 1000 / 1000 / 0.01 ( -10 % / +10 % / -200% )
	//        * AvgMs = 500 ( -16.67 % )
	//        * PercentileMs = 900 ( -10 % )
	//    2. < simple-insert >
	//        * QPS / BPS / EPS = 1000 / 0 / 0.00 ( -10 % / -- / -- )
	//        * AvgMs = 500 ( -16.67 % )
	//        * PercentileMs = 900 ( -10 % )
	//    3. < simple-select >
	//        * QPS / BPS / EPS = 1000 / 0 / 0.99 ( -10 % / -- / +INF )
	//        * AvgMs = 500 ( -16.67 % )
	//        * PercentileMs = 900 ( -10 % )
	//    4. < simple-delete >
	//        * QPS / BPS / EPS = 1000 / 0 / 0.00 ( -10 % / -- / -INF )
	//        * AvgMs = 500 ( -16.67 % )
	//        * PercentileMs = 900 ( -10 % )
	// Details :
	//     * http://127.0.0.1/benchbot/chart?compare=11,10
	////////////////////////////////////////////////////////////////////////////////////////
	var tidb, tikv, pd string
	for _, _meta := range NowBTDetail.Meta.Packages {
		if _meta.Repo == "tidb" {
			tidb = _meta.GitHash[:7]
		} else if _meta.Repo == "tikv" {
			tikv = _meta.GitHash[:7]
		} else if _meta.Repo == "pd" {
			pd = _meta.GitHash[:7]
		}
	}
	var caseinfo string
	for _i, _caseDetail := range NowBTDetail.Result.Details {
		for _, _comparecaseDetail := range BeforeBTDetail.Result.Details {
			if _caseDetail.Name == _comparecaseDetail.Name {
				cds := _caseDetail.Stat
				cpcds := _comparecaseDetail.Stat
				_qpspercent := handlePercent(cds.Qps, cpcds.Qps)
				_epspercent := handlePercent(float64(cds.Eps), float64(cpcds.Eps))
				_avgmspercent := handlePercent(cds.Histogram.AvgMs, cpcds.Histogram.AvgMs)
				_avgpercentms := handlePercent(cds.Histogram.PercentileMs, cpcds.Histogram.PercentileMs)
				//print it
				caseinfo = caseinfo +
					fmt.Sprintf("\t%d. < %s >\n", _i+1, _caseDetail.Name) +
					fmt.Sprintf("\t\t* QPS / EPS = %.3f / %.3f [ %+.2f%% / %+.2f%% ]\n",
						cds.Qps, cds.Eps, _qpspercent, _epspercent) +
					fmt.Sprintf("\t\t* AvgMs = %.3f [ %+.2f%% ]\n", cds.Histogram.AvgMs, _avgmspercent) +
					fmt.Sprintf("\t\t* PercentileMs = %.3f [ %+.2f%% ]\n", cds.Histogram.PercentileMs, _avgpercentms)

				if math.Abs(_qpspercent) >= WarningPercent {
					rsm := fmt.Sprintf("Bench_Test\n\tJob[%d] Case[%s] QPS[%f]\t\n\tJob[%d] Case[%s] QPS[%f]\t\n\tBIG PERCENT [%+.2f%%]",
						NowBTDetail.ID, _caseDetail.Name, cds.Qps, BeforeBTDetail.ID, _comparecaseDetail.Name, cpcds.Qps, _qpspercent)
					log.Info("get qpspercent more than warningPercent, message: [%s] to channel id  [%s]", rsm, WarningchannelSlackID)
					r.SlackSendMessager(rsm, WarningchannelSlackID)
				}
			}

		}
	}
	info := "```" +
		fmt.Sprintf("[ JOB - %d ]\n", NowBTDetail.ID) +
		fmt.Sprintf("Create :\n\t* %s @ %s\n", NowBTDetail.Meta.Creator, NowBTDetail.CreateTime) +
		fmt.Sprintf("Packages:\n\t* tidb / tikv / pd =  %s / %s / %s\n", tidb, tikv, pd) +
		fmt.Sprintf("Result ( vs “Job-%d” ) :\n", BeforeBTDetail.ID) +
		caseinfo +
		fmt.Sprintf("Details :\n\t* %s/dashboard/#/\n", BenchTestBaseURL) +
		"```"
	r.SlackSendMessager(info, ChannelSlackID)
}
