package main

import (
	"fmt"
	"github.com/ngaut/log"
	"golang.org/x/net/context"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	var runmode bool
	var wg sync.WaitGroup
	wg.Add(1)

	//runmode = true
	if runmode {
		ChannelSlackID = DebugChannelSlackID
		ChannelName = DebugChannelName
	} else {
		ChannelSlackID = WorkChannelSlackID
		ChannelName = WorkChannelName
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &Run{
		Ctx:           ctx,
		Cancel:        cancel,
		BenchTestJobs: make(chan *BenchTestJob, MaxBenchTestJob),
	}

	go r.RunScheduler()
	go func() {
		log.Info("create http server")
		r.CreateRender()
		http.ListenAndServe(fmt.Sprintf(":%d", HTTPPORT), r.CreateRouter())
	}()

	go func() {
		log.Infof("starting listen slack")
		r.CreateSlackAPI()
		r.SlackHandler()
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Errorf("Got signal [%d] to exit.", sig)
		r.Cancel()
		wg.Done()
	}()

	wg.Wait()

}
