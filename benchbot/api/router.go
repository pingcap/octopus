package api

import (
	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	. "github.com/pingcap/octopus/benchbot/backend"
)

func CreateRouter(svr *Server) *mux.Router {
	rdr := render.New(render.Options{
		IndentJSON: true,
	})

	r := mux.NewRouter()
	hdl := newBenchmarkHandler(svr, rdr)
	// benchmark running request
	r.HandleFunc("/bench/plan", hdl.PlanBenchmark).Methods("POST")
	r.HandleFunc("/bench/release/plan", hdl.PlanReleaseBenchmark).Methods("POST")
	// benchmark job
	r.HandleFunc("/bench/job/{jobid}", hdl.QueryBenchJob).Methods("GET")
	r.HandleFunc("/bench/job/{jobid}", hdl.AbortBenchJob).Methods("DELETE")
	// benchmark summary
	r.HandleFunc("/bench/status", hdl.GetGlobalStatus).Methods("GET")
	r.HandleFunc("/bench/history", hdl.ShowHistory).Methods("GET")

	return r
}
