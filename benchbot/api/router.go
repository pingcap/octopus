package api

import (
	//"net/http"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	. "github.com/pingcap/octopus/benchbot/backend"
)

func CreateRouter(svr *Server) *mux.Router {
	rdr := render.New(render.Options{
		IndentJSON: true,
	})

	r := mux.NewRouter()
	hdl := newTestingHandler(svr, rdr)
	//	testing request
	r.HandleFunc("/test", hdl.RunTest).Methods("POST")
	r.HandleFunc("/test/release", hdl.RunReleaseTest).Methods("POST")
	//	test job
	r.HandleFunc("/job/{jobId}", hdl.QueryTestJob).Methods("GET")
	r.HandleFunc("/job/{jobId}", hdl.AbortTestJob).Methods("DELETE")
	//	global
	r.HandleFunc("/status", hdl.GetGlobalStatus).Methods("GET")
	r.HandleFunc("/history", hdl.ShowHistory).Methods("GET")

	return r
}
