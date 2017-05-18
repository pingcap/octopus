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
	h := newTestingHandler(svr, rdr)
	r.HandleFunc("/test", h.RunTesting).Methods("POST")
	r.HandleFunc("/test/release", h.RunReleaseTesting).Methods("POST")
	r.HandleFunc("/info", h.GetGlobalInfo).Methods("GET")
	r.HandleFunc("/history", h.ShowHistory).Methods("GET")
	r.HandleFunc("/job/{jobId}", h.QueryTestingJob).Methods("GET")
	r.HandleFunc("/job/{jobId}", h.AbortTestingJob).Methods("DELETE")

	return r
}
