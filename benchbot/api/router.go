package api

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/backend"
)

func httpRequestMiddleware(h http.Handler, svr *Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := time.Now()
		h.ServeHTTP(w, r)
		cost := time.Now().Sub(s).Seconds()
		log.Debugf("HttpRequst - %s - %s - %s (%.3f sec)", r.RemoteAddr, r.Method, r.URL, cost)
	})
}

func CreateRouter(svr *Server) http.Handler {
	rdr := render.New(render.Options{
		IndentJSON: true,
	})

	r := mux.NewRouter()
	hdl := newBenchmarkHandler(svr, rdr)
	// benchmark running request
	r.HandleFunc("/bench/plan", hdl.Plan).Methods("POST")
	// benchmark job
	r.HandleFunc("/bench/job/{id}", hdl.QueryJob).Methods("GET")
	r.HandleFunc("/bench/job/{id}", hdl.AbortJob).Methods("DELETE")
	r.HandleFunc("/bench/jobs/{ids}", hdl.GetJobs).Methods("GET")
	// benchmark summary
	r.HandleFunc("/bench/status", hdl.GetGlobalStatus).Methods("GET")
	r.HandleFunc("/bench/history", hdl.ShowHistory).Methods("GET")

	return httpRequestMiddleware(r, svr)
}
