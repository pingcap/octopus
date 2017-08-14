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
		start := time.Now()
		h.ServeHTTP(w, r)
		cost := time.Since(start).Seconds()
		log.Infof("HTTPRequst - %s - %s - %s (%.3f sec)", r.RemoteAddr, r.Method, r.URL, cost)
	})
}

func CreateRouter(svr *Server) http.Handler {
	rdr := render.New(render.Options{
		IndentJSON: true,
	})

	r := mux.NewRouter()
	hdl := newBenchmarkHandler(svr, rdr)
	r.HandleFunc("/bench/plan", hdl.Plan).Methods("POST")
	r.HandleFunc("/bench/jobs", hdl.ListJobs).Methods("GET")
	r.HandleFunc("/bench/job/{id}", hdl.GetJob).Methods("GET")
	r.HandleFunc("/bench/job/{id}", hdl.AbortJob).Methods("DELETE")
	r.HandleFunc("/bench/status", hdl.GetGlobalStatus).Methods("GET")

	return httpRequestMiddleware(r, svr)
}
