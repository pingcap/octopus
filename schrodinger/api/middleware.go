package api

import (
	"net/http"
	"time"

	"github.com/ngaut/log"
)

func httpRequest(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := time.Now()
		h.ServeHTTP(w, r)
		cost := time.Now().Sub(s).Seconds()
		log.Debugf("HttpRequst - %s - %s - %s (%.3f sec)", r.RemoteAddr, r.Method, r.URL, cost)
	})
}
