package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/octopus/schrodinger/cat"
	"github.com/unrolled/render"
)

func NewRouter(service *cat.CatService) http.Handler {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	catHandler := newCatHandler(service, rd)
	r := mux.NewRouter()
	r.HandleFunc("/cat/new", catHandler.newCat).Methods("POST")
	return httpRequest(r)
}
