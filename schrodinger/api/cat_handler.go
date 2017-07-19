package api

import (
	"net/http"

	"github.com/pingcap/octopus/schrodinger/cat"
	"github.com/unrolled/render"
)

type CatHandler struct {
	render  *render.Render
	service *cat.CatService
}

func newCatHandler(service *cat.CatService, rd *render.Render) *CatHandler {
	return &CatHandler{
		service: service,
		render:  rd,
	}
}

func (c *CatHandler) NewCat(w http.ResponseWriter, r *http.Request) {
	//defer r.Body.Close()
}
