package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/ngaut/log"
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

func (c *CatHandler) newCat(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		c.render.JSON(w, http.StatusBadRequest, "invalid post data !")
		return
	}
	cat := &cat.Cat{}
	if err := json.Unmarshal(data, cat); err != nil {
		log.Errorf("invalid json data - %s", data)
		c.render.JSON(w, http.StatusBadRequest, fmt.Sprintf("post data json incorrent: %s", err.Error()))
		return
	}
	if !cat.Valid() {
		c.render.JSON(w, http.StatusBadRequest, "not no valid request, params request!")
		return
	}
	if err := c.service.PutCat(cat); err != nil {
		log.Errorf("put cat to service list failed: %s", err)
		c.render.JSON(w, http.StatusInternalServerError, fmt.Sprintf("put cat to service list failed: %s", err.Error()))
		return
	}
	c.render.JSON(w, http.StatusOK, "success!")
	return
}

func (c *CatHandler) runCat(w http.ResponseWriter, r *http.Request) {
	catName := mux.Vars(r)["name"]
	if !c.service.IsExist(catName) {
		c.render.JSON(w, http.StatusBadRequest, "cat is not exist")
		return
	}
	go func() {
		err := c.service.RunCat(catName)
		if err != nil {
			log.Errorf("Start Cat failed: %s", err.Error())
		}
	}()
	c.render.JSON(w, http.StatusOK, "cat is building")
}
