// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/pd/server"
)

const (
	redirectorHeader = "PD-Redirector"
)

const (
	errRedirectFailed      = "redirect failed"
	errRedirectToNotLeader = "redirect to not leader"
)

type redirector struct {
	s *server.Server
}

func newRedirector(s *server.Server) *redirector {
	return &redirector{s: s}
}

func (h *redirector) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if h.s.IsLeader() {
		next(w, r)
		return
	}

	// Prevent more than one redirection.
	if name := r.Header.Get(redirectorHeader); len(name) != 0 {
		log.Errorf("redirect from %v, but %v is not leader", name, h.s.Name())
		http.Error(w, errRedirectToNotLeader, http.StatusInternalServerError)
		return
	}

	r.Header.Set(redirectorHeader, h.s.Name())

	leader, err := h.s.GetLeader()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	urls, err := server.ParseUrls(strings.Join(leader.GetClientUrls(), ","))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	newCustomReverseProxies(urls).ServeHTTP(w, r)
}

type customReverseProxies struct {
	urls    []url.URL
	clients []*http.Client
}

func newCustomReverseProxies(urls []url.URL) *customReverseProxies {
	p := &customReverseProxies{}

	for _, u := range urls {
		var client *http.Client

		// Use unix socket in tests.
		if u.Scheme == "unix" {
			u.Scheme = "http"
			client = &http.Client{Transport: &http.Transport{Dial: unixDial}}
		} else {
			client = &http.Client{}
		}

		p.urls = append(p.urls, u)
		p.clients = append(p.clients, client)
	}

	return p
}

func (p *customReverseProxies) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for i, client := range p.clients {
		r.RequestURI = ""
		r.URL.Host = p.urls[i].Host
		r.URL.Scheme = p.urls[i].Scheme

		resp, err := client.Do(r)
		if err != nil {
			log.Error(err)
			continue
		}

		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Error(err)
			continue
		}

		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, err := w.Write(b); err != nil {
			log.Error(err)
			continue
		}

		return
	}

	http.Error(w, errRedirectFailed, http.StatusInternalServerError)
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
