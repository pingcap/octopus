// Copyright 2017 PingCAP, Inc.
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

package client

import (
	"github.com/juju/errors"

	tapi "github.com/pingcap/tidb-operator/pkg/api"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/rest"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Interface is a mixin kubernetes.Interface with tidb's TPR client interface
type Interface interface {
	kubernetes.Interface
	PingcapV1() PingcapV1Interface
}

// PingcapV1Interface is tidb's TPR client interface
type PingcapV1Interface interface {
	TidbClusters(string) TidbClustersInterface
	TidbSets(string) TidbSetsInterface
}

// New returns a client.Interface
func New(kubeconfig string) (Interface, error) {
	kubeCli, err := newKubernetesClientset(kubeconfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg, err := NewConfig(kubeconfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.GroupVersion = &tapi.GroupVersion
	cfg.APIPath = "/apis"
	cfg.ContentType = runtime.ContentTypeJSON
	cfg.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}

	cli, err := rest.RESTClientFor(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &client{kubeCli, &pingcapV1Client{cli}}, nil
}

// NewConfig gets *restclient.Config from kubeconfig
func NewConfig(kubeconfig string) (cfg *restclient.Config, err error) {
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return
}

func newKubernetesClientset(kubeconfig string) (*kubernetes.Clientset, error) {
	cfg, err := NewConfig(kubeconfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return kubernetes.NewForConfig(cfg)
}

type client struct {
	*kubernetes.Clientset
	*pingcapV1Client
}

func (c *client) PingcapV1() PingcapV1Interface {
	if c == nil {
		return nil
	}

	return c.pingcapV1Client
}

type pingcapV1Client struct {
	*rest.RESTClient
}

// TidbClusters returns TidbClustersInterface
func (c *pingcapV1Client) TidbClusters(ns string) TidbClustersInterface {
	return newTidbClusters(c, ns)
}

// TidbSets returns TidbSetsInterface
func (c *pingcapV1Client) TidbSets(ns string) TidbSetsInterface {
	return newTidbSets(c, ns)
}
