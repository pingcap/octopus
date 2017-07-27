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
	tcapi "github.com/pingcap/tidb-operator/pkg/tidbcluster/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// TidbClustersInterface has methods to work with TiDBCLuster resources
type TidbClustersInterface interface {
	Create(*tcapi.TidbCluster) (*tcapi.TidbCluster, error)
	List(metav1.ListOptions) (*tcapi.TidbClusterList, error)
	Get(string, metav1.GetOptions) (*tcapi.TidbCluster, error)
	Delete(string, *metav1.DeleteOptions) error
	Update(*tcapi.TidbCluster) (*tcapi.TidbCluster, error)
	Watch(metav1.ListOptions) (watch.Interface, error)
}

type tidbClusters struct {
	client *pingcapV1Client
	ns     string
}

func newTidbClusters(cli *pingcapV1Client, ns string) *tidbClusters {
	return &tidbClusters{cli, ns}
}

// Create creates a new TidbCluster
func (tcs *tidbClusters) Create(tidbcluster *tcapi.TidbCluster) (*tcapi.TidbCluster, error) {
	result := &tcapi.TidbCluster{}
	err := tcs.client.Post().
		Namespace(tcs.ns).
		Resource("tidbclusters").
		Body(tidbcluster).
		Do().
		Into(result)
	return result, err
}

// List takes a selector, and returns the list of TidbClusters that match that selector
func (tcs *tidbClusters) List(opts metav1.ListOptions) (*tcapi.TidbClusterList, error) {
	result := &tcapi.TidbClusterList{}
	err := tcs.client.Get().
		Namespace(tcs.ns).
		Resource("tidbclusters").
		VersionedParams(&opts, metav1.ParameterCodec).
		Do().
		Into(result)
	return result, err
}

// Get returns a TidbCluster
func (tcs *tidbClusters) Get(name string, opts metav1.GetOptions) (*tcapi.TidbCluster, error) {
	result := &tcapi.TidbCluster{}
	err := tcs.client.Get().
		Namespace(tcs.ns).
		Resource("tidbclusters").
		Name(name).
		VersionedParams(&opts, metav1.ParameterCodec).
		Do().
		Into(result)
	return result, err
}

// Delete takes the name of the endpoint, and returns an error if one occurs
func (tcs *tidbClusters) Delete(name string, opts *metav1.DeleteOptions) error {
	return tcs.client.Delete().
		Namespace(tcs.ns).
		Resource("tidbclusters").
		Name(name).
		Body(opts).
		Do().
		Error()
}

// Watch returns a watch.Interface that watches the requested tidbclusters
func (tcs *tidbClusters) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return tcs.client.Get().
		Prefix("watch").
		Namespace(tcs.ns).
		Resource("tidbclusters").
		VersionedParams(&opts, metav1.ParameterCodec).
		Watch()
}

// Update update a TidbCluster
func (tcs *tidbClusters) Update(tidbcluster *tcapi.TidbCluster) (*tcapi.TidbCluster, error) {
	result := &tcapi.TidbCluster{}
	err := tcs.client.Put().
		Namespace(tcs.ns).
		Resource("tidbclusters").
		Name(tidbcluster.Metadata.Name).
		Body(tidbcluster).
		Do().
		Into(result)
	return result, err
}
