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
	tsapi "github.com/pingcap/tidb-operator/pkg/tidbset/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// TidbSetsInterface has methods to work with TiDBCLuster resources
type TidbSetsInterface interface {
	Create(*tsapi.TidbSet) (*tsapi.TidbSet, error)
	List(metav1.ListOptions) (*tsapi.TidbSetList, error)
	Get(string, metav1.GetOptions) (*tsapi.TidbSet, error)
	Delete(string, *metav1.DeleteOptions) error
	Update(*tsapi.TidbSet) (*tsapi.TidbSet, error)
	Watch(metav1.ListOptions) (watch.Interface, error)
}

type tidbSets struct {
	client *pingcapV1Client
	ns     string
}

func newTidbSets(cli *pingcapV1Client, ns string) *tidbSets {
	return &tidbSets{cli, ns}
}

func (tss *tidbSets) Create(tidbset *tsapi.TidbSet) (*tsapi.TidbSet, error) {
	result := &tsapi.TidbSet{}
	err := tss.client.Post().
		Namespace(tss.ns).
		Resource("tidbsets").
		Body(tidbset).
		Do().
		Into(result)
	return result, err
}

func (tss *tidbSets) List(opts metav1.ListOptions) (*tsapi.TidbSetList, error) {
	result := &tsapi.TidbSetList{}
	err := tss.client.Get().
		Namespace(tss.ns).
		Resource("tidbsets").
		VersionedParams(&opts, metav1.ParameterCodec).
		Do().
		Into(result)
	return result, err
}

func (tss *tidbSets) Get(name string, opts metav1.GetOptions) (*tsapi.TidbSet, error) {
	result := &tsapi.TidbSet{}
	err := tss.client.Get().
		Namespace(tss.ns).
		Resource("tidbsets").
		Name(name).
		VersionedParams(&opts, metav1.ParameterCodec).
		Do().
		Into(result)
	return result, err
}

func (tss *tidbSets) Delete(name string, opts *metav1.DeleteOptions) error {
	return tss.client.Delete().
		Namespace(tss.ns).
		Resource("tidbsets").
		Name(name).
		Body(opts).
		Do().
		Error()
}

func (tss *tidbSets) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return tss.client.Get().
		Prefix("watch").
		Namespace(tss.ns).
		Resource("tidbsets").
		VersionedParams(&opts, metav1.ParameterCodec).
		Watch()
}

func (tss *tidbSets) Update(tidbset *tsapi.TidbSet) (*tsapi.TidbSet, error) {
	result := &tsapi.TidbSet{}
	err := tss.client.Put().
		Namespace(tss.ns).
		Resource("tidbsets").
		Name(tidbset.Metadata.Name).
		Body(tidbset).
		Do().
		Into(result)
	return result, err
}
