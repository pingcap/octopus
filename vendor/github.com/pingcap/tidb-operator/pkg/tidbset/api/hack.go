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
// limitations under the License.package spec

package api

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GetObjectKind is required to satisfy Object interface
func (t *TidbSet) GetObjectKind() schema.ObjectKind {
	return &t.TypeMeta
}

// GetObjectMeta is required to satisfy ObjectMetaAccessor interface
func (t *TidbSet) GetObjectMeta() metav1.Object {
	return &t.Metadata
}

// GetObjectKind is required to satisfy Object interface
func (ts *TidbSetList) GetObjectKind() schema.ObjectKind {
	return &ts.TypeMeta
}

// GetListMeta is required to satisfy ListMetaAccessor interface
func (ts *TidbSetList) GetListMeta() metav1.List {
	return &ts.Metadata
}

// The code below is used only to work around a known problem with third-party
// resources and ugorji. If/when these issues are resolved, the code below
// should no longer be required.

// TidbSetListCopy is an alias for TidbSetList
type TidbSetListCopy TidbSetList

// TidbSetCopy is an alias for TidbSet
type TidbSetCopy TidbSet

// UnmarshalJSON is required to fix TPR problem
func (t *TidbSet) UnmarshalJSON(data []byte) error {
	tmp := TidbSetCopy{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	tmp2 := TidbSet(tmp)
	*t = tmp2
	return nil
}

// UnmarshalJSON is required to fix TPR problem
func (ts *TidbSetList) UnmarshalJSON(data []byte) error {
	tmp := TidbSetListCopy{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	tmp2 := TidbSetList(tmp)
	*ts = tmp2
	return nil
}
