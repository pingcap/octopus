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

package e2e

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pingcap/tidb-operator/test/e2e/framework"

	"testing"
)

var bootstrap *framework.Bootstrap

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "TiDB Operator Smoke tests")
}

var _ = BeforeSuite(func() {
	By("Bootstrapping TiDB Operator")
	var err error
	bootstrap, err = framework.NewBootstrap()
	Expect(err).NotTo(HaveOccurred())

	err = bootstrap.Setup()
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	By("Tearing down TiDB Operator")
	err := bootstrap.Teardown()
	Expect(err).NotTo(HaveOccurred())
})
