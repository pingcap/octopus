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
// limitations under the License

package suite

import (
	"testing"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

func TestTPCH(t *testing.T) {
	cfg := &TPCHConfig{
		Host:       "127.0.0.1",
		Port:       4000,
		ScriptsDir: "../../tpch_scripts",
	}
	tpchSuite := NewTPCHSuite(cfg)
	caseResults, err := tpchSuite.run()
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	for _, caseResult := range caseResults {
		if caseResult.Stat.Error > 0 {
			log.Fatal("tpch failed!")
		}
	}
}
