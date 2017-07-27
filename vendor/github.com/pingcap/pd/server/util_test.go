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

package server

import (
	"math/rand"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testMinMaxSuite{})

type testMinMaxSuite struct{}

func (s *testMinMaxSuite) TestMinUint64(c *C) {
	c.Assert(minUint64(1, 2), Equals, uint64(1))
	c.Assert(minUint64(2, 1), Equals, uint64(1))
	c.Assert(minUint64(1, 1), Equals, uint64(1))
}

func (s *testMinMaxSuite) TestMaxUint64(c *C) {
	c.Assert(maxUint64(1, 2), Equals, uint64(2))
	c.Assert(maxUint64(2, 1), Equals, uint64(2))
	c.Assert(maxUint64(1, 1), Equals, uint64(1))
}

func (s *testMinMaxSuite) TestMinDuration(c *C) {
	c.Assert(minDuration(time.Minute, time.Second), Equals, time.Second)
	c.Assert(minDuration(time.Second, time.Minute), Equals, time.Second)
	c.Assert(minDuration(time.Second, time.Second), Equals, time.Second)
}

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestParseTimestap(c *C) {
	for i := 0; i < 3; i++ {
		t := time.Now().Add(time.Second * time.Duration(rand.Int31n(1000)))
		data := uint64ToBytes(uint64(t.UnixNano()))
		nt, err := parseTimestamp(data)
		c.Assert(err, IsNil)
		c.Assert(nt, Equals, t)
	}
	data := []byte("pd")
	nt, err := parseTimestamp(data)
	c.Assert(err, NotNil)
	c.Assert(nt.Equal(zeroTime), IsTrue)
}
