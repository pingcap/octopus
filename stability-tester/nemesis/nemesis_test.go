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

package nemesis

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"

	"github.com/pingcap/octopus/stability-tester/cluster"
	"github.com/pingcap/octopus/stability-tester/config"
)

const fooName = "foo"

type fooNemesis struct {
	ch chan string
}

func newFooFactory(ch chan string) func(string) (Nemesis, error) {
	return func(_ string) (Nemesis, error) {
		return &fooNemesis{
			ch: ch,
		}, nil
	}
}

// Execute implements Nemesis Execute interface.
func (n *fooNemesis) Execute(ctx context.Context, targets []cluster.Node) error {
	for _, t := range targets {
		log.Infof("fooNemesis executes on %s", t.Name())
		n.ch <- t.Name()
	}
	return nil
}

// String implements fmt.Stringer interface.
func (n *fooNemesis) String() string {
	return fooName
}

type mockNode struct {
	name string
}

// Start the services in this node.
func (n *mockNode) Start() error {
	return nil
}

// Stop the services in this node.
func (n *mockNode) Stop() error {
	return nil
}

// Kill the services in this node.
func (n *mockNode) Kill() error {
	return nil
}

// IsRunning returns whether the services is running or not.
func (n *mockNode) IsRunning() (bool, error) {
	return false, nil
}

// Execute executes a command in this node.
func (n *mockNode) Execute(ctx context.Context, command string) error {
	return nil
}

// Name returns the name of this service
func (n *mockNode) Name() string {
	return n.name
}

func TestNemesis(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testNemesisSuite{})

type testNemesisSuite struct {
	c  *cluster.Cluster
	ch chan string
}

func (s *testNemesisSuite) SetUpSuite(c *C) {
	// Initialize the default random number source.
	rand.Seed(time.Now().UTC().UnixNano())

	s.ch = make(chan string)
	RegisterNemesis(fooName, newFooFactory(s.ch))

	s.c = cluster.NewEmptyCluster()
	s.c.AddNode(&mockNode{name: "t1"})
	s.c.AddNode(&mockNode{name: "t2"})
	s.c.AddNode(&mockNode{name: "t3"})
}

func (s *testNemesisSuite) TestNewFooNemesis(c *C) {
	nemesesMu.Lock()
	f, ok := nemeses[fooName]
	nemesesMu.Unlock()
	c.Assert(ok, IsTrue)

	n, err := f("")
	c.Assert(err, IsNil)
	c.Assert(n, NotNil)

	nemesesMu.Lock()
	_, ok = nemeses["not exists"]
	nemesesMu.Unlock()
	c.Assert(ok, IsFalse)
}

const testConfig = `
wait = "1s"
names = ["foo"]
ranges = ["one"]

[targets]
pd = [""]
# FIXME: for now, only tikv will be scheduled.
tikv = ["t1", "t2", "t3"]
tidb = [""]

[cfgs]
foo = ""
`

func (s *testNemesisSuite) TestRunNemeses(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ncfg config.NemesesConfig
	err := toml.Unmarshal([]byte(testConfig), &ncfg)
	c.Assert(err, IsNil)

	RunNemeses(ctx, &ncfg, s.c)

	// Only schedule tikvs .
	count := len(ncfg.Targets.Tikv)
	// Pass if all node has been scheduled.
	set := make(map[string]struct{})
	for {
		name := <-s.ch
		log.Info("for loop")
		n, err := s.c.GetNode(name)
		c.Assert(err, IsNil)
		c.Assert(n, NotNil)
		set[name] = struct{}{}

		if len(set) == count {
			return
		}
	}
}
