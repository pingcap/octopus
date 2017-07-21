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

package cluster

import (
	"fmt"
	"io/ioutil"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
	"golang.org/x/crypto/ssh"
)

//sshNode is a embeded structure for other node.
//no matter binary node , docker node ,k8s node we need ssh initialize the environmnet  or execute command
type sshNode struct {
	cli     *ssh.Client
	host    string
	user    string
	port    int
	keyPath string
}

func newSSHNode(cfg config.ServiceConfig) *sshNode {
	n := &sshNode{
		host:    cfg.Host,
		user:    cfg.SSH.User,
		port:    cfg.SSH.Port,
		keyPath: cfg.SSH.KeyPath,
	}

	n.cli = n.createClient()
	return n
}

//Create ssh Client
func (s *sshNode) createClient() *ssh.Client {
	key, err := ioutil.ReadFile(s.keyPath)
	if err != nil {
		log.Fatalf("unable to read private key: %v", err)
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Fatalf("unable to parse private key: %v", err)
	}

	sshConfig := &ssh.ClientConfig{
		User: s.user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	client, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		log.Fatalf("unable to connect: %v", err)
	}
	return client
}

// execute command without result
func (s *sshNode) execCmd(cmd string) error {
	session, err := s.cli.NewSession()
	defer session.Close()
	if err != nil {
		return err
	}
	err = session.Run(cmd)
	if err != nil {
		log.Warnf("exec: %s", cmd)
		return err
	}
	return nil
}

// execute command with result
func (s *sshNode) execCmdWithResult(cmd string) []byte {
	session, err := s.cli.NewSession()
	defer session.Close()
	if err != nil {
		log.Error(err)
	}
	res, err := session.Output(cmd)
	if err != nil {
		log.Warnf("exec: %s", cmd)
		log.Error(err)
	}
	return res
}

func (s *sshNode) Close() {
	s.cli.Close()
}
