package backend

import (
	//"errors"
	"fmt"
	//"io"
	//"os"
	"bufio"
	"bytes"
	"errors"

	//"net/http"
	//"os/exec"
	"path/filepath"
)

const downloadsDir string = "./bins/"

type TiClusterConfig struct {
	tidb *BinPackage
	tikv *BinPackage
	pd   *BinPackage
}

type TiClusterByAnsible struct {
	cfg *TiClusterConfig
}

func newTiClusterByAnsible(cfg *TiClusterConfig) *TiClusterByAnsible {
	c := &TiClusterByAnsible{
		cfg: cfg,
	}
	return c
}

func (c *TiClusterByAnsible) Prepare() (err error) {
	if err = c.fetchResources(); err != nil {
		return
	}

	if err = c.boostrap(); err != nil {
		return
	}

	return
}

func (c *TiClusterByAnsible) fetchResources() error {
	components := []*BinPackage{c.cfg.tidb, c.cfg.tikv, c.cfg.pd}
	for _, pkg := range components {
		saveto := filepath.Join(downloadsDir, pkg.Name) // TODO ... temp dir by date mark
		if _, err := Download(pkg.Url, saveto); err != nil {
			return err
		}
	}
	return nil
}

func (c *TiClusterByAnsible) boostrap() error {
	buf := new(bytes.Buffer)

	if ok := ExecCmd("ansible-playbook -i inventory.ini bootstrap.yml -k -K", buf); !ok {
		return errors.New(fmt.Sprintf("start failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *TiClusterByAnsible) Deploy() error {
	//buf := bytes.NewBuffer(make([]byte, 0))
	//w := bufio.NewWriter(buf)
	buf := new(bytes.Buffer)

	if ok := ExecCmd("ansible-playbook -i inventory.ini deploy.yml -k -K", buf); !ok {
		return errors.New(fmt.Sprintf("start failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *TiClusterByAnsible) Start() error {
	buf := bytes.NewBuffer(make([]byte, 0))
	w := bufio.NewWriter(buf)

	if ok := ExecCmd("ansible-playbook -i inventory.ini start.yml", w); !ok {
		return errors.New(fmt.Sprintf("start failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *TiClusterByAnsible) Stop() error {
	buf := bytes.NewBuffer(make([]byte, 0))
	w := bufio.NewWriter(buf)

	if ok := ExecCmd("ansible-playbook -i inventory.ini stop.yml", w); !ok {
		return errors.New(fmt.Sprintf("start failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *TiClusterByAnsible) Reset() error {
	return nil
}

func (c *TiClusterByAnsible) checkAvaiable() (bool, error) {
	return true, nil
}
