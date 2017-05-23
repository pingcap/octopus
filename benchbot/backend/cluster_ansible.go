package backend

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"runtime"

	"path"
	"path/filepath"
)

var ansibleDir, downloadsDir, binsDir string

func init() {
	_, filename, _, _ := runtime.Caller(1)
	pdir := path.Dir(filename)

	ansibleDir = filepath.Join(path.Dir(pdir), "ansible")
	downloadsDir = filepath.Join(ansibleDir, "downloads")
	binsDir = filepath.Join(ansibleDir, "resources/bin")
}

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
		// fmt.Printf("[%s] to down from -- %s\n", pkg.Name, pkg.Url)
		if _, err := Download(pkg.Url, saveto); err != nil {
			return err
		}
	}
	return nil
}

func (c *TiClusterByAnsible) boostrap() error {
	buf := new(bytes.Buffer)

	//if ok := ExecCmd("ansible-playbook -i inventory.ini bootstrap.yml -k -K", buf); !ok {
	cmd := fmt.Sprintf("sh %s bootstrap >> out.log 2&>1", filepath.Join(ansibleDir, "run.sh"))
	if ok := ExecCmd(cmd, buf); !ok {
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

func (c *TiClusterByAnsible) Destory() error {
	return nil
}
