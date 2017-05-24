package backend

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
)

const debug bool = true

var ansibleDir, downloadsDir, binsDir string

func init() {
	_, filename, _, _ := runtime.Caller(1)
	pdir := path.Dir(filename)

	ansibleDir = filepath.Join(path.Dir(pdir), "ansible")
	downloadsDir = filepath.Join(ansibleDir, "downloads")
	binsDir = filepath.Join(ansibleDir, "resources/bin")
	os.MkdirAll(downloadsDir, os.ModePerm)
	os.MkdirAll(binsDir, os.ModePerm)
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
		saveto := filepath.Join(downloadsDir, pkg.Repo+".tar.gz")
		// TODO ... temp dir by date mark
		if _, err := Download(pkg.Url, saveto); err != nil {
			return err
		}

		// ps : unarchive through sys command
		cmd := fmt.Sprintf("tar -zxf %s -C %s", saveto, binsDir)
		if ok := ExecCmd(cmd, nil); !ok {
			return errors.New("unarchive failed : %s\n")
		}
	}
	return nil
}

func (c *TiClusterByAnsible) boostrap() error {
	fmt.Printf("[ansible] boostrap ...\n")
	buf := new(bytes.Buffer)
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s bootstrap", filepath.Join(ansibleDir, "run.sh"))
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("boostrap failed : %s\n", string(buf.Bytes())))
	}

	return nil
}

func (c *TiClusterByAnsible) Deploy() error {
	fmt.Printf("[ansible] deploy ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s deploy", filepath.Join(ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("deploy failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *TiClusterByAnsible) Run() error {
	fmt.Printf("[ansible] run ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s start", filepath.Join(ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)

	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("start failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *TiClusterByAnsible) Stop() error {
	fmt.Printf("[ansible] stop ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s stop", filepath.Join(ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("stop failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *TiClusterByAnsible) Reset() error {
	fmt.Printf("[ansible] reset ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s reset", filepath.Join(ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("stop failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *TiClusterByAnsible) checkAvaiable() (bool, error) {
	return true, nil
}

func (c *TiClusterByAnsible) Destory() error {
	fmt.Printf("[ansible] destory ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s destory", filepath.Join(ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("destory failed : %s\n", string(buf.Bytes())))
	}
	return nil
}
