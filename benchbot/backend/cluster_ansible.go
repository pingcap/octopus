package backend

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const debug bool = true

// cluster offer

type ClusterManager struct {
	cfg *ServerConfig
}

func newClusterManager(cfg *ServerConfig) *ClusterManager {
	os.MkdirAll(cfg.AnsibleDownloadsDir, os.ModePerm)
	os.MkdirAll(cfg.AnsibleBinsDir, os.ModePerm)

	return &ClusterManager{cfg: cfg}
}

func (cmgr *ClusterManager) newAnsibleCluster(ccfg *ClusterConfig) *AnsibleCluster {
	c := &AnsibleCluster{
		ccfg:         ccfg,
		ansibleDir:   cmgr.cfg.AnsibleDir,
		downloadsDir: cmgr.cfg.AnsibleDownloadsDir,
		binsDir:      cmgr.cfg.AnsibleBinsDir,
	}
	return c
}

// cluster real deployment by ansible

type ClusterConfig struct {
	tidb *BinPackage
	tikv *BinPackage
	pd   *BinPackage
}

type AnsibleCluster struct {
	ccfg         *ClusterConfig
	ansibleDir   string
	downloadsDir string
	binsDir      string
}

func (c *AnsibleCluster) Prepare() (err error) {
	if err = c.fetchResources(); err != nil {
		return
	}

	if err = c.boostrap(); err != nil {
		return
	}

	return
}

func (c *AnsibleCluster) fetchResources() error {
	components := []*BinPackage{c.ccfg.tidb, c.ccfg.tikv, c.ccfg.pd}
	for _, pkg := range components {
		saveto := filepath.Join(c.downloadsDir, pkg.Repo+".tar.gz")
		// TODO ... temp dir by date mark
		if _, err := Download(pkg.BinUrl, saveto); err != nil {
			return err
		}

		// ps : unarchive through sys command
		cmd := fmt.Sprintf("tar -zxf %s -C %s", saveto, c.binsDir)
		if ok := ExecCmd(cmd, nil); !ok {
			return errors.New("unarchive failed : %s\n")
		}
	}
	return nil
}

func (c *AnsibleCluster) boostrap() error {
	fmt.Printf("[ansible] boostrap ...\n")
	buf := new(bytes.Buffer)
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s bootstrap", filepath.Join(c.ansibleDir, "run.sh"))
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("boostrap failed : %s\n", string(buf.Bytes())))
	}

	return nil
}

func (c *AnsibleCluster) Deploy() error {
	fmt.Printf("[ansible] deploy ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s deploy", filepath.Join(c.ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("deploy failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleCluster) Run() error {
	fmt.Printf("[ansible] run ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s start", filepath.Join(c.ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)

	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("start failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleCluster) Stop() error {
	fmt.Printf("[ansible] stop ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s stop", filepath.Join(c.ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("stop failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleCluster) Reset() error {
	fmt.Printf("[ansible] reset ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s reset", filepath.Join(c.ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("stop failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleCluster) checkAvaiable() (bool, error) {
	return true, nil
}

func (c *AnsibleCluster) Destory() error {
	fmt.Printf("[ansible] destory ...\n")
	if debug {
		return nil
	}

	cmd := fmt.Sprintf("sh %s destory", filepath.Join(c.ansibleDir, "run.sh"))
	buf := new(bytes.Buffer)
	if ok := ExecCmd(cmd, buf); !ok {
		return errors.New(fmt.Sprintf("destory failed : %s\n", string(buf.Bytes())))
	}
	return nil
}
