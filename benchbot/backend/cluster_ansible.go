package backend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	log "github.com/ngaut/log"
)

var (
	ansibleOperationScript string = ""
	ansibleResoucesDir     string = ""
	ansibleDowloadsDir     string = ""

	clusterHost     string = ""
	clusterPort     uint16 = 3306
	clusterDB       string = ""
	clusterAuthUser string = ""
	clusterAuthPsw  string = ""
)

func initAnsibleEnv(cfg *ServerConfig) error {
	ansibleOperationScript = filepath.Join(cfg.Ansible.Dir, "operation.sh")
	ansibleResoucesDir = filepath.Join(cfg.Ansible.Dir, "resources")
	ansibleDowloadsDir = filepath.Join(cfg.Ansible.Dir, "downloads")

	clusterHost = cfg.Ansible.ClusterDSN.Host
	clusterPort = cfg.Ansible.ClusterDSN.Port
	clusterDB = cfg.Ansible.ClusterDSN.DB
	clusterAuthUser = cfg.Ansible.ClusterDSN.AuthUser
	clusterAuthPsw = cfg.Ansible.ClusterDSN.AuthPassword

	if err := os.MkdirAll(ansibleResoucesDir, os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(ansibleDowloadsDir, os.ModePerm); err != nil {
		return err
	}
	return nil
}

type ClusterMeta struct {
	tidb *BinPackage
	tikv *BinPackage
	pd   *BinPackage
}

type AnsibleClusterInstance struct {
	meta *ClusterMeta
}

func newAnsibleClusterInstance(meta *ClusterMeta) *AnsibleClusterInstance {
	return &AnsibleClusterInstance{meta: meta}
}

func operate(op string, output io.Writer) bool {
	cmd := fmt.Sprintf("sh %s bootstrap", ansibleOperationScript)
	return ExecCmd(cmd, output)
}

func (c *AnsibleClusterInstance) Prepare() (err error) {
	ops := [](func() error){c.fetchResources, c.boostrap}
	for _, op := range ops {
		if err = op(); err != nil {
			return
		}
	}
	return
}

func (c *AnsibleClusterInstance) fetchResources() error {
	log.Info("[Ansible] fetching resources ...")
	packages := []*BinPackage{c.meta.tidb, c.meta.tikv, c.meta.pd}
	for _, pkg := range packages {
		log.Infof("[Ansible] download - %s", pkg.BinUrl)
		saveto := filepath.Join(ansibleDowloadsDir, pkg.Repo+".tar.gz")
		// TODO ... temp dir by date mark
		if _, err := Download(pkg.BinUrl, saveto); err != nil {
			return err
		}

		// ps : unarchive through sys command
		cmd := fmt.Sprintf("tar -zxf %s -C %s", saveto, ansibleResoucesDir)
		if ok := ExecCmd(cmd, nil); !ok {
			return errors.New("unarchive failed : %s\n")
		}
	}
	return nil
}

func (c *AnsibleClusterInstance) boostrap() error {
	log.Info("[Ansible] running boostrap ...")
	buf := new(bytes.Buffer)
	if ok := operate("bootstrap", buf); !ok {
		return errors.New(fmt.Sprintf("boostrap failed : %s\n", string(buf.Bytes())))
	}

	return nil
}

func (c *AnsibleClusterInstance) Deploy() error {
	log.Info("[Ansible] deploying ...")
	buf := new(bytes.Buffer)
	if ok := operate("deploy", buf); !ok {
		return errors.New(fmt.Sprintf("deploy failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleClusterInstance) Run() error {
	log.Info("[Ansible] starting ...")
	buf := new(bytes.Buffer)
	if ok := operate("start", buf); !ok {
		return errors.New(fmt.Sprintf("start failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleClusterInstance) Stop() error {
	log.Info("[Ansible] stoping ...")
	buf := new(bytes.Buffer)
	if ok := operate("stop", buf); !ok {
		return errors.New(fmt.Sprintf("stop failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleClusterInstance) Reset() error {
	log.Info("[Ansible] reseting ...")
	buf := new(bytes.Buffer)
	if ok := operate("reset", buf); !ok {
		return errors.New(fmt.Sprintf("reset failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleClusterInstance) Destory() error {
	log.Info("[Ansible] destorying ...")
	buf := new(bytes.Buffer)
	if ok := operate("destory", buf); !ok {
		return errors.New(fmt.Sprintf("destory failed : %s\n", string(buf.Bytes())))
	}
	return nil
}

func (c *AnsibleClusterInstance) Valid() bool {
	db, err := ConnectDB(clusterAuthUser, clusterAuthPsw, clusterHost, clusterPort, clusterDB)
	success := (db != nil && err == nil)
	if db != nil {
		db.Close()
	}
	return success
}
