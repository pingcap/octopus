package backend

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

var (
	ansibleOperationScript string = ""
	ansibleResoucesDir     string = ""
	ansibleDowloadsDir     string = ""

	clusterDetectMaxtTimes int = 60
)

func initAnsibleEnv(cfg *ServerConfig) error {
	ansibleOperationScript = filepath.Join(cfg.Ansible.Dir, "operation.sh")
	ansibleResoucesDir = filepath.Join(cfg.Ansible.Dir, "resources")
	ansibleDowloadsDir = filepath.Join(cfg.Ansible.Dir, "downloads")

	if err := os.MkdirAll(ansibleResoucesDir, os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(ansibleDowloadsDir, os.ModePerm); err != nil {
		return err
	}
	return nil
}

type AnsibleCluster struct {
	meta *clusterMeta
	ctx  context.Context
	db   *sql.DB
}

func newAnsibleCluster(meta *clusterMeta, ctx context.Context) *AnsibleCluster {
	return &AnsibleCluster{meta: meta, ctx: ctx}
}

func (c *AnsibleCluster) operate(op string, output io.Writer) bool {
	cmd := fmt.Sprintf("sh %s %s", ansibleOperationScript, op)
	return ExecCmd(cmd, c.ctx, output)
}

func (c *AnsibleCluster) Prepare() (err error) {
	ops := [](func() error){c.fetchResources, c.boostrap}
	for _, op := range ops {
		if err = op(); err != nil {
			return
		}
	}
	return
}

func (c *AnsibleCluster) fetchResources() error {
	log.Info("[Ansible] fetching resources ...")
	tempDir, err := ioutil.TempDir("", "ansible-downs")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	packages := []*BinPackage{c.meta.tidb, c.meta.tikv, c.meta.pd}
	for _, pkg := range packages {
		log.Infof("[Ansible] download - %s", pkg.BinUrl)
		saveto := filepath.Join(ansibleDowloadsDir, pkg.Repo+".tar.gz")
		// TODO ... temp dir by date mark
		if _, err := Download(pkg.BinUrl, saveto); err != nil {
			return err
		}

		// ps : unarchive through sys command
		cmd := fmt.Sprintf("tar -zxf %s -C %s", saveto, tempDir)
		if ok := ExecCmd(cmd, c.ctx, nil); !ok {
			return fmt.Errorf("unarchive failed : %s", pkg.BinUrl)
		}
	}

	ExecCmd(fmt.Sprintf("cp -r %s/* %s/", tempDir, ansibleResoucesDir), c.ctx, nil)

	return nil
}

func (c *AnsibleCluster) boostrap() error {
	log.Info("[Ansible] running boostrap ...")
	buf := new(bytes.Buffer)
	if ok := c.operate("bootstrap", buf); !ok {
		return fmt.Errorf("boostrap failed : %s\n", string(buf.Bytes()))
	}

	return nil
}

func (c *AnsibleCluster) Deploy() error {
	log.Info("[Ansible] deploying ...")
	buf := new(bytes.Buffer)
	if ok := c.operate("deploy", buf); !ok {
		return fmt.Errorf("deploy failed : %s\n", string(buf.Bytes()))
	}
	return nil
}

func (c *AnsibleCluster) Run() error {
	log.Info("[Ansible] starting ...")
	buf := new(bytes.Buffer)
	if ok := c.operate("start", buf); !ok {
		return fmt.Errorf("start failed : %s\n", string(buf.Bytes()))
	}
	return nil
}

func (c *AnsibleCluster) Stop() error {
	log.Info("[Ansible] stoping ...")
	buf := new(bytes.Buffer)
	if ok := c.operate("stop", buf); !ok {
		return fmt.Errorf("stop failed : %s\n", string(buf.Bytes()))
	}
	return nil
}

func (c *AnsibleCluster) Reset() error {
	log.Info("[Ansible] reseting ...")
	buf := new(bytes.Buffer)
	if ok := c.operate("reset", buf); !ok {
		return fmt.Errorf("reset failed : %s\n", string(buf.Bytes()))
	}
	return nil
}

func (c *AnsibleCluster) Destory() error {
	c.Stop()
	return c.free()
}

func (c *AnsibleCluster) free() error {
	log.Info("[Ansible] destorying ...")
	buf := new(bytes.Buffer)
	if ok := c.operate("destory", buf); !ok {
		return fmt.Errorf("destory failed : %s\n", string(buf.Bytes()))
	}
	return nil
}

func (c *AnsibleCluster) Ping() (err error) {
	if err = CheckConnection(c.db); err == nil {
		return
	}

	if c.db != nil {
		c.db.Close()
	}

	c.db, err = c.connectCluster()

	return
}

func (c *AnsibleCluster) connectCluster() (db *sql.DB, err error) {
	dsn := c.meta.dsn
	for i := 0; i < clusterDetectMaxtTimes; i++ {
		db, err = ConnectDB(dsn.user, dsn.password, dsn.host, dsn.port, dsn.db)
		if err == nil {
			return
		}
		time.Sleep(time.Second * 1)
	}
	return
}

func (c *AnsibleCluster) Close() {
	// TODO ... ?
}

func (c *AnsibleCluster) Accessor() *sql.DB {
	return c.db
}
