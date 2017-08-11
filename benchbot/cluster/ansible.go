package cluster

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ngaut/log"
	"golang.org/x/net/context"

	. "github.com/pingcap/octopus/benchbot/common"
)

const (
	GitHashNote = "Git Commit Hash"
)

var (
	ansiblePackagesDir     string = ""
	ansibleResoucesDir     string = ""
	ansibleDowloadsDir     string = ""
	ansibleOperationScript string = ""

	clusterDetectMaxTimes int = 60
)

type ansibleProcess func() error

func initAnsibleEnv(cfg *AnsibleConfig) error {
	ansiblePackagesDir = filepath.Join(cfg.Dir, "packages")
	ansibleResoucesDir = filepath.Join(cfg.Dir, "resources")
	ansibleDowloadsDir = filepath.Join(cfg.Dir, "downloads")
	ansibleOperationScript = filepath.Join(cfg.Dir, "ansible.sh")

	if err := os.MkdirAll(ansiblePackagesDir, os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(ansibleResoucesDir, os.ModePerm); err != nil {
		return err
	}
	if err := os.MkdirAll(ansibleDowloadsDir, os.ModePerm); err != nil {
		return err
	}
	return nil
}

type AnsibleCluster struct {
	ctx  context.Context
	meta *clusterMeta
	conn *sql.DB
}

func NewAnsibleCluster(ctx context.Context, meta *clusterMeta) *AnsibleCluster {
	return &AnsibleCluster{ctx: ctx, meta: meta}
}

func (c *AnsibleCluster) Prepare() error {
	ops := []ansibleProcess{c.fetchResources, c.Boostrap}
	for _, op := range ops {
		if err := op(); err != nil {
			return err
		}
	}
	return nil
}

func (c *AnsibleCluster) Boostrap() error {
	log.Info("[Ansible] bootstrap ...")
	return c.operate("bootstrap")
}

func (c *AnsibleCluster) Deploy() error {
	log.Info("[Ansible] deploy ...")
	return c.operate("deploy")
}

func (c *AnsibleCluster) Start() error {
	log.Info("[Ansible] start ...")
	if err := c.operate("start"); err != nil {
		return err
	}
	return c.Open()
}

func (c *AnsibleCluster) Stop() error {
	log.Info("[Ansible] stop ...")
	if err := c.operate("stop"); err != nil {
		return err
	}
	return c.Close()
}

func (c *AnsibleCluster) Open() error {
	dsn := c.meta.dsn
	for i := 0; i < clusterDetectMaxTimes; i++ {
		db, err := ConnectDB(dsn.User, dsn.Password, dsn.Host, dsn.Port, dsn.DB)
		if err == nil {
			c.conn = db
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("failed to connect db")
}

func (c *AnsibleCluster) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *AnsibleCluster) Reset() error {
	log.Info("[Ansible] reset ...")
	if err := c.operate("reset"); err != nil {
		return err
	}
	return c.Close()
}

func (c *AnsibleCluster) Destory() error {
	log.Info("[Ansible] destory ...")
	if err := c.operate("destory"); err != nil {
		return err
	}
	return c.Close()
}

func (c *AnsibleCluster) Accessor() *sql.DB {
	return c.conn
}

func (c *AnsibleCluster) operate(op string) error {
	cmd := fmt.Sprintf("sh %s %s", ansibleOperationScript, op)
	_, err := ExecCmd(c.ctx, cmd)
	return err
}

func (c *AnsibleCluster) fetchResources() error {
	log.Info("[Ansible] fetch resources ...")

	packages := []*BinPackage{c.meta.pd, c.meta.tikv, c.meta.tidb}
	for _, pkg := range packages {
		log.Infof("[Ansible] download package = %s", pkg.BinUrl)

		pkgDir := filepath.Join(ansiblePackagesDir, fmt.Sprintf("%s_%s", pkg.Repo, pkg.GitHash))
		binDir := filepath.Join(pkgDir, "bin")
		binFile := filepath.Join(binDir, pkg.Repo+"-server")

		if !FileExists(binFile) {
			if err := os.MkdirAll(pkgDir, os.ModePerm); err != nil {
				return err
			}
			if err := c.downloadPackage(pkg, pkgDir); err != nil {
				return err
			}
		}

		if err := checkGitVersion(binFile, pkg.GitHash); err != nil {
			os.Remove(binFile)
			return err
		}

		cmd := fmt.Sprintf("cp -r %s %s", binDir, ansibleResoucesDir)
		if _, err := ExecCmd(c.ctx, cmd); err != nil {
			return err
		}
	}

	return nil
}

func (c *AnsibleCluster) downloadPackage(pkg *BinPackage, pkgDir string) error {
	tempDir, err := ioutil.TempDir("", "ansible-pakcages")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	// TODO : check SHA1 checksum
	tarFile := filepath.Join(tempDir, pkg.Repo+".tar.gz")
	if _, err := Download(pkg.BinUrl, tarFile); err != nil {
		return err
	}

	cmd := fmt.Sprintf("tar -zxf %s -C %s", tarFile, pkgDir)
	_, err = ExecCmd(c.ctx, cmd)
	return err
}

func checkGitVersion(binFile string, mustGitHash string) error {
	if !FileExists(binFile) {
		return fmt.Errorf("bin file not exists: %s", binFile)
	}

	binGitHash, err := exportGitVersion(binFile)
	if err != nil {
		return err
	}

	if strings.ToLower(binGitHash) != strings.ToLower(mustGitHash) {
		return fmt.Errorf("git Hash not match: %s != %s", binGitHash, mustGitHash)
	}

	return nil
}

func exportGitVersion(binFile string) (string, error) {
	output, err := ExecCmd(context.Background(), binFile+" -V")
	if err != nil {
		return output, err
	}

	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, GitHashNote) {
			continue
		}

		if parts := strings.Split(line, " "); len(parts) > 1 {
			ver := parts[len(parts)-1]
			return ver, nil
		}
	}

	return "", nil
}
