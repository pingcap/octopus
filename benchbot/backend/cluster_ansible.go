package backend

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/octopus/benchbot/pkg"
	"golang.org/x/net/context"
)

var (
	packagesDir            string = ""
	ansibleResoucesDir     string = ""
	ansibleDowloadsDir     string = ""
	ansibleOperationScript string = ""

	clusterDetectMaxtTimes int = 60
)

const (
	GitHashNote = "Git Commit Hash"
)

type ansibleProcess func() error

func initAnsibleEnv(cfg *ServerConfig) error {
	packagesDir = filepath.Join(cfg.Dir, "packages")
	ansibleResoucesDir = filepath.Join(cfg.Ansible.Dir, "resources")
	ansibleDowloadsDir = filepath.Join(cfg.Ansible.Dir, "downloads")
	ansibleOperationScript = filepath.Join(cfg.Ansible.Dir, "operation.sh")

	if err := os.MkdirAll(packagesDir, os.ModePerm); err != nil {
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

func exportGitVersion(binFile string) (string, error) {
	buf := new(bytes.Buffer)
	success := ExecCmd(binFile+" -V", nil, buf)
	output := string(buf.Bytes())

	if !success {
		return "", fmt.Errorf("'%s' git version export failed : %s", binFile, output)
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

func checkGitVersion(binFile string, mustGitHash string) error {
	if !FileExists(binFile) {
		return fmt.Errorf("bin file not exists : %s", binFile)
	}

	binGitHash, err := exportGitVersion(binFile)
	if err != nil {
		return err
	}

	if strings.ToLower(binGitHash) != strings.ToLower(mustGitHash) {
		return fmt.Errorf("Git Hash not match : %s != %s", binGitHash, mustGitHash)
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
	ops := []ansibleProcess{c.fetchResources, c.boostrap}
	for _, op := range ops {
		if err = op(); err != nil {
			return
		}
	}
	return
}

func (c *AnsibleCluster) fetchResources() error {
	log.Info("[Ansible] fetching resources ...")

	packages := []*BinPackage{c.meta.tidb, c.meta.tikv, c.meta.pd}
	for _, pkg := range packages {
		log.Infof("[Ansible] download pkg = %s", pkg.BinUrl)

		// TODO : add expired & auto clean
		pkgDir := filepath.Join(packagesDir, fmt.Sprintf("%s_%s", pkg.Repo, pkg.GitHash))
		pkgBinFile := filepath.Join(pkgDir, "bin", pkg.Repo+"-server")

		var localCached bool = FileExists(pkgBinFile)
		if localCached {
			if err := checkGitVersion(pkgBinFile, pkg.GitHash); err != nil {
				log.Errorf("cached bin with unexpected git hash : %s - %s", pkg.Repo, err.Error())
				os.Remove(pkgBinFile)
				localCached = false
			}
		}

		if !localCached {
			if err := os.MkdirAll(pkgDir, os.ModePerm); err != nil {
				return err
			}

			if err := c.downloadPackage(pkg, pkgDir); err != nil {
				return err
			}

			if err := checkGitVersion(pkgBinFile, pkg.GitHash); err != nil {
				log.Errorf("%s - %s", pkg.Repo, err.Error())
				os.Remove(pkgBinFile)
				return err
			}
		}

		// move bin files to ansbile specified dir for deploying
		bins := filepath.Join(pkgDir, "bin")
		cmd := fmt.Sprintf("cp -r %s %s", bins, ansibleResoucesDir)
		ExecCmd(cmd, c.ctx, nil)
	}

	return nil
}

func (c *AnsibleCluster) downloadPackage(pkg *BinPackage, saveDir string) error {
	tempDir, err := ioutil.TempDir("", "ansible-pkg-temp")
	defer os.RemoveAll(tempDir)
	if err != nil {
		return err
	}

	// TODO : check SHA1 checksum
	tarFile := filepath.Join(tempDir, pkg.Repo+".tar.gz")
	if _, err := Download(pkg.BinUrl, tarFile); err != nil {
		return err
	}

	// unarchive
	cmd := fmt.Sprintf("tar -zxf %s -C %s", tarFile, saveDir)
	if ok := ExecCmd(cmd, c.ctx, nil); !ok {
		return fmt.Errorf("unarchive failed : %s", pkg.BinUrl)
	}

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

	if c.db != nil {
		c.db.Close()
		c.db = nil
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
	return c.destory()
}

func (c *AnsibleCluster) destory() error {
	log.Info("[Ansible] destorying ...")
	buf := new(bytes.Buffer)
	if ok := c.operate("destory", buf); !ok {
		return fmt.Errorf("destory failed : %s\n", string(buf.Bytes()))
	}
	return nil
}

func (c *AnsibleCluster) Ping() error {
	if err := CheckConnection(c.db); err == nil {
		return err
	}

	if c.db != nil {
		c.db.Close()
	}

	if db, err := c.connectCluster(); err != nil {
		return err
	} else {
		c.db = db
	}

	return nil
}

func (c *AnsibleCluster) connectCluster() (*sql.DB, error) {
	var db *sql.DB
	var err error

	dsn := c.meta.dsn
	for i := 0; i < clusterDetectMaxtTimes; i++ {
		db, err = ConnectDB(dsn.user, dsn.password, dsn.host, dsn.port, dsn.db)
		if err == nil {
			return db, nil
		}

		time.Sleep(time.Second * 1)
	}

	return nil, err
}

func (c *AnsibleCluster) Close() {
	// TODO ... ?
	if c.db != nil {
		c.db.Close()
	}
}

func (c *AnsibleCluster) Accessor() *sql.DB {
	return c.db
}
