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
	"golang.org/x/net/context"
)

var (
	packagesDir            string = ""
	ansibleResoucesDir     string = ""
	ansibleDowloadsDir     string = ""
	ansibleOperationScript string = ""

	clusterDetectMaxtTimes int = 60
)

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

func checkGitVersion(binFile string, mustGitHash string) error {
	if !FileExists(binFile) {
		return fmt.Errorf("bin file not exists : %s", binFile)
	}

	buf := new(bytes.Buffer)
	if ok := ExecCmd(binFile+" -V", nil, buf); !ok {
		return fmt.Errorf("load bin version failed : ", string(buf.Bytes()))
	}

	output := string(buf.Bytes())
	// TODO : use reg to search
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "Git Commit Hash:") {
			continue
		}

		if parts := strings.Split(line, " "); len(parts) > 1 {
			binGitHash := parts[len(parts)-1]
			if strings.ToLower(binGitHash) != strings.ToLower(mustGitHash) {
				return fmt.Errorf("Git Hash not match : %s != %s", binGitHash, mustGitHash)
			} else {
				return nil
			}
		}
		break
	}

	return fmt.Errorf("Git Hash not found : output = (%s)", output)
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
