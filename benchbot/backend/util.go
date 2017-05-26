package backend

import (
	"io"
	"os"
	"strconv"
	"strings"

	"encoding/json"
	"io/ioutil"
	"net/http"
	"os/exec"
	"path/filepath"
)

func ParseInt64(val string) (int64, error) {
	if num, err := strconv.ParseInt(string(val), 10, 64); err != nil {
		return 0, err
	} else {
		return num, nil
	}
}

func FormatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}

func ReadJson(r io.ReadCloser, data interface{}) error {
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err // errors.Trace(err) // TODO : "github.com/juju/errors"
	}
	err = json.Unmarshal(b, data)
	if err != nil {
		return err // errors.Trace(err) // TODO : "github.com/juju/errors"
	}

	return nil
}

func ExecCmd(command string, output io.Writer) bool {
	// ps : Just return success or not ~
	args := strings.Split(command, " ")
	bin := args[0]
	args = args[1:]

	cmd := exec.Command(bin, args...)
	data, e := cmd.CombinedOutput()
	if output != nil {
		output.Write(data)
	}
	if e != nil {
		return (e.(*exec.ExitError)).Success()
	}
	return true
}

func Download(resourceUrl string, localPath string) (int64, error) {
	setup := func() {

	}
	setup()

	// local prepare
	os.MkdirAll(filepath.Dir(localPath), os.ModePerm)
	f, err := os.Create(localPath)
	if err != nil {
		os.Remove(localPath)
		return 0, err
	}

	// request http
	resourceUrl = strings.ToLower(resourceUrl)
	if !strings.HasPrefix(resourceUrl, "http://") {
		resourceUrl = "http://" + resourceUrl
	}

	res, err := http.Get(resourceUrl)
	if err != nil {
		os.Remove(localPath)
		return 0, err
	}

	// local save
	bytes, err := io.Copy(f, res.Body)
	res.Body.Close()

	return bytes, err
}
