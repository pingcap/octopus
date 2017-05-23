package backend

import (
	//"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"os/exec"
	"path/filepath"
)

/*
func ReadJSON(r io.ReadCloser, data interface{}) error {
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
*/

/*
func GetCurrentDir() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	return strings.Replace(dir, "\\", "/", -1)
}
*/

func ExecCmd(command string, output io.Writer) bool {
	//	ps : Just return success or not ~
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
	//	local prepare
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

	//	local save
	bytes, _ := io.Copy(f, res.Body)
	//fmt.Printf("download %d bytes\n", bytes)

	return bytes, nil
}

func GetMD5String(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func GenHexUUID() string {
	b := make([]byte, 48)

	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return GetMD5String(base64.URLEncoding.EncodeToString(b))
}

func GenIncrUUID() string {
	return ""
}

func GenTimestampUUID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

func GenUUID() string {
	//datetime := time.Now().Format("2000-01-01 23:59:59")
	//return datetime
	return GenTimestampUUID()
}
