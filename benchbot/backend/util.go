package backend

import (
	//"fmt"
	//"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"strconv"
	"time"

	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
)

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
