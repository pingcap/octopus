package backend

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestTypeConvert(t *testing.T) {
	var sz string = "1234"
	var num int64 = 1234

	if Repr(num) != sz {
		t.Error("repr failed !")
	}

	if n, err := Numeric(sz); err != nil || n != num {
		t.Error("numeric failed !")
	}
}

func TestJsonReader(t *testing.T) {
	jsonData := `{
		"id" : 123,
		"name" : "abc",
		"nations" : ["CN", "USA", "JP"]
	}`

	out := make(map[string]interface{})
	stream := ioutil.NopCloser(strings.NewReader(jsonData))

	if err := ReadJson(stream, &out); err != nil {
		t.Error("json read failed : ", err.Error())
	}

	if len(out) != 3 {
		t.Error("json read failed : feilds count incorrect !")
	}
}

func TestSysCmdExecutor(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0))

	if ok := ExecCmd("echo hello", buf); !ok {
		t.Error("exe command failed !")
	}

	output := string(buf.Bytes())
	if output != "hello\n" {
		t.Error("exe command failed !")
	}
}

func TestHttpDownloader(t *testing.T) {
	saveTo := "./logo.png"
	defer os.Remove(saveTo)

	if fsize, err := Download("http://www.baidu.com/img/bd_logo1.png", saveTo); err != nil {
		t.Error("http download failed : ", err.Error())
	} else if fsize <= 0 {
		t.Error("http download failed : file empty !")
	}

	if _, err := os.Stat(saveTo); err != nil {
		t.Error("downloaded file access failed : ", err.Error())
	}
}
