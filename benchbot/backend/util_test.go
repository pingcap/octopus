package backend

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
)

func TestTypeConvert(t *testing.T) {
	var sz string = "1234"
	var num int64 = 1234

	if FormatInt64(num) != sz {
		t.Error("repr failed !")
	}

	if n, err := ParseInt64(sz); err != nil || n != num {
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

	if ok := ExecCmd("echo hello", nil, buf); !ok {
		t.Error("exe command failed !")
	}

	output := string(buf.Bytes())
	if output != "hello\n" {
		t.Error("exe command failed !")
	}
}

func TestHttpDownloader(t *testing.T) {
	url := "http://personal.cqc.com/test.md"
	saveTo := "./download-file"
	httpmock.Activate()
	defer func() {
		httpmock.DeactivateAndReset()
		os.Remove(saveTo)
	}()

	httpmock.RegisterResponder("GET", url,
		httpmock.NewBytesResponder(200, []byte(`** Markdown Format **`)))

	if fsize, err := Download(url, saveTo); err != nil {
		t.Error("http download failed : ", err.Error())
	} else if fsize <= 0 {
		t.Error("http download failed : file empty !")
	}

	if _, err := os.Stat(saveTo); err != nil {
		t.Error("downloaded file access failed : ", err.Error())
	}
}
