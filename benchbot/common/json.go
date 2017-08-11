package pkg

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
)

func ReadJSON(r io.ReadCloser, data interface{}) error {
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, data)
}

func DumpJSON(obj interface{}, format bool) (string, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	if format {
		var out bytes.Buffer
		json.Indent(&out, data, "", "\t")
		data = out.Bytes()
	}
	return string(data), nil
}
