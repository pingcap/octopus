package pkg

import (
	"bytes"
	"encoding/json"
)

func DumpJSON(obj interface{}, format bool) (string, error) {
	if data, err := json.Marshal(obj); err != nil {
		return "", err
	} else {
		if format {
			var out bytes.Buffer
			json.Indent(&out, data, "", "\t")
			return string(out.Bytes()), nil
		} else {
			return string(data), nil
		}
	}
}
