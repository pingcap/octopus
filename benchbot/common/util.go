package pkg

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func ExecCmd(ctx context.Context, command string) (string, error) {
	args := strings.Split(command, " ")
	name := args[0]
	args = args[1:]
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func Download(resourceUrl string, localPath string) (int64, error) {
	os.MkdirAll(filepath.Dir(localPath), os.ModePerm)
	file, err := os.Create(localPath)
	if err != nil {
		os.Remove(localPath)
		return 0, err
	}

	resourceUrl = strings.ToLower(resourceUrl)
	if !strings.HasPrefix(resourceUrl, "http://") {
		resourceUrl = "http://" + resourceUrl
	}

	res, err := http.Get(resourceUrl)
	if err != nil {
		os.Remove(localPath)
		return 0, err
	}

	bytes, err := io.Copy(file, res.Body)
	res.Body.Close()

	return bytes, err
}

func FileExists(filePath string) bool {
	if _, err := os.Stat(filePath); err != nil {
		return false
	}
	return true
}
