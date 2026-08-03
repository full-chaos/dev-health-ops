package providersync

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestGitLabCICDDefaultMaxMatchesActivePythonCaller(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Join(packageDir, "..", "..")
	output, err := exec.Command(
		python,
		filepath.Join(packageDir, "testdata", "python_gitlab_cicd_window_oracle.py"),
		filepath.Join(root, "src", "dev_health_ops", "processors", "gitlab.py"),
		filepath.Join(root, "src", "dev_health_ops", "utils.py"),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("execute active Python pipeline-window oracle: %v: %s", err, output)
	}
	var actual struct {
		MaxPipelines int `json:"max_pipelines"`
	}
	if err := json.Unmarshal(output, &actual); err != nil {
		t.Fatalf("decode active Python pipeline-window oracle: %v: %s", err, output)
	}
	if defaultGitLabCICDMaxPipelines != actual.MaxPipelines {
		t.Fatalf(
			"Go default max pipelines=%d, active Python process_gitlab_project max=%d",
			defaultGitLabCICDMaxPipelines, actual.MaxPipelines,
		)
	}
}
