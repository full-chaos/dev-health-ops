package providersync

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

type workItemContractVariant struct {
	SyncPRs             bool  `json:"sync_prs"`
	IncludeIssues       *bool `json:"include_issues"`
	IncludePullRequests *bool `json:"include_pull_requests"`
	HasFetchComments    bool  `json:"has_fetch_comments"`
	HasFetchMilestones  bool  `json:"has_fetch_milestones"`
}

type workItemContractEntry struct {
	Variants []workItemContractVariant `json:"variants"`
}

func TestWorkItemCompatibilityContractComesFromLivePythonAdapter(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	adapterSource := filepath.Join(
		packageDir, "..", "..", "src", "dev_health_ops", "processors", "dataset_adapters.py",
	)
	oracleScript := filepath.Join(packageDir, "testdata", "python_work_item_contract_oracle.py")
	output, err := exec.Command(python, oracleScript, adapterSource).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python work-item oracle: %v: %s", err, output)
	}
	var contract map[string]map[string]workItemContractEntry
	if err := json.Unmarshal(output, &contract); err != nil {
		t.Fatalf("decode Python work-item oracle: %v: %s", err, output)
	}
	for _, provider := range []string{"github", "gitlab"} {
		for _, dataset := range []string{
			"work-items",
			"work-item-labels",
			"work-item-projects",
			"work-item-history",
			"work-item-comments",
		} {
			entry, ok := contract[provider][dataset]
			if !ok || len(entry.Variants) != 2 {
				t.Fatalf("%s/%s entry=%+v", provider, dataset, entry)
			}
			for index, variant := range entry.Variants {
				if variant.HasFetchComments || variant.HasFetchMilestones {
					t.Fatalf("%s/%s unexpectedly partitions full work-item job: %+v", provider, dataset, variant)
				}
				if provider == "github" {
					if variant.IncludeIssues == nil || !*variant.IncludeIssues ||
						variant.IncludePullRequests == nil ||
						*variant.IncludePullRequests != (index == 1) {
						t.Fatalf("%s/%s variant=%+v", provider, dataset, variant)
					}
				} else if variant.IncludeIssues != nil || variant.IncludePullRequests != nil {
					t.Fatalf("%s/%s GitLab flags drifted: %+v", provider, dataset, variant)
				}
			}
		}
	}
}

func TestWorkItemCompatibilityOracleRejectsUnexpectedSource(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	oracleScript := filepath.Join(packageDir, "testdata", "python_work_item_contract_oracle.py")
	unexpectedSource := filepath.Join(
		packageDir, "..", "..", "src", "dev_health_ops", "processors", "__init__.py",
	)
	output, err := exec.Command(python, oracleScript, unexpectedSource).CombinedOutput()
	if err == nil || !strings.Contains(string(output), "unexpected oracle source") {
		t.Fatalf("unexpected source error=%v output=%s", err, output)
	}
}

func TestWorkItemCompatibilityOracleIgnoresSiblingPythonPath(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Join(packageDir, "..", "..")
	oracleScript := filepath.Join(packageDir, "testdata", "python_work_item_contract_oracle.py")
	adapterSource := filepath.Join(root, "src", "dev_health_ops", "processors", "dataset_adapters.py")

	siblingSourceRoot := filepath.Join(t.TempDir(), "src")
	siblingPackage := filepath.Join(siblingSourceRoot, "dev_health_ops")
	if err := os.MkdirAll(filepath.Join(siblingPackage, "processors"), 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(t.TempDir(), "sibling-module-executed")
	malicious := []byte(
		"from pathlib import Path\nPath(" + strconv.Quote(sentinel) + ").write_text('executed')\n",
	)
	for path, content := range map[string][]byte{
		filepath.Join(siblingPackage, "__init__.py"):                       malicious,
		filepath.Join(siblingPackage, "processors", "__init__.py"):         {},
		filepath.Join(siblingPackage, "processors", "dataset_adapters.py"): malicious,
	} {
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	command := exec.Command(python, oracleScript, adapterSource)
	command.Env = make([]string, 0, len(os.Environ())+1)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "PYTHONPATH=") {
			command.Env = append(command.Env, value)
		}
	}
	command.Env = append(
		command.Env,
		"PYTHONPATH="+strings.Join(
			[]string{siblingSourceRoot, filepath.Join(root, "src")},
			string(os.PathListSeparator),
		),
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python work-item oracle: %v: %s", err, output)
	}
	if _, err := os.Stat(sentinel); err == nil || !os.IsNotExist(err) {
		t.Fatalf("sibling module executed: stat error=%v output=%s", err, output)
	}
}

func TestPythonOracleLoaderPurgesForgedAndHostilePreloads(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	adapterSource := filepath.Join(
		packageDir, "..", "..", "src", "dev_health_ops", "processors", "dataset_adapters.py",
	)
	probeScript := filepath.Join(
		packageDir, "testdata", "python_oracle_loader_probe.py",
	)
	output, err := exec.Command(python, probeScript, adapterSource).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python oracle loader probe: %v: %s", err, output)
	}
	var result struct {
		Canonical      bool     `json:"canonical"`
		HostileTouches []string `json:"hostile_touches"`
		Origin         string   `json:"origin"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode Python oracle loader probe: %v: %s", err, output)
	}
	if !result.Canonical || len(result.HostileTouches) != 0 {
		t.Fatalf("unsafe Python oracle resolution: %+v", result)
	}
	expectedOrigin, err := filepath.EvalSymlinks(adapterSource)
	if err != nil {
		t.Fatal(err)
	}
	if origin, err := filepath.EvalSymlinks(result.Origin); err != nil ||
		origin != expectedOrigin {
		t.Fatalf("Python oracle origin=%q err=%v", result.Origin, err)
	}
}
