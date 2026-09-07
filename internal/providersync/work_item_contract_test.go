package providersync

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-5351: TestWorkItemCompatibilityContractComesFromLivePythonAdapter used
// to live here, executing testdata/python_work_item_contract_oracle.py
// against dataset_adapters.py's _work_item_kwargs/_github_work_item_options
// to pin the github/gitlab include_issues/include_pull_requests/
// fetch_comments/fetch_milestones/comments_limit contract. Both Python
// functions are deleted along with run_work_items_sync_job and the Celery
// dataset-unit dispatch that called them (dataset_adapters.run_dataset_unit's
// work-items branch) -- there is no longer a Python "work item kwargs"
// contract to assert on; the native provider-sync route
// (cmd/dev-health-worker/provider_sync.go's work-items dataset case, one Go
// implementation per provider) is production now. Deleted the oracle script
// (testdata/python_work_item_contract_oracle.py) and this test with it.
//
// TestWorkItemCompatibilityOracleRejectsUnexpectedSource and
// TestWorkItemCompatibilityOracleIgnoresSiblingPythonPath below used that
// SAME script only as a vehicle to exercise python_oracle_loader.py's
// generic security properties (ALLOWED_MODULES identity rejection,
// PYTHONPATH sibling-injection immunity) -- properties of the shared loader,
// not of work-items specifically, and this package's only tests of them
// (`rg` confirmed no equivalent for the other two live oracle scripts).
// Repointed to python_launchdarkly_normalization_oracle.py /
// processors/launchdarkly.py instead of deleting: that oracle is unrelated
// to this ticket, still live, and exercises load_live_module the identical
// way (single source-file argv[1]).

func TestPythonOracleLoaderRejectsUnexpectedSource(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	oracleScript := filepath.Join(packageDir, "testdata", "python_launchdarkly_normalization_oracle.py")
	unexpectedSource := filepath.Join(
		packageDir, "..", "..", "src", "dev_health_ops", "processors", "__init__.py",
	)
	output, err := exec.Command(python, oracleScript, unexpectedSource).CombinedOutput()
	if err == nil || !strings.Contains(string(output), "unexpected oracle source") {
		t.Fatalf("unexpected source error=%v output=%s", err, output)
	}
}

func TestPythonOracleLoaderIgnoresSiblingPythonPath(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Join(packageDir, "..", "..")
	oracleScript := filepath.Join(packageDir, "testdata", "python_launchdarkly_normalization_oracle.py")
	adapterSource := filepath.Join(root, "src", "dev_health_ops", "processors", "launchdarkly.py")

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
		filepath.Join(siblingPackage, "__init__.py"):                   malicious,
		filepath.Join(siblingPackage, "processors", "__init__.py"):     {},
		filepath.Join(siblingPackage, "processors", "launchdarkly.py"): malicious,
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
		t.Fatalf("execute Python launchdarkly oracle: %v: %s", err, output)
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
		Canonical        bool     `json:"canonical"`
		HostileTouches   []string `json:"hostile_touches"`
		Origin           string   `json:"origin"`
		PreloadedModules []string `json:"preloaded_modules"`
		ReusedModules    []string `json:"reused_modules"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode Python oracle loader probe: %v: %s", err, output)
	}
	if !result.Canonical || len(result.HostileTouches) != 0 ||
		len(result.PreloadedModules) != 21 || len(result.ReusedModules) != 0 {
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

func TestParityOraclesRunWithoutSQLAlchemy(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Join(packageDir, "..", "..")

	pythonCommand := func(withoutSite bool, args ...string) *exec.Cmd {
		commandArgs := args
		if withoutSite {
			commandArgs = append([]string{"-S"}, args...)
		}
		command := exec.Command(python, commandArgs...)
		command.Env = make([]string, 0, len(os.Environ()))
		for _, value := range os.Environ() {
			if !strings.HasPrefix(value, "PYTHONPATH=") &&
				!strings.HasPrefix(value, "PYTHONHOME=") {
				command.Env = append(command.Env, value)
			}
		}
		return command
	}

	output, err := pythonCommand(
		true,
		"-c",
		"import importlib.util; assert importlib.util.find_spec('sqlalchemy') is None",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("minimal Python unexpectedly exposes sqlalchemy: %v: %s", err, output)
	}

	for _, test := range []struct {
		name   string
		script string
		source []string
	}{
		{
			name:   "provider budget parity",
			script: "python_provider_budget_oracle.py",
			source: []string{
				filepath.Join(root, "src", "dev_health_ops", "providers", "github", "budget.py"),
				filepath.Join(root, "src", "dev_health_ops", "providers", "gitlab", "budget.py"),
				filepath.Join(root, "src", "dev_health_ops", "providers", "linear", "budget.py"),
				filepath.Join(root, "src", "dev_health_ops", "providers", "jira", "budget.py"),
				filepath.Join(root, "src", "dev_health_ops", "providers", "launchdarkly", "budget.py"),
			},
		},
		{
			name:   "LaunchDarkly normalization parity",
			script: "python_launchdarkly_normalization_oracle.py",
			source: []string{
				filepath.Join(root, "src", "dev_health_ops", "processors", "launchdarkly.py"),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append(
				[]string{filepath.Join(packageDir, "testdata", test.script)},
				test.source...,
			)
			defaultOutput, err := pythonCommand(false, args...).CombinedOutput()
			if err != nil {
				t.Fatalf("execute default-Python oracle: %v: %s", err, defaultOutput)
			}
			if len(bytes.TrimSpace(defaultOutput)) == 0 || !json.Valid(defaultOutput) {
				t.Fatalf("default-Python oracle returned invalid JSON: %s", defaultOutput)
			}
			minimalOutput, err := pythonCommand(true, args...).CombinedOutput()
			if err != nil {
				t.Fatalf("execute minimal-Python oracle: %v: %s", err, minimalOutput)
			}
			if len(bytes.TrimSpace(minimalOutput)) == 0 || !json.Valid(minimalOutput) {
				t.Fatalf("minimal-Python oracle returned invalid JSON: %s", minimalOutput)
			}
			if !bytes.Equal(minimalOutput, defaultOutput) {
				t.Fatalf(
					"minimal-Python oracle output differs from default:\nminimal=%s\ndefault=%s",
					minimalOutput,
					defaultOutput,
				)
			}
		})
	}
}

func TestPythonOracleLoaderHasNoCallerControlledExecutionOrSubprocess(t *testing.T) {
	_, currentFile, _, _ := runtime.Caller(0)
	loaderPath := filepath.Join(filepath.Dir(currentFile), "testdata", "python_oracle_loader.py")
	contents, err := os.ReadFile(loaderPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"importlib.import_module",
		"__import__(",
		"import subprocess",
		"subprocess.",
		"builtins.exec",
		"builtins.compile",
		"\nexec(",
		"\ncompile(",
	} {
		if strings.Contains(string(contents), forbidden) {
			t.Fatalf("oracle loader must not use %q", forbidden)
		}
	}
}
