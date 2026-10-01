package complexity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// runComplexityOracle returns the stdout of one oracle script of this package's testdata fed `stdin`. The
// script was executed once on the last build that carried the Python sources and its stdout is frozen in
// the golden at goldenPath (recipe in the golden's spec); a frozen run reads it and runs no Python. The
// script's text and its stdin are part of the request, so a changed script or corpus is refused until it is
// recorded again.
func runComplexityOracle(t *testing.T, goldenPath, testPattern, name, script string, stdin []byte) []byte {
	t.Helper()
	root := complexityRepositoryRoot(t)
	scriptPath := "internal/jobs/metrics/complexity/testdata/" + script
	source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scriptPath)))
	if err != nil {
		t.Fatal(err)
	}
	spec := rotguard.Spec(goldenPath, complexityGoldenPins[filepath.Base(goldenPath)], "./internal/jobs/metrics/complexity/", testPattern)
	answers := programoracle.Run(t, spec, root, []programoracle.Program{
		programoracle.Script(name, scriptPath, string(source), stdin),
	})
	if answers[0].ExitCode != 0 {
		t.Fatalf("the %s exited %d (stdout %q)", name, answers[0].ExitCode, answers[0].Stdout)
	}
	return []byte(answers[0].Stdout)
}

// configPath resolves the real complexity.yaml, five levels up from this
// package. The Go executor reads the SAME file Python reads -- embedding a
// copy would let the two drift silently the moment someone edits the yaml.
func configPath() string {
	return filepath.Join("..", "..", "..", "..",
		"src", "dev_health_ops", "config", "complexity.yaml")
}

// shouldProcessPaths are chosen to exercise the fnmatch subtleties the config
// actually depends on, not to demonstrate that globbing works.
var shouldProcessPaths = []string{
	// Root-level and nested source files. These are the reason include_globs
	// are written "*.py" and not "**/*.py": fnmatch's `*` crosses `/`, so one
	// pattern covers both depths.
	"main.py",
	"src/dev_health_ops/cli.py",
	"a/b/c/deep.py",
	"app.ts",
	"web/src/App.tsx",
	"cmd/worker/main.go",
	"lib/thing.rs",

	// Excluded trees at BOTH depths. The config pairs "**/tests/**" with
	// "tests/**" because the former does not match a top-level path -- the
	// leading "**/" requires a separator before it.
	"tests/test_thing.py",
	"src/tests/test_thing.py",
	"migrations/001_init.py",
	"src/migrations/001_init.py",
	"node_modules/pkg/index.js",
	"web/node_modules/pkg/index.js",
	"vendor/lib/x.go",
	"dist/bundle.js",
	"build/out.js",
	".next/static/x.js",
	"venv/lib/python3.14/site-packages/x.py",
	".venv/lib/python3.14/site-packages/x.py",

	// __init__.py at both depths.
	"__init__.py",
	"src/dev_health_ops/__init__.py",

	// Suffix excludes, which apply at any depth because of the crossing `*`.
	"web/app.min.js",
	"types/global.d.ts",
	"next.config.js",
	"vite.config.ts",
	"web/vite.config.mjs",

	// Extensions NOT in include_globs: rejected even though nothing excludes
	// them, because the default is DENY.
	"README.md",
	"config.yaml",
	"Makefile",
	"data.json",
	"script.sh",

	// A path that looks excluded but is not: "contests" contains "tests" as a
	// substring, and fnmatch is not a substring match.
	"contests/thing.py",
	// A file literally named like a directory pattern.
	"tests.py",
}

func TestShouldProcessMatchesLivePython(t *testing.T) {
	encoded, err := json.Marshal(shouldProcessPaths)
	if err != nil {
		t.Fatalf("encode paths: %v", err)
	}
	output := runComplexityOracle(t, "testdata/golden/should_process_oracle.json", "^TestShouldProcessMatchesLivePython$",
		"should process oracle", "python_should_process_oracle.py", encoded)

	var oracle struct {
		ConfigPath        string   `json:"config_path"`
		IncludeGlobs      []string `json:"include_globs"`
		ExcludeGlobs      []string `json:"exclude_globs"`
		HighThreshold     int      `json:"high_threshold"`
		VeryHighThreshold int      `json:"very_high_threshold"`
		Verdicts          []struct {
			Path          string `json:"path"`
			ShouldProcess bool   `json:"should_process"`
		} `json:"verdicts"`
	}
	if err := json.Unmarshal(output, &oracle); err != nil {
		t.Fatalf("parse oracle output: %v\n%s", err, output)
	}
	if len(oracle.Verdicts) != len(shouldProcessPaths) {
		t.Fatalf("oracle returned %d verdicts for %d paths",
			len(oracle.Verdicts), len(shouldProcessPaths))
	}

	config, err := LoadScanConfig(configPath())
	if err != nil {
		t.Fatalf("LoadScanConfig: %v", err)
	}

	// The glob LISTS must match first. If Go loaded a different set, the
	// per-path verdicts could still coincide on this sample while the two
	// implementations disagree on everything else.
	if len(config.IncludeGlobs) != len(oracle.IncludeGlobs) {
		t.Errorf("include_globs: go has %d, python %d", len(config.IncludeGlobs), len(oracle.IncludeGlobs))
	}
	for i := range oracle.IncludeGlobs {
		if i < len(config.IncludeGlobs) && config.IncludeGlobs[i] != oracle.IncludeGlobs[i] {
			t.Errorf("include_globs[%d]: go %q, python %q", i, config.IncludeGlobs[i], oracle.IncludeGlobs[i])
		}
	}
	if len(config.ExcludeGlobs) != len(oracle.ExcludeGlobs) {
		t.Errorf("exclude_globs: go has %d, python %d", len(config.ExcludeGlobs), len(oracle.ExcludeGlobs))
	}
	for i := range oracle.ExcludeGlobs {
		if i < len(config.ExcludeGlobs) && config.ExcludeGlobs[i] != oracle.ExcludeGlobs[i] {
			t.Errorf("exclude_globs[%d]: go %q, python %q", i, config.ExcludeGlobs[i], oracle.ExcludeGlobs[i])
		}
	}
	if config.Thresholds.High != oracle.HighThreshold {
		t.Errorf("high threshold: go %d, python %d", config.Thresholds.High, oracle.HighThreshold)
	}
	if config.Thresholds.VeryHigh != oracle.VeryHighThreshold {
		t.Errorf("very-high threshold: go %d, python %d", config.Thresholds.VeryHigh, oracle.VeryHighThreshold)
	}

	for i, want := range oracle.Verdicts {
		if want.Path != shouldProcessPaths[i] {
			t.Fatalf("verdict %d is for %q, expected %q -- results not aligned with inputs",
				i, want.Path, shouldProcessPaths[i])
		}
		if got := config.ShouldProcess(want.Path); got != want.ShouldProcess {
			t.Errorf("ShouldProcess(%q) = %v, python %v", want.Path, got, want.ShouldProcess)
		}
	}
}

func TestShouldProcessDefaultsMatchPythonGetFallbacks(t *testing.T) {
	// Python: include_globs defaults to ["**/*.py"], exclude_globs to [],
	// thresholds to 15/25. Reproducing the defaults matters because they apply
	// exactly when the config is malformed -- the moment a divergence is
	// hardest to notice.
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.yaml")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	config, err := LoadScanConfig(path)
	if err != nil {
		t.Fatalf("LoadScanConfig: %v", err)
	}
	if len(config.IncludeGlobs) != 1 || config.IncludeGlobs[0] != "**/*.py" {
		t.Errorf("include_globs default: got %v, want [**/*.py]", config.IncludeGlobs)
	}
	if len(config.ExcludeGlobs) != 0 {
		t.Errorf("exclude_globs default: got %v, want []", config.ExcludeGlobs)
	}
	if config.Thresholds != (Thresholds{High: 15, VeryHigh: 25}) {
		t.Errorf("threshold defaults: got %+v, want {15 25}", config.Thresholds)
	}
	// The default include glob is "**/*.py", which does NOT match a top-level
	// file -- the leading "**/" needs a separator. That is Python's behaviour
	// too, and it is why the real config uses "*.py" instead.
	if config.ShouldProcess("main.py") {
		t.Errorf(`"**/*.py" must not match a top-level file`)
	}
	if !config.ShouldProcess("a/main.py") {
		t.Errorf(`"**/*.py" must match a nested file`)
	}
}

func TestShouldProcessRejectsWhenNothingIncludes(t *testing.T) {
	config := ScanConfig{IncludeGlobs: []string{"*.py"}, ExcludeGlobs: nil}
	if config.ShouldProcess("README.md") {
		t.Errorf("default must be DENY for an unmatched path")
	}
}

func TestShouldProcessExcludeWinsOverInclude(t *testing.T) {
	// Excludes are checked FIRST and win outright; the order is not symmetric.
	config := ScanConfig{
		IncludeGlobs: []string{"*.py"},
		ExcludeGlobs: []string{"**/tests/**"},
	}
	if !config.ShouldProcess("src/app.py") {
		t.Errorf("an included, non-excluded path must be processed")
	}
	if config.ShouldProcess("src/tests/test_app.py") {
		t.Errorf("exclude must win over include")
	}
}

// complexityRepositoryRoot walks up to the module root, so the resolved
// interpreter can find the checked-out virtualenv.
func complexityRepositoryRoot(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}

// complexityGoldenPins holds the digest each golden of this package is pinned to (a placeholder until the
// goldenrecord verb records the golden and replaces it).
var complexityGoldenPins = map[string]string{
	"blame_maxfiles_oracle.json": "PIN:blame_maxfiles_oracle",
	"should_process_oracle.json": "PIN:should_process_oracle",
}
