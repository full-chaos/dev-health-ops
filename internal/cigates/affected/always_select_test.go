package affected_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// always.tsv holds no package: the tests that read or run other packages' files (a DDL scan of every _test.go, `go run` of
// a command) are bounded by the walks, dynreads and edges tables, and the repros below plan them without a row.
var alwaysSelected []string

func TestAlwaysSelectListHoldsExactlyItsPackages(t *testing.T) {
	var rows []string
	for _, line := range strings.Split(readText(t, scriptPath(t, "internal/cigates/affected/always.tsv")), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			rows = append(rows, line)
		}
	}
	if len(rows) != 0 || len(alwaysSelected) != 0 {
		t.Fatalf("always.tsv rows %q, want %q", rows, alwaysSelected)
	}
}

var (
	realPlanInputs struct {
		once         sync.Once
		golist, keys string
		err          string
	}
	realPlanDir string
)

// realPlanSlots lets four real-tree plan verdicts (each one core, about 19 s) run at once: one at a time they queue the
// package past its go test timeout beside the other packages of the shard.
var realPlanSlots = make(chan struct{}, 4)

// runRealPlanVerdict is cmdSpec.run under realPlanSlots instead of the one-process cap of the helper (whose copy must stay
// byte-identical across packages).
func runRealPlanVerdict(dir string, env []string, name string, args ...string) cmdResult {
	realPlanSlots <- struct{}{}
	defer func() { <-realPlanSlots }()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = withNoPythonShims(env)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	code := 0
	if err := cmd.Run(); err != nil {
		code = -1
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		} else {
			errb.WriteString(err.Error())
		}
	}
	return cmdResult{code: code, stdout: out.String(), stderr: errb.String()}
}

func removeRealPlanDir() {
	if realPlanDir != "" {
		_ = os.RemoveAll(realPlanDir)
	}
}

// realPlan runs the integration plan verdict over the real module, as go.yml's plan step does.
func realPlan(t *testing.T, changed string) []string {
	t.Helper()
	root := repoRoot(t)
	in := &realPlanInputs
	in.once.Do(func() {
		dir, err := os.MkdirTemp("", "cigate-always-select-")
		if err != nil {
			in.err = err.Error()
			return
		}
		realPlanDir = dir
		env := inheritedEnv([]string{"GOFLAGS"}, "GOFLAGS=")
		cov := cmdSpec{dir: root, env: env}.run("bash", filepath.Join(root, "ci", "check_go.sh"), "integration-coverage")
		if cov.code != 0 {
			in.err = "integration-coverage: " + cov.stderr + cov.stdout
			return
		}
		var keys []string
		for _, m := range runLine.FindAllStringSubmatch(cov.stdout, -1) {
			keys = append(keys, m[1])
		}
		gl := cmdSpec{dir: root, env: env}.run("go", "list", "-mod=readonly", "-tags=integration", "-test", "-json", "./...")
		if gl.code != 0 {
			in.err = "go list: " + gl.stderr
			return
		}
		in.golist, in.keys = filepath.Join(dir, "golist.json"), filepath.Join(dir, "keys.txt")
		if err := os.WriteFile(in.golist, []byte(gl.stdout), 0o644); err != nil {
			in.err = err.Error()
			return
		}
		if err := os.WriteFile(in.keys, []byte(strings.Join(keys, "\n")+"\n"), 0o644); err != nil {
			in.err = err.Error()
		}
	})
	if in.err != "" {
		t.Fatal(in.err)
	}
	list := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(list, []byte(changed+"\x00"), 0o644); err != nil {
		t.Fatal(err)
	}
	res := runRealPlanVerdict(root, inheritedEnv([]string{"GOFLAGS"}, "GOFLAGS="), cigate(t), "plan-verdict", "--event", "pull_request",
		"--changed", list, "--golist", in.golist, "--keys", in.keys, "--root", root)
	if res.code != 0 {
		t.Fatalf("plan-verdict exited %d: %s", res.code, res.stderr)
	}
	return strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")
}

// One row per executed repro of a test that reads another package's files.
func TestAlwaysSelectedPackagesRunOnGoOnlyChanges(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ changed, want string }{
		// pgmigrate's hand-written-DDL guard scans every *_test.go in the module.
		{"internal/jobruntime/worker_presence_integration_test.go", "internal/pgmigrate"},
		// queryapi/server tests `go run ./cmd/registrydump`.
		{"cmd/registrydump/main.go", "internal/queryapi/server"},
		// fixturescli tests `go run ../../cmd/dho migrate`, which runs rivermigrate.
		{"internal/rivermigrate/rivermigrate.go", "internal/fixturescli"},
		{"cmd/dho/main.go", "internal/fixturescli"},
		// scancensus's LoadSites reads every queryapi/storage source file.
		{"internal/queryapi/aianalytics/ai_opportunities.go", "internal/queryapi/scancensus"},
	} {
		t.Run(c.changed, func(t *testing.T) {
			t.Parallel()
			lines := realPlan(t, c.changed)
			if !strings.HasPrefix(lines[0], "AFFECTED ") {
				t.Fatalf("a .go-only change plans %q, want AFFECTED", lines[0])
			}
			for _, k := range lines[1:] {
				if k == c.want {
					return
				}
			}
			t.Fatalf("a change to %s plans %q without %s", c.changed, lines[0], c.want)
		})
	}
}

// The planner's code, its compiled-in data and the steps that run it plan FULL: a pull request that edits them must not
// be planned by the closure it changes.
func TestPlannerInputsPlanFull(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{
		"cmd/cigate/main.go",
		"internal/cigates/affected/plan.go",
		"internal/cigates/affected/always.tsv",
		"internal/cigates/affected/reads.tsv",
		".github/workflows/go.yml",
		".github/workflows/go-quality.yml",
	} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			if lines := realPlan(t, changed); lines[0] != "FULL planner input: "+changed {
				t.Fatalf("a change to %s plans %q, want %q", changed, lines[0], "FULL planner input: "+changed)
			}
		})
	}
}
