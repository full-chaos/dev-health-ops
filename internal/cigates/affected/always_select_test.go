package affected_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/affected"
)

// Packages whose tests read or run other packages' files (a DDL scan of every _test.go, `go run` of a command) are
// selected by every .go-only integration plan: the import closure cannot see those reads.
var alwaysSelected = []string{"internal/fixturescli", "internal/pgmigrate", "internal/queryapi/server"}

func TestAlwaysSelectListHoldsExactlyItsPackages(t *testing.T) {
	var rows []string
	for _, line := range strings.Split(readText(t, scriptPath(t, "internal/cigates/affected/always.tsv")), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			rows = append(rows, line)
		}
	}
	if !reflect.DeepEqual(rows, alwaysSelected) {
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
	var out, errb bytes.Buffer
	if code := affected.PlanVerdict([]string{"--event", "pull_request", "--changed", list, "--golist", in.golist,
		"--keys", in.keys, "--root", root}, &out, &errb); code != 0 {
		t.Fatalf("plan-verdict exited %d: %s", code, errb.String())
	}
	return strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
}

// One row per executed repro of a test that reads another package's files.
func TestAlwaysSelectedPackagesRunOnGoOnlyChanges(t *testing.T) {
	for _, c := range []struct{ changed, want string }{
		// pgmigrate's hand-written-DDL guard scans every *_test.go in the module.
		{"internal/jobruntime/worker_presence_integration_test.go", "internal/pgmigrate"},
		// queryapi/server tests `go run ./cmd/registrydump`.
		{"cmd/registrydump/main.go", "internal/queryapi/server"},
		// fixturescli tests `go run ../../cmd/dho migrate`, which runs rivermigrate.
		{"internal/rivermigrate/rivermigrate.go", "internal/fixturescli"},
		{"cmd/dho/main.go", "internal/fixturescli"},
	} {
		t.Run(c.changed, func(t *testing.T) {
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
