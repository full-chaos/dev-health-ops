package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The ops census (`go test` over the cigates, testsupport and httpguard packages) runs internal/cigates/scripts, which
// takes 744 s under load against the 10 m go test default. ci/check_go.sh census-ops is the repo-owned entry: it takes
// the -timeout from cigates_go_test_timeout, the same function the hosted legs use, so a caller never relies on the default.
// --print-cmd prints the go test argv and runs nothing; the cells read that line, never a clock.

const censusOpsWant = "go test -mod=readonly -trimpath -count=1 -timeout 20m ./internal/cigates/... ./internal/testsupport/... ./internal/httpguard/..."

// censusOpsLine: check_go.sh prints its module list before any verb runs; the verb line is the last one.
func censusOpsLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}

func censusOps(t *testing.T, checkGo string, args ...string) cmdResult {
	t.Helper()
	env := inheritedEnv(nil, "DEV_HEALTH_GO_CACHE="+t.TempDir(), "GIT_CONFIG_GLOBAL="+os.DevNull)
	return freeSpec{dir: rootOf(t), env: env}.run("bash", append([]string{checkGo, "census-ops"}, args...)...)
}

func TestCensusOpsPrintsTheCensusCommandWithTheCigatesTimeout(t *testing.T) {
	t.Parallel()
	res := censusOps(t, filepath.Join(rootOf(t), "ci", "check_go.sh"), "--print-cmd")
	if res.code != 0 || censusOpsLine(res.stdout) != censusOpsWant {
		t.Fatalf("census-ops --print-cmd: rc=%d stdout=%q stderr=%q, want rc 0 and %q", res.code, res.stdout, res.stderr, censusOpsWant)
	}
}

func TestCensusOpsTakesTheTimeoutFromTheCigatesFunction(t *testing.T) {
	t.Parallel()
	planted := plantedCheckGoRoot(t,
		"github.com/full-chaos/dev-health-ops/internal/cigates/?*) printf '20m\\n'; return 0 ;;",
		"github.com/full-chaos/dev-health-ops/internal/cigates/?*) printf '7m\\n'; return 0 ;;")
	res := censusOps(t, planted, "--print-cmd")
	if want := strings.Replace(censusOpsWant, "-timeout 20m", "-timeout 7m", 1); res.code != 0 || censusOpsLine(res.stdout) != want {
		t.Fatalf("census-ops follows cigates_go_test_timeout: rc=%d stdout=%q stderr=%q, want %q", res.code, res.stdout, res.stderr, want)
	}
}

func TestCensusOpsRefusesAnythingButPrintCmd(t *testing.T) {
	t.Parallel()
	res := censusOps(t, filepath.Join(rootOf(t), "ci", "check_go.sh"), "--bogus")
	if res.code == 0 || !strings.Contains(res.stderr, "census-ops accepts only an optional --print-cmd") {
		t.Fatalf("census-ops --bogus: rc=%d stderr=%q, want a refusal naming --print-cmd", res.code, res.stderr)
	}
}

// plantedCheckGoRoot returns a ci/check_go.sh with one planted edit inside a scratch root that links every other entry of
// the real root and of the real ci directory, so the script finds its siblings through its own location.
func plantedCheckGoRoot(t *testing.T, old, repl string) string {
	t.Helper()
	root, scratch := rootOf(t), t.TempDir()
	link := func(from, to string, skip string) {
		entries, err := os.ReadDir(from)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.Name() == skip {
				continue
			}
			if err := os.Symlink(filepath.Join(from, e.Name()), filepath.Join(to, e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.Mkdir(filepath.Join(scratch, "ci"), 0o755); err != nil {
		t.Fatal(err)
	}
	link(root, scratch, "ci")
	link(filepath.Join(root, "ci"), filepath.Join(scratch, "ci"), "check_go.sh")
	planted := mutatedCopy(t, filepath.Join(root, "ci", "check_go.sh"), old, repl)
	dst := filepath.Join(scratch, "ci", "check_go.sh")
	if err := os.Rename(planted, dst); err != nil {
		t.Fatal(err)
	}
	return dst
}
