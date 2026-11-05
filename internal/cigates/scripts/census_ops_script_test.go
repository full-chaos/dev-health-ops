package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The ops census (`go test` over the cigates, testsupport and httpguard packages) runs internal/cigates/scripts, which
// takes 744 s under load against the 10 m go test default. ci/check_go.sh census-ops is the repo-owned entry: it takes
// the -timeout from cigates_go_test_timeout, the same function the hosted legs use, so a caller never relies on the default.
// The cells put a fake go on PATH that records its argv and answers `test` with success; no cell runs the census.

const censusOpsArgv = "test -mod=readonly -trimpath -count=1 -timeout 20m ./internal/cigates/... ./internal/testsupport/... ./internal/httpguard/..."

const censusOpsWant = "test GOENV=off GOFLAGS=unset argv=" + censusOpsArgv + "\n"

func censusOps(t *testing.T, checkGo string, args ...string) cmdResult {
	t.Helper()
	return censusOpsEnv(t, checkGo, nil, args...)
}

func censusOpsEnv(t *testing.T, checkGo string, extra []string, args ...string) cmdResult {
	t.Helper()
	env := inheritedEnv(nil, append([]string{"DEV_HEALTH_GO_CACHE=" + t.TempDir(), "GIT_CONFIG_GLOBAL=" + os.DevNull}, extra...)...)
	return freeSpec{dir: rootOf(t), env: env}.run("bash", append([]string{checkGo, "census-ops"}, args...)...)
}

// censusFakeGo puts a go on PATH that records every `test` and `list` call with its GOENV and GOFLAGS and answers `test` with
// success; every other subcommand goes to the real go. It returns the PATH entry and the call log, so no cell runs the census.
func censusFakeGo(t *testing.T) (pathEnv, logFile string) {
	t.Helper()
	real, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	dir, logDir := t.TempDir(), t.TempDir()
	logFile = filepath.Join(logDir, "calls")
	script := "#!/bin/bash\ncase \"$1\" in test|list) printf '%s GOENV=%s GOFLAGS=%s argv=%s\\n' \"$1\" \"${GOENV-unset}\" \"${GOFLAGS-unset}\" \"$*\" >>" + logFile + " ;; esac\n" +
		"if [ \"$1\" = test ]; then exit 0; fi\nexec " + real + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return "PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"), logFile
}

func censusGoCalls(t *testing.T, logFile string) string {
	t.Helper()
	b, err := os.ReadFile(logFile)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(b)
}

func TestCensusOpsRunsExactlyOneGoTestWithTheCigatesTimeout(t *testing.T) {
	t.Parallel()
	pathEnv, logFile := censusFakeGo(t)
	res := censusOpsEnv(t, filepath.Join(rootOf(t), "ci", "check_go.sh"), []string{pathEnv})
	if calls := censusGoCalls(t, logFile); res.code != 0 || calls != censusOpsWant {
		t.Fatalf("census-ops: rc=%d stderr=%q go calls=%q, want exactly %q", res.code, res.stderr, calls, censusOpsWant)
	}
}

func TestCensusOpsTakesTheTimeoutFromTheCigatesFunction(t *testing.T) {
	t.Parallel()
	planted := plantedCheckGoRoot(t,
		"github.com/full-chaos/dev-health-ops/internal/cigates/?*) printf '20m\\n'; return 0 ;;",
		"github.com/full-chaos/dev-health-ops/internal/cigates/?*) printf '7m\\n'; return 0 ;;")
	pathEnv, logFile := censusFakeGo(t)
	res := censusOpsEnv(t, planted, []string{pathEnv})
	if want := strings.Replace(censusOpsWant, "-timeout 20m", "-timeout 7m", 1); res.code != 0 || censusGoCalls(t, logFile) != want {
		t.Fatalf("census-ops follows cigates_go_test_timeout: rc=%d stderr=%q go calls=%q, want %q", res.code, res.stderr, censusGoCalls(t, logFile), want)
	}
}

func TestCensusOpsRefusesAnyArgument(t *testing.T) {
	t.Parallel()
	pathEnv, logFile := censusFakeGo(t)
	for _, args := range [][]string{{"--bogus"}, {"--print-cmd"}} {
		res := censusOpsEnv(t, filepath.Join(rootOf(t), "ci", "check_go.sh"), []string{pathEnv}, args...)
		if res.code == 0 || !strings.Contains(res.stderr, "census-ops accepts no arguments") {
			t.Fatalf("census-ops %v: rc=%d stderr=%q, want a refusal", args, res.code, res.stderr)
		}
		if calls := censusGoCalls(t, logFile); calls != "" {
			t.Fatalf("census-ops %v refused but ran go: %q", args, calls)
		}
	}
}

func TestCensusOpsRunsGoTestWithGoenvOff(t *testing.T) {
	t.Parallel()
	pathEnv, logFile := censusFakeGo(t)
	goenv := filepath.Join(t.TempDir(), "goenv")
	if err := os.WriteFile(goenv, []byte("GOFLAGS=-run=^$\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	res := censusOpsEnv(t, filepath.Join(rootOf(t), "ci", "check_go.sh"), []string{pathEnv, "GOENV=" + goenv})
	if calls := censusGoCalls(t, logFile); res.code != 0 || calls != censusOpsWant {
		t.Fatalf("census-ops with a GOENV file: rc=%d stderr=%q go calls=%q, want exactly %q", res.code, res.stderr, calls, censusOpsWant)
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
