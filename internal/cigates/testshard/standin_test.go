package testshard_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Shared harness of the ported go-quality leg tests (the Go side of tests/tooling/test_go_quality_legs.py): a stand-in `go`
// that records `go test` calls, a once-per-process replay of the real providersync race listing, and runCheckGo.

const standInGo = `#!/usr/bin/env bash
# Stand-in go: go test records "<args>" and succeeds (or fails when FAKE_GO_FAIL is set); every other subcommand is the real go.
if [ "$1" != "test" ]; then exec "%[1]s" "$@"; fi
case " $* " in *" -list "*)
  printf '%%s\n' "$*" >> "${FAKE_GO_LIST_LOG}"
  # FAKE_GO_LIST_REPLAY: the real go test -race -list output, replayed only for a call that carries BOTH -race and -list .*
  if [ -n "${FAKE_GO_LIST_REPLAY:-}" ] && [[ " $* " == *" -race "* ]] && [[ " $* " == *" -list .* "* ]]; then
    cat "${FAKE_GO_LIST_REPLAY}"
  else
    "%[1]s" "$@" || exit $?
  fi
  # FAKE_GO_LIST_EXTRA appends one name to the listing, as a Fuzz/Example/race-tagged test would be.
  [ -z "${FAKE_GO_LIST_EXTRA:-}" ] || printf '%%s\n' "${FAKE_GO_LIST_EXTRA}"
  exit 0 ;;
esac
printf '%%s\n' "$*" >> "${FAKE_GO_LOG}"
[ -z "${FAKE_GO_FAIL:-}" ] || exit 1
# FAKE_GO_FAIL_ONLY fails exactly one of the two race calls of a leg: "providersync" or "packages".
case "${FAKE_GO_FAIL_ONLY:-}" in
  providersync) case " $* " in *" ./internal/providersync "*) exit 1 ;; esac ;;
  packages) case " $* " in *" ./internal/providersync "*) ;; *) exit 1 ;; esac ;;
esac
exit 0
`

var (
	listReplayOnce sync.Once
	listReplayPath string
)

// providersyncRaceListing runs the REAL `go test -race -list .* ./internal/providersync` once; the stand-in replays it.
func providersyncRaceListing(t *testing.T) string {
	t.Helper()
	listReplayOnce.Do(func() {
		dir, err := os.MkdirTemp("", "providersync-listing-")
		if err != nil {
			return
		}
		cmd := exec.Command("go", "test", "-mod=readonly", "-race", "-list", ".*", "./internal/providersync")
		cmd.Dir = repoRoot(t)
		cmd.Env = append(os.Environ(), "GOWORK=off")
		out, err := cmd.Output()
		if err != nil {
			return
		}
		listReplayPath = filepath.Join(dir, "providersync_race_list.txt")
		_ = os.WriteFile(listReplayPath, out, 0o644)
	})
	if listReplayPath == "" {
		t.Fatal("could not produce the real providersync race listing")
	}
	return listReplayPath
}

type legRun struct {
	Stdout, Stderr string
	Err            error
	Calls          []string // `go test` calls the stand-in saw (arguments after test)
	ListCalls      []string // `go test ... -list ...` calls
}

// runCheckGoIn runs ci/check_go.sh with the stand-in go on PATH, from the tree `root` (the real tree or a planted copy).
func runCheckGoIn(t *testing.T, root string, extraEnv []string, fail bool, args ...string) legRun {
	t.Helper()
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fmt.Sprintf(standInGo, realGo)), 0o755); err != nil {
		t.Fatal(err)
	}
	log, listLog := filepath.Join(dir, "go_test.log"), filepath.Join(dir, "go_list.log")
	for _, f := range []string{log, listLog} {
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_GO_LOG="+log, "FAKE_GO_LIST_LOG="+listLog, "TMPDIR="+filepath.Join(dir, "tmp"),
		"FAKE_GO_LIST_REPLAY="+providersyncRaceListing(t))
	env = append(env, extraEnv...)
	if fail {
		env = append(env, "FAKE_GO_FAIL=1")
	}
	cmd := exec.Command("bash", filepath.Join(root, "ci", "check_go.sh"))
	cmd.Args = append(cmd.Args, args...)
	cmd.Dir = root
	cmd.Env = env
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	runErr := cmd.Run()
	lines := func(p string) []string {
		b, _ := os.ReadFile(p)
		var r []string
		for _, l := range strings.Split(string(b), "\n") {
			if l != "" {
				r = append(r, l)
			}
		}
		return r
	}
	return legRun{out.String(), errb.String(), runErr, lines(log), lines(listLog)}
}

func runCheckGo(t *testing.T, extraEnv []string, fail bool, args ...string) legRun {
	t.Helper()
	return runCheckGoIn(t, repoRoot(t), extraEnv, fail, args...)
}
