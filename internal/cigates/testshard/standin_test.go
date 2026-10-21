package testshard_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Shared harness of the ported go-quality leg tests (the Go side of tests/tooling/test_go_quality_legs.py): a stand-in `go`
// that records `go test` calls, a once-per-process replay of the real providersync race listing, and runCheckGo.

const standInGo = `#!/usr/bin/env bash
# Stand-in go: go test records "<args>" and succeeds (or fails when FAKE_GO_FAIL is set); every other subcommand is the real go.
# A go list run in the real tree (FAKE_GO_LIST_CACHE_ROOT, which no test changes) is the real go's output, kept per directory,
# arguments and whole environment for the test process (FAKE_GO_LIST_CACHE): a check_go.sh run lists the packages several times.
# Left out of the key: the shell's own _, SHLVL and OLDPWD, the per-test TMPDIR and FAKE_GO_* paths of this harness, and the
# per-test directory of this stand-in at the head of PATH (the rest of PATH is kept).
if [ "$1" = "list" ] && [ -n "${FAKE_GO_LIST_CACHE:-}" ]; then
  case "${PWD}/" in "${FAKE_GO_LIST_CACHE_ROOT}"/*)
    key="$({ printf '%%s\0' "${PWD}" "$@" "PATH=${PATH#"${0%%/*}:"}"; env -0 | grep -zv '^\(_\|SHLVL\|OLDPWD\|TMPDIR\|PATH\|FAKE_GO_[A-Z_]*\)=' | LC_ALL=C sort -z; } | sha256sum | cut -c1-64)"
    if [ -f "${FAKE_GO_LIST_CACHE}/${key}" ]; then cat "${FAKE_GO_LIST_CACHE}/${key}"; exit 0; fi
    tmp="$(mktemp "${FAKE_GO_LIST_CACHE}/.${key}.XXXXXX")" || exit 1
    "%[1]s" "$@" > "${tmp}"; rc=$?
    cat "${tmp}"
    if [ "${rc}" -eq 0 ]; then mv -f "${tmp}" "${FAKE_GO_LIST_CACHE}/${key}"; else rm -f "${tmp}"; fi
    exit "${rc}" ;;
  esac
fi
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
case " $* " in *" -json "*) printf '{"Time":"2026-01-01T00:00:00Z","Action":"pass","Package":"p","Test":"TestFake","Elapsed":0.01}\n' ;; esac
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
	listReplayData []byte
	// goListCache is the stand-in's go list cache for this test process (removed when it ends).
	goListCache string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "testshard-golist-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	goListCache = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// providersyncListUnset and providersyncListArgs are the race leg's providersync listing call: the variables it unsets (GO_ENV_OFF)
// and its go arguments.
var (
	providersyncListUnset = []string{"GO_PROVIDER_ROUTES", "DEV_HEALTH_ENV", "GOFLAGS", "GOEXPERIMENT"}
	providersyncListArgs  = []string{"test", "-mod=readonly", "-trimpath", "-race", "-list", ".*", "./internal/providersync"}
)

// providersyncGoEnv is the race leg's go environment for a providersync call (GO_ENV_OFF) plus GOWORK=off.
func providersyncGoEnv(t *testing.T, goArgs ...string) *exec.Cmd {
	t.Helper()
	args := []string{}
	for _, name := range providersyncListUnset {
		args = append(args, "-u", name)
	}
	cmd := exec.Command("env", append(append(args, "GOWORK=off", "go"), goArgs...)...)
	cmd.Dir = repoRoot(t)
	return cmd
}

// providersyncRealRaceListing runs the race leg's own call (check_go.sh check_providersync_race_shard). It compiles the race test
// binary, so only the //go:build race oracle calls it: in a race leg that call has already built it.
func providersyncRealRaceListing(t *testing.T) []byte {
	t.Helper()
	out, err := providersyncGoEnv(t, providersyncListArgs...).Output()
	if err != nil {
		t.Fatalf("go test -race -list: %v", err)
	}
	return out
}

// providersyncRaceListing returns the race listing the stand-in replays: the Test names of the race build's test files, one per
// line, produced once per process without compiling anything (the race leg lists ^Test names only; the oracle in
// providersynclist_race_test.go holds this set equal to the real listing).
func providersyncRaceListing(t *testing.T) []byte {
	t.Helper()
	listReplayOnce.Do(func() {
		files, err := providersyncRaceTestFiles(t)
		if err != nil {
			return
		}
		listReplayData = []byte(strings.Join(providersyncTestNames(files), "\n") + "\n")
	})
	if len(listReplayData) == 0 {
		t.Fatal("could not derive the providersync race listing")
	}
	return listReplayData
}

// providersyncRaceTestFiles returns path -> text of every test file of the providersync race build (go list -race applies the race tag).
func providersyncRaceTestFiles(t *testing.T) (map[string]string, error) {
	t.Helper()
	out, err := providersyncGoEnv(t, "list", "-mod=readonly", "-trimpath", "-race", "-f",
		"{{.Dir}}{{range .TestGoFiles}}\n{{.}}{{end}}{{range .XTestGoFiles}}\n{{.}}{{end}}", "./internal/providersync").Output()
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	files := map[string]string{}
	for _, name := range lines[1:] {
		b, err := os.ReadFile(filepath.Join(lines[0], name))
		if err != nil {
			return nil, err
		}
		files[name] = string(b)
	}
	return files, nil
}

// providersyncTestFunc is what go test lists as a test: func Test<not lowercase>...(x *testing.T), the parameters possibly on several lines. TestMain takes *testing.M and is not listed.
var providersyncTestFunc = regexp.MustCompile(`(?m)^func (Test(?:[^a-z(\s]\w*)?)\(([^)]*)\)`)

// providersyncTestNames returns the sorted, unique test names found in the files' text.
func providersyncTestNames(files map[string]string) []string {
	seen := map[string]bool{}
	for _, text := range files {
		for _, m := range providersyncTestFunc.FindAllStringSubmatch(text, -1) {
			if strings.Contains(m[2], "*testing.T") {
				seen[m[1]] = true
			}
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
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
	replay := filepath.Join(dir, "providersync_race_list.txt")
	if err := os.WriteFile(replay, providersyncRaceListing(t), 0o644); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_GO_LOG="+log, "FAKE_GO_LIST_LOG="+listLog, "TMPDIR="+filepath.Join(dir, "tmp"),
		"GO_TEST_JSON_DIR="+filepath.Join(dir, "json"), "FAKE_GO_LIST_REPLAY="+replay, "FAKE_GO_LIST_CACHE="+goListCache, "FAKE_GO_LIST_CACHE_ROOT="+repoRoot(t))
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
