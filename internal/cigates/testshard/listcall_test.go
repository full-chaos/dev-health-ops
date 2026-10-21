package testshard_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// The harness lists providersync with the race leg's own call (check_providersync_race_shard): the same flags and the same go
// environment, so it reuses the build the leg made of the race test binary. A call with other flags compiles that binary again
// (about 100 s cold, the cost that timed out the test leg).

var (
	legGoEnvOff     = regexp.MustCompile(`(?m)^GO_ENV_OFF=\(env((?: -u [A-Z_]+)+)\)$`)
	legProvidersync = regexp.MustCompile(`(?m)^check_providersync_race_shard\(\) \{\n(?:.*\n)*?.*"\$\{GO_ENV_OFF\[@\]\}" GOWORK=off go (test [^)]*-list[^)]*)\)`)
)

// legListCall returns the variables the leg unsets and the go arguments of its providersync listing, from the text of check_go.sh.
func legListCall(t *testing.T, source string) ([]string, []string) {
	t.Helper()
	env := legGoEnvOff.FindStringSubmatch(source)
	if env == nil {
		t.Fatal("ci/check_go.sh: no GO_ENV_OFF=(env -u ...) line")
	}
	unset := strings.Fields(strings.ReplaceAll(env[1], "-u ", ""))
	call := legProvidersync.FindStringSubmatch(source)
	if call == nil {
		t.Fatal("ci/check_go.sh: check_providersync_race_shard holds no \"${GO_ENV_OFF[@]}\" GOWORK=off go test ... -list call")
	}
	return unset, strings.Fields(strings.ReplaceAll(call[1], "'", ""))
}

func TestTheHarnessListsProvidersyncWithTheLegsOwnCall(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	unset, args := legListCall(t, string(b))
	if !reflect.DeepEqual(args, providersyncListArgs) {
		t.Errorf("the harness lists providersync with go %v; the race leg with go %v", providersyncListArgs, args)
	}
	if !reflect.DeepEqual(unset, providersyncListUnset) {
		t.Errorf("the harness unsets %v; the race leg (GO_ENV_OFF) unsets %v", providersyncListUnset, unset)
	}
}

func TestTheHarnessListCallGuardPlantedDefectsAreCaught(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(b)
	for _, c := range []struct{ name, from, to string }{
		{"the leg drops -trimpath", "go test -mod=readonly -trimpath -race -list '.*' ./internal/providersync", "go test -mod=readonly -race -list '.*' ./internal/providersync"},
		{"the leg unsets one more variable", "GO_ENV_OFF=(env -u GO_PROVIDER_ROUTES", "GO_ENV_OFF=(env -u GOTOOLCHAIN -u GO_PROVIDER_ROUTES"},
	} {
		planted := strings.Replace(source, c.from, c.to, 1)
		if planted == source {
			t.Fatalf("%s: the text to plant is not in ci/check_go.sh", c.name)
		}
		unset, args := legListCall(t, planted)
		if reflect.DeepEqual(args, providersyncListArgs) && reflect.DeepEqual(unset, providersyncListUnset) {
			t.Errorf("%s: the guard still holds", c.name)
		}
	}
}

// The stand-in's go list cache keys on the directory, the whole environment and the arguments, serves a repeated call without the real
// go, caches only in the real tree and only a call that succeeded.
func TestTheStandInGoListCacheKeysOnDirectoryEnvironmentAndArguments(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	fakeGo := filepath.Join(dir, "realgo")
	// the real go stands in as a script that prints what it was given and counts its calls; FAIL_LIST makes it fail
	script := "#!/usr/bin/env bash\nprintf x >> " + calls + "\nprintf '%s|%s|%s\\n' \"$PWD\" \"$*\" \"$(env | grep -v '^_=' | LC_ALL=C sort | sha256sum)\"\n" +
		"[ -z \"${FAIL_LIST:-}\" ] || exit 3\n"
	if err := os.WriteFile(fakeGo, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	standIn := filepath.Join(dir, "go")
	if err := os.WriteFile(standIn, []byte(fmt.Sprintf(standInGo, fakeGo)), 0o755); err != nil {
		t.Fatal(err)
	}
	root, other, cache := filepath.Join(dir, "root"), filepath.Join(dir, "other"), filepath.Join(dir, "cache")
	for _, d := range []string{root, filepath.Join(root, "sub"), other, cache} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	list := func(cwd string, env []string, args ...string) (string, int) {
		t.Helper()
		cmd := exec.Command(standIn, append([]string{"list"}, args...)...)
		cmd.Dir = cwd
		cmd.Env = append(os.Environ(), append([]string{"FAKE_GO_LIST_CACHE=" + cache, "FAKE_GO_LIST_CACHE_ROOT=" + root, "PWD=" + cwd,
			"GOFLAGS=", "GOWORK=off"}, env...)...)
		out, err := cmd.Output()
		code := 0
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		return string(out), code
	}
	count := func() int {
		b, _ := os.ReadFile(calls)
		return len(b)
	}
	first, _ := list(root, nil, "-m")
	if again, _ := list(root, nil, "-m"); again != first || count() != 1 {
		t.Fatalf("a repeated call was not served from the cache: %q then %q, %d real call(s)", first, again, count())
	}
	seen := map[string]bool{first: true}
	for _, c := range []struct {
		name string
		cwd  string
		env  []string
		args []string
	}{
		{"other arguments", root, nil, []string{"./..."}},
		{"another directory of the tree", filepath.Join(root, "sub"), nil, []string{"-m"}},
		{"other GOFLAGS", root, []string{"GOFLAGS=-tags=integration"}, []string{"-m"}},
		{"other GOWORK", root, []string{"GOWORK=on"}, []string{"-m"}},
		{"other GO111MODULE", root, []string{"GO111MODULE=off"}, []string{"-m"}},
		{"another variable of the environment", root, []string{"GOTOOLCHAIN=local"}, []string{"-m"}},
		{"a variable go does not read (the key holds the whole environment)", root, []string{"ZZ_UNRELATED_TO_GO=1"}, []string{"-m"}},
		{"another PATH", root, []string{"PATH=" + other + string(os.PathListSeparator) + os.Getenv("PATH")}, []string{"-m"}},
	} {
		before := count()
		out, _ := list(c.cwd, c.env, c.args...)
		if count() != before+1 || seen[out] {
			t.Errorf("%s: served from another call's cache entry (%q)", c.name, out)
		}
		seen[out] = true
	}
	// the stand-in's own directory at the head of PATH (a per-test directory) is not part of the key: a second stand-in in another
	// directory, with its own directory at the head of PATH, is served the first one's entry
	second := filepath.Join(dir, "second")
	if err := os.MkdirAll(second, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(second, "go"), []byte(fmt.Sprintf(standInGo, fakeGo)), 0o755); err != nil {
		t.Fatal(err)
	}
	headed := func(bin string) (string, int) {
		t.Helper()
		cmd := exec.Command(filepath.Join(bin, "go"), "list", "-json")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "FAKE_GO_LIST_CACHE="+cache, "FAKE_GO_LIST_CACHE_ROOT="+root, "PWD="+root, "GOFLAGS=", "GOWORK=off",
			"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		out, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		return string(out), count()
	}
	firstOut, n := headed(dir)
	if secondOut, m := headed(second); secondOut != firstOut || m != n {
		t.Errorf("a stand-in in another directory missed the entry of the same call (%d real call(s) after it, want %d)", m, n)
	}
	before := count()
	list(other, nil, "-m")
	list(other, nil, "-m")
	if count() != before+2 {
		t.Errorf("a call outside the real tree was cached")
	}
	before = count()
	if _, code := list(root, []string{"FAIL_LIST=1"}, "-e", "./failing"); code != 3 {
		t.Fatalf("a failing go list exited %d, want its own code 3", code)
	}
	if _, code := list(root, nil, "-e", "./failing"); code != 0 || count() != before+2 {
		t.Errorf("a failing call was cached (exit %d, %d real call(s))", code, count()-before)
	}
}

// With the real go: the same go list in the same directory, under a GO111MODULE and then a GOFLAGS that make real go fail, is not
// served from the cache; it fails as real go does.
func TestTheStandInGoListCacheMissesWhenTheGoEnvironmentFlips(t *testing.T) {
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	standIn := filepath.Join(dir, "go")
	if err := os.WriteFile(standIn, []byte(fmt.Sprintf(standInGo, realGo)), 0o755); err != nil {
		t.Fatal(err)
	}
	root := repoRoot(t)
	run := func(bin string, env ...string) (string, int) {
		t.Helper()
		cmd := exec.Command(bin, "list", "-m")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), append([]string{"FAKE_GO_LIST_CACHE=" + dir, "FAKE_GO_LIST_CACHE_ROOT=" + root, "PWD=" + root,
			"GOWORK=off", "GOFLAGS=-mod=readonly", "GO111MODULE=on"}, env...)...)
		out, err := cmd.Output()
		code := 0
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		return string(out), code
	}
	if out, code := run(standIn); code != 0 || out == "" {
		t.Fatalf("go list -m through the stand-in: exit %d, %q", code, out)
	}
	for _, flip := range []string{"GO111MODULE=off", "GOFLAGS=-mod=bogus"} {
		wantOut, wantCode := run(realGo, flip)
		gotOut, gotCode := run(standIn, flip)
		if wantCode == 0 {
			t.Fatalf("%s: real go list -m succeeded; the flip must make it fail", flip)
		}
		if gotCode != wantCode || gotOut != wantOut {
			t.Errorf("%s: the stand-in gave exit %d %q, real go exit %d %q (a stale cache entry)", flip, gotCode, gotOut, wantCode, wantOut)
		}
	}
}
