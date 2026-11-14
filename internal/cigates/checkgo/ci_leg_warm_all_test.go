package checkgo_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-13718: `ci/check_go.sh ci-leg-warm all build|race` fills one go-quality Go cache family (go-quality.yml job
// go-quality-cache). The entry is only useful when its compile actions are the ones the legs look up, so the cell pins the
// exact go argv per family: the legs' flags (-mod=readonly -trimpath), -race only in race, -exec true, resolved from PATH (no test code
// runs), the static vet passes only in build, and the race package set = the module's packages but the race-excluded rows.
// A stand-in go records every test and vet call (cwd and argv) and answers them with success; every other call goes to the
// real go.

func warmAllFakeGo(t *testing.T) (pathEnv, logFile string) {
	t.Helper()
	real, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	dir, logDir := t.TempDir(), t.TempDir()
	logFile = filepath.Join(logDir, "calls")
	script := "#!/bin/bash\ncase \"$1\" in test|vet) printf '%s\\t%s\\n' \"$PWD\" \"$*\" >>" + logFile + "; exit 0 ;; esac\nexec " + real + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return "PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"), logFile
}

type warmCall struct{ dir, argv string }

func runWarmAll(t *testing.T, family string) (cmdResult, []warmCall) {
	t.Helper()
	root := rootOf(t)
	pathEnv, logFile := warmAllFakeGo(t)
	env := inheritedEnv(nil, "DEV_HEALTH_GO_CACHE="+t.TempDir(), "GIT_CONFIG_GLOBAL="+os.DevNull, pathEnv)
	res := cmdSpec{dir: root, env: env}.run("bash", filepath.Join(root, "ci", "check_go.sh"), "ci-leg-warm", "all", family)
	b, err := os.ReadFile(logFile)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var calls []warmCall
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if d, a, ok := strings.Cut(l, "\t"); ok {
			rel, err := filepath.Rel(root, d)
			if err != nil {
				t.Fatal(err)
			}
			calls = append(calls, warmCall{rel, a})
		}
	}
	return res, calls
}

// warmModuleDirs is the module list check_go.sh walks: the root module first, then every other tracked go.mod outside vendor/.
func warmModuleDirs(t *testing.T, root string) []string {
	t.Helper()
	res := cmdSpec{dir: root, env: gitEnv}.run("git", "ls-files", "--cached", "--", ":(glob)**/go.mod")
	if res.code != 0 {
		t.Fatalf("git ls-files: %s", res.stderr)
	}
	dirs := []string{"."}
	for _, f := range strings.Fields(res.stdout) {
		if f == "go.mod" || strings.HasPrefix(f, "vendor/") || strings.Contains(f, "/vendor/") {
			continue
		}
		dirs = append(dirs, strings.TrimSuffix(f, "/go.mod"))
	}
	return dirs
}

func TestCiLegWarmAllRunsTheLegFlagsPerFamily(t *testing.T) {
	root := rootOf(t)
	modules := warmModuleDirs(t, root)

	t.Run("build", func(t *testing.T) {
		res, calls := runWarmAll(t, "build")
		if res.code != 0 {
			t.Fatalf("ci-leg-warm all build: rc=%d stderr=%q", res.code, res.stderr)
		}
		var want []warmCall
		for _, m := range modules {
			want = append(want,
				warmCall{m, "test -mod=readonly -trimpath -exec true ./..."},
				warmCall{m, "vet -mod=readonly -trimpath ./..."},
				warmCall{m, "vet -mod=readonly -trimpath -tags=integration ./..."})
		}
		if !sameCalls(calls, want) {
			t.Fatalf("build go calls:\n%v\nwant:\n%v", calls, want)
		}
	})

	t.Run("race", func(t *testing.T) {
		res, calls := runWarmAll(t, "race")
		if res.code != 0 {
			t.Fatalf("ci-leg-warm all race: rc=%d stderr=%q", res.code, res.stderr)
		}
		const prefix = "test -mod=readonly -trimpath -race -exec true "
		byDir := map[string][]string{}
		for _, c := range calls {
			if !strings.HasPrefix(c.argv, prefix) {
				t.Fatalf("race go call %q in %s: want only %q<packages>", c.argv, c.dir, prefix)
			}
			if _, dup := byDir[c.dir]; dup {
				t.Fatalf("two race go calls in %s", c.dir)
			}
			byDir[c.dir] = strings.Fields(strings.TrimPrefix(c.argv, prefix))
		}
		got := byDir["."]
		want := raceWarmWant(t, root)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("root module race packages: %d listed, want %d (the module's packages but ci/race_excluded_packages.tsv)", len(got), len(want))
		}
		for _, m := range modules {
			if len(byDir[m]) == 0 {
				t.Errorf("module %s has no race go call", m)
			}
		}
		if len(byDir) != len(modules) {
			t.Errorf("race go calls in %d dirs, want the %d modules", len(byDir), len(modules))
		}
	})

	t.Run("an unknown family is refused", func(t *testing.T) {
		res, calls := runWarmAll(t, "static")
		if res.code == 0 || !strings.Contains(res.stderr, "ci-leg-warm all FAMILY must be build or race") || len(calls) != 0 {
			t.Fatalf("ci-leg-warm all static: rc=%d stderr=%q calls=%v", res.code, res.stderr, calls)
		}
	})
}

func sameCalls(got, want []warmCall) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// raceWarmWant is `go list ./...` of the root module without the rows of ci/race_excluded_packages.tsv, in go list order.
func raceWarmWant(t *testing.T, root string) []string {
	t.Helper()
	env := inheritedEnv([]string{"GOFLAGS"}, "GOWORK=off")
	mod := cmdSpec{dir: root, env: env}.run("go", "list", "-m")
	list := cmdSpec{dir: root, env: env}.run("go", "list", "-mod=readonly", "./...")
	if mod.code != 0 || list.code != 0 {
		t.Fatalf("go list: %s %s", mod.stderr, list.stderr)
	}
	modpath := strings.TrimSpace(mod.stdout)
	b, err := os.ReadFile(filepath.Join(root, "ci", "race_excluded_packages.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	skip := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		if tl := strings.TrimSpace(l); tl == "" || strings.HasPrefix(tl, "#") {
			continue
		}
		key, _, _ := strings.Cut(l, "\t")
		skip[modpath+"/"+key] = true
	}
	if len(skip) == 0 {
		t.Fatal("ci/race_excluded_packages.tsv has no row: the cell would not prove the exclusion")
	}
	var want []string
	excluded := 0
	for _, p := range strings.Fields(list.stdout) {
		if skip[p] {
			excluded++
			continue
		}
		want = append(want, p)
	}
	if excluded == 0 {
		t.Fatal("no package of the module is race-excluded: the cell would not prove the exclusion")
	}
	return want
}
