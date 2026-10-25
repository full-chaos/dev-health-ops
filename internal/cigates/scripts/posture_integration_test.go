package scripts_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11439: ci/posture_integration.sh lists the role / grant / posture / migration integration packages a lane runs
// locally. The list file and the script's trigger globs must not drift from the packages that hold posture tests.

const postureModule = "github.com/full-chaos/dev-health-ops/"

func postureKeys(t *testing.T, root string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "ci", "posture_integration_packages.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, l := range strings.Split(string(b), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
			keys = append(keys, l)
		}
	}
	return keys
}

func posturePaths(t *testing.T, paths ...string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(f, []byte(strings.Join(paths, "\x00")+"\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func postureList(t *testing.T, file string) cmdResult {
	return cmdSpec{dir: t.TempDir(), env: []string{shimBasePath()}}.run("bash", filepath.Join(repoRoot(t), "ci", "posture_integration.sh"), "--paths", file)
}

func postureLines(out string) (base string, pkgs []string) {
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		switch {
		case strings.HasPrefix(l, "BASE "):
			base = strings.TrimPrefix(l, "BASE ")
		case strings.HasPrefix(l, "POSTURE "):
			pkgs = append(pkgs, strings.TrimPrefix(l, "POSTURE "))
		}
	}
	return base, pkgs
}

func TestPostureIntegrationListsTheTwelvePackagesOnATrigger(t *testing.T) {
	root := repoRoot(t)
	keys := postureKeys(t, root)
	if len(keys) != 12 {
		t.Fatalf("the posture list has %d packages, want 12", len(keys))
	}
	triggers := []string{
		"internal/pgmigrate/sql/0158_x.sql", "internal/pgmigrate/baseline/head.json", "internal/rivermigrate/x.go",
		"internal/testsupport/domainroledb/domainroledb.go", "internal/storage/postgres/domain_authorization.go",
		"internal/storage/postgres/posture_manifest.go", "internal/storage/postgres/role_posture_integration_test.go",
		"internal/storage/postgres/authschema/x.go", "internal/storage/postgres/aclguard/x.go",
		"internal/storage/roleprovision/x.go", "ci/bigboy/pg-grants-check.sh", "ci/posture_integration_packages.txt",
	}
	for _, k := range keys { // every listed package directory is itself a trigger
		triggers = append(triggers, k+"/anything.go")
	}
	for _, p := range triggers {
		res := postureList(t, posturePaths(t, "docs/a.md", p))
		if res.code != 0 {
			t.Fatalf("%s: exit %d stderr=%q", p, res.code, res.stderr)
		}
		base, got := postureLines(res.stdout)
		if base != "paths-file" {
			t.Fatalf("%s: BASE %q, want paths-file", p, base)
		}
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(keys, ",") {
			t.Fatalf("%s: listed %v, want all 12 %v", p, got, keys)
		}
	}
}

func TestPostureIntegrationListsNothingForUnrelatedPaths(t *testing.T) {
	for _, p := range []string{"internal/foo/bar.go", "docs/a.md", "internal/pgmigrate2/x.go", "internal/storage/postgresx/a.go", "internal/storage/postgres/store.go"} {
		res := postureList(t, posturePaths(t, p))
		if res.code != 0 || !strings.Contains(res.stdout, "NONE ") || strings.Contains(res.stdout, "POSTURE ") {
			t.Fatalf("%s: exit %d stdout=%q, want NONE and no package", p, res.code, res.stdout)
		}
	}
}

func TestPostureIntegrationFailsClosedOnBadInput(t *testing.T) {
	dir := t.TempDir()
	unterminated := filepath.Join(dir, "u.txt")
	if err := os.WriteFile(unterminated, []byte("internal/pgmigrate/sql/a.sql"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := posturePaths(t, "docs/a.md", "")
	for name, file := range map[string]string{"missing file": filepath.Join(dir, "none.txt"), "unterminated": unterminated, "empty record": empty} {
		res := postureList(t, file)
		if res.code == 0 || strings.Contains(res.stdout, "POSTURE ") || strings.Contains(res.stdout, "NONE ") {
			t.Fatalf("%s: exit %d stdout=%q, want non-zero and no verdict", name, res.code, res.stdout)
		}
	}
	// default mode: no origin/main in a bare repo fails closed
	res := cmdSpec{dir: t.TempDir(), env: []string{shimBasePath()}}.run("bash", filepath.Join(repoRoot(t), "ci", "posture_integration.sh"))
	if res.code == 0 || strings.Contains(res.stdout, "POSTURE ") || strings.Contains(res.stdout, "NONE ") {
		t.Fatalf("no origin/main: exit %d stdout=%q, want non-zero and no verdict", res.code, res.stdout)
	}
}

func TestPostureIntegrationDefaultModePrintsTheBaseSha(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) cmdResult {
		a := append([]string{"-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false"}, args...)
		res := cmdSpec{dir: repo, env: []string{shimBasePath()}}.run("git", a...)
		if res.code != 0 {
			t.Fatalf("git %v: %q", args, res.stderr)
		}
		return res
	}
	git("init", "-q", "-b", "main")
	git("commit", "-q", "--allow-empty", "-m", "base")
	base := strings.TrimSpace(git("rev-parse", "HEAD").stdout)
	git("update-ref", "refs/remotes/origin/main", base)
	if err := os.MkdirAll(filepath.Join(repo, "internal", "pgmigrate", "sql"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "internal", "pgmigrate", "sql", "0001_x.sql"), []byte("select 1;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-q", "-m", "change")
	res := cmdSpec{dir: repo, env: []string{shimBasePath()}}.run("bash", filepath.Join(repoRoot(t), "ci", "posture_integration.sh"))
	if res.code != 0 {
		t.Fatalf("exit %d stderr=%q", res.code, res.stderr)
	}
	gotBase, pkgs := postureLines(res.stdout)
	if gotBase != base || len(pkgs) != 12 {
		t.Fatalf("BASE %q (want %q), %d packages (want 12)", gotBase, base, len(pkgs))
	}
}

// postureDefects returns what is wrong with keys against the listed go packages: a key that is no package, and a
// package that holds posture tests but is not in keys.
func postureDefects(keys []string, goList string) []string {
	have := map[string]bool{}
	for _, k := range keys {
		have[k] = true
	}
	var defects []string
	seen := map[string]bool{}
	for _, l := range strings.Split(strings.TrimSpace(goList), "\n") {
		f := strings.Split(l, "|")
		if len(f) != 4 {
			continue
		}
		pkg := strings.TrimPrefix(f[0], postureModule)
		seen[pkg] = true
		imports := f[1]
		files := strings.Split(f[2]+","+f[3], ",")
		posture := strings.Contains(imports, "internal/testsupport/domainroledb")
		for _, n := range files {
			if strings.HasSuffix(n, "_integration_test.go") && (strings.Contains(n, "role_posture") || strings.Contains(n, "domain_role")) {
				posture = true
			}
		}
		if posture && !have[pkg] {
			defects = append(defects, "posture-test package missing from the list: "+pkg)
		}
	}
	for _, k := range keys {
		if !seen[k] {
			defects = append(defects, "key is not a go package: "+k)
		}
	}
	return defects
}

func TestPostureIntegrationListMatchesThePostureTestPackages(t *testing.T) {
	root := repoRoot(t)
	keys := postureKeys(t, root)
	res := cmdSpec{dir: root, env: []string{shimBasePath(), "GOFLAGS=-mod=readonly"}}.run("go", "list", "-tags=integration", "-e", "-f",
		"{{.ImportPath}}|{{join .TestImports \",\"}},{{join .XTestImports \",\"}}|{{join .TestGoFiles \",\"}}|{{join .XTestGoFiles \",\"}}", "./...")
	if res.code != 0 {
		t.Fatalf("go list: exit %d stderr=%q", res.code, res.stderr)
	}
	if d := postureDefects(keys, res.stdout); len(d) > 0 {
		t.Fatalf("drift: %v", d)
	}
	// negative controls: the guard must go red both ways
	var without []string
	for _, k := range keys {
		if k != "internal/llmorgsettings" {
			without = append(without, k)
		}
	}
	if d := postureDefects(without, res.stdout); len(d) == 0 || !strings.Contains(strings.Join(d, ";"), "missing from the list: "+postureModule+"internal/llmorgsettings") && !strings.Contains(strings.Join(d, ";"), "missing from the list: internal/llmorgsettings") {
		t.Fatalf("a removed posture package must be red; got %v", d)
	}
	if d := postureDefects(append([]string{"internal/nosuchpkg"}, keys...), res.stdout); len(d) == 0 {
		t.Fatal("a bogus key must be red")
	}
}
