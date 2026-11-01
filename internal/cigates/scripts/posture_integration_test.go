package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11439: ci/posture_integration.sh lists the role / grant / posture / migration integration packages a lane runs
// locally. The list file and the script's trigger globs must not drift from the packages that hold posture tests.

const postureModule = "github.com/full-chaos/dev-health-ops/"

func postureKeys(t *testing.T, root string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "ci", "posture_integration_packages.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 || b[len(b)-1] != '\n' {
		t.Fatal("the posture list must end with a newline: a last line without one is dropped by a line reader")
	}
	if strings.ContainsAny(string(b), "\r") {
		t.Fatal("the posture list must not contain a carriage return")
	}
	var keys []string
	for _, l := range strings.Split(string(b), "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		if strings.ContainsAny(l, " \t") {
			t.Fatalf("the posture list line %q must not contain spaces or tabs", l)
		}
		keys = append(keys, l)
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

func TestPostureIntegrationListsTheFourteenPackagesOnATrigger(t *testing.T) {
	root := repoRoot(t)
	keys := postureKeys(t, root)
	if len(keys) != 14 {
		t.Fatalf("the posture list has %d packages, want 14", len(keys))
	}
	triggers := []string{
		"internal/pgmigrate/sql/0158_x.sql", "internal/pgmigrate/baseline/head.json", "internal/pgmigrate/archive/sql/0139_x.sql", "internal/rivermigrate/x.go",
		"internal/testsupport/domainroledb/domainroledb.go", "internal/storage/postgres/domain_authorization.go",
		"internal/storage/postgres/posture_manifest.go", "internal/storage/postgres/role_posture_integration_test.go",
		"internal/storage/postgres/authschema/x.go", "internal/storage/postgres/aclguard/x.go",
		"internal/storage/roleprovision/x.go", "ci/bigboy/pg-grants-check.sh", "ci/posture_integration_packages.tsv", "ci/posture_integration.sh",
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
			t.Fatalf("%s: listed %v, want all 14 %v", p, got, keys)
		}
	}
}

func TestPostureIntegrationListsNothingForUnrelatedPaths(t *testing.T) {
	for _, p := range []string{"internal/foo/bar.go", "docs/a.md", "internal/pgmigrate2/x.go", "internal/storage/postgresx/a.go", "internal/storage/clickhouse/store.go"} {
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
		res := cmdSpec{dir: repo, env: []string{shimBasePath()}}.run("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}, a...)...)
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
	if gotBase != base || len(pkgs) != 14 {
		t.Fatalf("BASE %q (want %q), %d packages (want 14)", gotBase, base, len(pkgs))
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
	res := cmdSpec{dir: root, env: goListEnv()}.run("go", "list", "-tags=integration", "-e", "-f",
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

func goListEnv() []string {
	env := []string{shimBasePath(), "GOFLAGS=-mod=readonly"}
	for _, name := range []string{"HOME", "GOMODCACHE", "GOCACHE", "GOPATH", "GOTMPDIR", "GOROOT"} {
		if v, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+v)
		}
	}
	return env
}

func TestPostureIntegrationRunRefusesADirtyTree(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) string {
		a := append([]string{"-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false"}, args...)
		res := cmdSpec{dir: repo, env: []string{shimBasePath()}}.run("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}, a...)...)
		if res.code != 0 {
			t.Fatalf("git %v: %q", args, res.stderr)
		}
		return strings.TrimSpace(res.stdout)
	}
	git("init", "-q", "-b", "main")
	git("commit", "-q", "--allow-empty", "-m", "base")
	git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD"))
	if err := os.MkdirAll(filepath.Join(repo, "internal", "pgmigrate", "sql"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "internal", "pgmigrate", "sql", "0001_x.sql"), []byte("select 1;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-q", "-m", "change")
	if err := os.WriteFile(filepath.Join(repo, "untracked.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(t.TempDir(), "capped.sh")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(repoRoot(t), "ci", "posture_integration.sh")
	res := cmdSpec{dir: repo, env: []string{shimBasePath()}}.run("bash", script, "--run", "--test-runner", stub)
	if res.code == 0 || strings.Contains(res.stdout, "RECEIPT ") || !strings.Contains(res.stderr, "clean=no") ||
		strings.Contains(res.stderr, "RECEIPT-HEAD") || !strings.Contains(res.stderr, "SIMULATED-HEAD") {
		t.Fatalf("dirty tree: exit %d stdout=%q stderr=%q, want non-zero, clean=no, no receipt", res.code, res.stdout, res.stderr)
	}
	if err := os.Remove(filepath.Join(repo, "untracked.txt")); err != nil {
		t.Fatal(err)
	}
	sha := git("rev-parse", "HEAD")
	res = cmdSpec{dir: repo, env: []string{"PATH=" + filepath.Dir(goBinary(t)) + string(os.PathListSeparator) + "/usr/bin:/bin"}}.run("bash", script, "--run", "--test-runner", stub)
	// The script that runs must be the one committed at HEAD: this repo does not carry it, so the run is refused.
	if res.code != 3 || !strings.Contains(res.stderr, "not the one committed at HEAD") || strings.Contains(res.stdout, "RECEIPT") || strings.Contains(res.stdout, "SIMULATED-HEAD") {
		t.Fatalf("a script that is not committed at HEAD: exit %d stdout=%q stderr=%q, want 3 and a refusal", res.code, res.stdout, res.stderr)
	}
	_ = sha
}

// The runner is not replaceable by the environment: POSTURE_CAPPED_SH=/bin/true must not yield a PASS receipt.
func TestPostureIntegrationRunIgnoresARunnerOverrideInTheEnvironment(t *testing.T) {
	repo, git := postureRepo(t)
	git("init", "-q", "-b", "main")
	git("commit", "-q", "--allow-empty", "-m", "base")
	git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD"))
	if err := os.MkdirAll(filepath.Join(repo, "internal", "pgmigrate", "sql"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "internal", "pgmigrate", "sql", "0001_x.sql"), []byte("select 1;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-q", "-m", "change")
	res := cmdSpec{dir: repo, env: []string{shimBasePath(), "POSTURE_CAPPED_SH=/bin/true"}}.run("bash", filepath.Join(repoRoot(t), "ci", "posture_integration.sh"), "--run")
	missing := res.code == 3 && (strings.Contains(res.stderr, "runner not found") || strings.Contains(res.stderr, "go not found") || strings.Contains(res.stderr, "not the one committed at HEAD"))
	named := strings.Contains(res.stdout, "runner=/home/ubuntu/devhealth/scripts/capped.sh sha256=")
	if res.code == 0 || strings.Contains(res.stdout, " PASS ") || strings.Contains(res.stdout, "pass=14") || !(missing || named) {
		t.Fatalf("env runner override: exit %d stdout=%q stderr=%q, want non-zero, no PASS, and either the missing-runner exit 3 or the real capped.sh named with its sha256", res.code, res.stdout, res.stderr)
	}
}

func postureRepo(t *testing.T) (repo string, git func(args ...string) string) {
	t.Helper()
	repo = t.TempDir()
	git = func(args ...string) string {
		a := append([]string{"-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false"}, args...)
		res := cmdSpec{dir: repo, env: []string{shimBasePath()}}.run("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}, a...)...)
		if res.code != 0 {
			t.Fatalf("git %v: %q", args, res.stderr)
		}
		return strings.TrimSpace(res.stdout)
	}
	return repo, git
}

// A file moved out of a posture path leaves the old path out of a rename-detecting diff; the helper must still list.
func TestPostureIntegrationListsAFileMovedOutOfAPosturePath(t *testing.T) {
	repo, git := postureRepo(t)
	git("init", "-q", "-b", "main")
	for _, d := range []string{"internal/storage/postgres", "internal/storage/clickhouse"} {
		if err := os.MkdirAll(filepath.Join(repo, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	src := filepath.Join(repo, "internal", "storage", "postgres", "posture_probe.go")
	if err := os.WriteFile(src, []byte("package postgres\n\nvar probe = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-q", "-m", "base")
	git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD"))
	git("mv", "internal/storage/postgres/posture_probe.go", "internal/storage/clickhouse/probe.go")
	git("commit", "-q", "-m", "move out")
	res := cmdSpec{dir: repo, env: []string{shimBasePath()}}.run("bash", filepath.Join(repoRoot(t), "ci", "posture_integration.sh"))
	if _, pkgs := postureLines(res.stdout); res.code != 0 || len(pkgs) != 14 {
		t.Fatalf("a file moved out of a posture path: exit %d stdout=%q, want the 14 packages", res.code, res.stdout)
	}
}

// A git status that fails is not a clean tree: --run must exit non-zero and run nothing.
func TestPostureIntegrationRunFailsClosedWhenGitStatusFails(t *testing.T) {
	repo, git := postureRepo(t)
	git("init", "-q", "-b", "main")
	git("commit", "-q", "--allow-empty", "-m", "base")
	git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD"))
	if err := os.MkdirAll(filepath.Join(repo, "internal", "pgmigrate", "sql"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "internal", "pgmigrate", "sql", "0001_x.sql"), []byte("select 1;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-q", "-m", "change")
	if err := os.WriteFile(filepath.Join(repo, ".git", "index"), []byte("not an index"), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(t.TempDir(), "calls")
	stub := filepath.Join(t.TempDir(), "capped.sh")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\necho x >> "+calls+"\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	res := cmdSpec{dir: repo, env: []string{"PATH=" + filepath.Dir(goBinary(t)) + string(os.PathListSeparator) + "/usr/bin:/bin"}}.run("bash", filepath.Join(repoRoot(t), "ci", "posture_integration.sh"), "--run", "--test-runner", stub)
	if res.code == 0 || strings.Contains(res.stdout, "clean=yes") || strings.Contains(res.stdout, "RECEIPT ") {
		t.Fatalf("failed git status: exit %d stdout=%q, want non-zero, no clean=yes, no receipt", res.code, res.stdout)
	}
	if _, err := os.Stat(calls); err == nil {
		t.Fatal("a failed git status must run no package")
	}
}

// postureFixture is a throwaway repository (base/repo) with a copy of the helper, a two-package list, a package whose test
// writes a marker file (outside the repo, its path baked into the committed test) when it really runs, a package with no
// test files, and a nested dependency module. Everything is committed.
type postureFixture struct {
	base, dir, marker string
	git               func(args ...string) string
}

func postureDemo(t *testing.T) postureFixture {
	t.Helper()
	base := t.TempDir()
	dir := filepath.Join(base, "repo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		a := append([]string{"-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false"}, args...)
		res := cmdSpec{dir: dir, env: []string{shimBasePath()}}.run("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}, a...)...)
		if res.code != 0 {
			t.Fatalf("git %v: %q", args, res.stderr)
		}
		return strings.TrimSpace(res.stdout)
	}
	marker := filepath.Join(t.TempDir(), "ran.marker")
	write := func(rel, body string, mode os.FileMode) {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "posture_integration.sh"))
	if err != nil {
		t.Fatal(err)
	}
	write("ci/posture_integration.sh", string(script), 0o755)
	write("ci/posture_integration_packages.tsv", "internal/empty\ninternal/ok\n", 0o644)
	write("go.mod", "module demo\n\ngo 1.21\n\nrequire example.com/dep v0.0.0\n\nreplace example.com/dep => ./dep\n", 0o644)
	write("dep/go.mod", "module example.com/dep\n\ngo 1.21\n", 0o644)
	write("dep/dep.go", "package dep\n\nfunc Val() int { return 1 }\n", 0o644)
	write("internal/ok/ok_test.go", okTest(marker), 0o644)
	write("internal/empty/empty.go", "package empty\n", 0o644)
	git("init", "-q", "-b", "main")
	git("add", ".")
	git("commit", "-q", "-m", "demo")
	return postureFixture{base: base, dir: dir, marker: marker, git: git}
}

func okTest(marker string) string {
	return "package ok\n\nimport (\n\t\"os\"\n\t\"testing\"\n\n\t\"example.com/dep\"\n)\n\nfunc TestRan(t *testing.T) {\n\tif dep.Val() != 1 {\n\t\tt.Fatal(\"dep\")\n\t}\n\tif err := os.WriteFile(" + "\"" + marker + "\"" + ", []byte(\"x\"), 0o600); err != nil {\n\t\tt.Fatal(err)\n\t}\n}\n"
}

func goBinary(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// postureDemoRun runs the helper copy committed in the fixture with a pass-through test runner, from cwd.
func postureDemoRun(t *testing.T, f postureFixture, cwd string, extraEnv ...string) cmdResult {
	t.Helper()
	return postureDemoRunArgs(t, f, cwd, nil, extraEnv...)
}

func postureDemoRunArgs(t *testing.T, f postureFixture, cwd string, args []string, extraEnv ...string) cmdResult {
	t.Helper()
	pass := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(pass, []byte("#!/bin/sh\nshift 5\nexec \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	paths := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(paths, []byte("internal/ok/ok_test.go\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := append(isolatedGoEnv(t), extraEnv...)
	hasPath := false
	for _, e := range extraEnv {
		hasPath = hasPath || strings.HasPrefix(e, "PATH=")
	}
	if !hasPath {
		env = append(env, "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	}
	return cmdSpec{dir: cwd, env: env}.run("bash", append([]string{filepath.Join(f.dir, "ci", "posture_integration.sh"), "--run", "--paths", paths, "--test-runner", pass}, args...)...)
}

// makeWritable lets t.TempDir remove a read-only Go module cache: the cleanup runs before the TempDir removal.
func makeWritable(t *testing.T, dir string) {
	t.Helper()
	t.Cleanup(func() { _ = exec.Command("chmod", "-R", "u+w", dir).Run() })
}

func withoutEnv(env []string, names ...string) []string {
	var out []string
	for _, e := range env {
		drop := false
		for _, n := range names {
			drop = drop || strings.HasPrefix(e, n+"=")
		}
		if !drop {
			out = append(out, e)
		}
	}
	return out
}

// isolatedGoEnv gives a test-runner run its own module cache, build cache and tmp dir, so a run never reads or fills the
// host caches and never leaves a dir of its own behind.
func isolatedGoEnv(t *testing.T) []string {
	t.Helper()
	env := withoutEnv(goListEnv(), "GOMODCACHE", "GOCACHE", "GOTMPDIR")
	for _, name := range []string{"GOMODCACHE", "GOCACHE", "GOTMPDIR"} {
		d := t.TempDir()
		makeWritable(t, d)
		env = append(env, name+"="+d)
	}
	return env
}

func postureRow(res cmdResult, prefix string) string {
	for _, l := range strings.Split(res.stdout, "\n") {
		if strings.HasPrefix(l, prefix) {
			return l
		}
	}
	return ""
}

// requireHonestPass: the committed test really ran (its marker exists outside the repo) and the row says PASS.
func requireHonestPass(t *testing.T, f postureFixture, res cmdResult, what string) {
	t.Helper()
	row := postureRow(res, "SIMULATED internal/ok ")
	_, err := os.Stat(f.marker)
	if !strings.Contains(row, " PASS ") || !strings.Contains(row, "tests=1") || err != nil {
		t.Fatalf("%s: want the committed test to run natively and PASS; row=%q marker=%v stdout=%q stderr=%q", what, row, err, res.stdout, res.stderr)
	}
}

// A package is PASS only when go test ran at least one test and none failed: a package with no test files, or a
// forged environment that makes go test exit 0 without running tests, must never print a PASS row.
func TestPostureIntegrationPassNeedsExecutedTests(t *testing.T) {
	cases := []struct {
		name string
		env  []string
	}{
		{"plain", nil},
		{"GOFLAGS exec true", []string{"GOFLAGS=-exec=/bin/true"}},
		{"GOFLAGS run nothing", []string{"GOFLAGS=-run=^$"}},
		{"GOENV file carries GOFLAGS", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := postureDemo(t)
			env := c.env
			if c.name == "GOENV file carries GOFLAGS" {
				p := filepath.Join(t.TempDir(), "goenv")
				if err := os.WriteFile(p, []byte("GOFLAGS=-exec=/bin/true\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				env = []string{"GOENV=" + p}
			}
			res := postureDemoRun(t, f, f.dir, env...)
			if res.code == 0 {
				t.Fatalf("a simulated run exits non-zero, got 0: %q", res.stdout)
			}
			if row := postureRow(res, "SIMULATED internal/empty "); !strings.Contains(row, " FAIL ") {
				t.Fatalf("a package with no tests must be FAIL, got %q (stdout=%q stderr=%q)", row, res.stdout, res.stderr)
			}
			requireHonestPass(t, f, res, c.name)
		})
	}
}

// In test-runner mode every stdout line starts SIMULATED (the list lines included) and the usage names the flag.
func TestPostureIntegrationSimulatedModeMarksEveryLine(t *testing.T) {
	f := postureDemo(t)
	res := postureDemoRun(t, f, f.dir)
	for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
		if !strings.HasPrefix(l, "SIMULATED") || strings.Contains(l, "RECEIPT") {
			t.Fatalf("line %q must start SIMULATED and not say RECEIPT; stdout=%q", l, res.stdout)
		}
	}
	if sum := postureRow(res, "SIMULATED-SUMMARY "); !strings.Contains(sum, " packages=2 partial=no") {
		t.Fatalf("the summary must carry packages= and partial=no, got %q", sum)
	}
	part := postureDemoRunArgs(t, f, f.dir, []string{"--only", "internal/ok"})
	if sum := postureRow(part, "SIMULATED-SUMMARY "); !strings.Contains(sum, " packages=1 partial=yes") {
		t.Fatalf("a --only run must say packages=1 partial=yes, got %q (stdout=%q stderr=%q)", sum, part.stdout, part.stderr)
	}
	bad := cmdSpec{dir: f.dir, env: []string{shimBasePath()}}.run("bash", filepath.Join(f.dir, "ci", "posture_integration.sh"), "--nope")
	if bad.code != 2 || !strings.Contains(bad.stderr, "--test-runner") {
		t.Fatalf("usage: exit %d stderr=%q, want 2 naming --test-runner", bad.code, bad.stderr)
	}
}

// The real runner path: RECEIPT-HEAD names the runner, the go toolchain, the worktree sha and the hashes, or the run fails
// closed with exit 3 and a missing-runner message (hosted runners have no capped.sh). Never a PASS without the head fields.
func TestPostureIntegrationRealRunHeadNamesTheToolchain(t *testing.T) {
	f := postureDemo(t)
	paths := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(paths, []byte("internal/ok/ok_test.go\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := append(isolatedGoEnv(t), "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	res := cmdSpec{dir: f.dir, env: env}.run("bash", filepath.Join(f.dir, "ci", "posture_integration.sh"), "--run", "--paths", paths)
	if res.code == 3 && strings.Contains(res.stderr, "runner not found") {
		return
	}
	head := postureRow(res, "RECEIPT-HEAD ")
	sha := f.git("rev-parse", "HEAD")
	for _, want := range []string{" clean=yes ", " runner=/home/ubuntu/devhealth/scripts/capped.sh sha256=", " go=", " go_sha256=", " go_version=", " go_env=GOOS=", " GOARCH=", " GOROOT=", " GOTOOLDIR=", " script_sha256=", " packages=2 packages_sha256=", " worktree_sha=" + sha} {
		if !strings.Contains(head, want) {
			t.Fatalf("RECEIPT-HEAD %q lacks %q (exit %d stdout=%q stderr=%q)", head, want, res.code, res.stdout, res.stderr)
		}
	}
	out, err := exec.Command("git", "-c", "maintenance.auto=false", "-c", "gc.auto=0", "-C", f.dir, "worktree", "list", "--porcelain").Output()
	if err != nil || strings.Count(string(out), "worktree ") != 1 {
		t.Fatalf("a run must leave no worktree behind: %q err=%v", out, err)
	}
}

// GOARCH=<other arch> makes go test run binaries through go_<os>_<arch>_exec on PATH; a fake helper that prints a passing
// test must not produce a PASS: the toolchain runs under env -i, so neither GOARCH nor the helper reaches it.
func TestPostureIntegrationForeignArchExecHelperCannotForgeAPass(t *testing.T) {
	f := postureDemo(t)
	fake := t.TempDir()
	helper := "#!/bin/sh\nprintf '=== RUN   TestRan\\n--- PASS: TestRan (0.00s)\\nPASS\\n'\nexit 0\n"
	for _, n := range []string{"go_linux_amd64_exec", "go_linux_arm64_exec"} {
		if err := os.WriteFile(filepath.Join(fake, n), []byte(helper), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	other := "amd64"
	if runtime.GOARCH == "amd64" {
		other = "arm64"
	}
	path := "PATH=" + fake + string(os.PathListSeparator) + filepath.Dir(goBinary(t)) + string(os.PathListSeparator) + "/usr/bin:/bin"
	requireHonestPass(t, f, postureDemoRun(t, f, f.dir, "GOARCH="+other, path), "foreign arch helper")
}

// A caller GOROOT must not reach the toolchain: with GOROOT pointing at an empty directory the run still resolves the
// real toolchain and runs the tests.
func TestPostureIntegrationCallerGorootDoesNotReachTheToolchain(t *testing.T) {
	f := postureDemo(t)
	requireHonestPass(t, f, postureDemoRun(t, f, f.dir, "GOROOT="+t.TempDir()), "caller GOROOT")
}

// Forge 1: a fake env on the caller PATH that prints a passing go test -json stream and exits 0.
func TestPostureIntegrationFakeEnvOnPathCannotForgeAPass(t *testing.T) {
	f := postureDemo(t)
	fake := t.TempDir()
	body := "#!/bin/sh\nprintf '{\"Action\":\"pass\",\"Test\":\"TestRan\"}\\n{\"Action\":\"pass\"}\\n'\nexit 0\n"
	if err := os.WriteFile(filepath.Join(fake, "env"), []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	path := "PATH=" + fake + string(os.PathListSeparator) + filepath.Dir(goBinary(t)) + string(os.PathListSeparator) + "/usr/bin:/bin"
	requireHonestPass(t, f, postureDemoRun(t, f, f.dir, path), "fake env on PATH")
}

// Forge 2: an edited test hidden from git status with skip-worktree. The run tests the committed HEAD, not the edit.
func TestPostureIntegrationSkipWorktreeEditCannotForgeAPass(t *testing.T) {
	f := postureDemo(t)
	if err := os.WriteFile(filepath.Join(f.dir, "internal", "ok", "ok_test.go"), []byte("package ok\n\nimport \"testing\"\n\nfunc TestRan(t *testing.T) {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.git("update-index", "--skip-worktree", "internal/ok/ok_test.go")
	requireHonestPass(t, f, postureDemoRun(t, f, f.dir), "skip-worktree edit")
}

// Forge 3: a go.work in a parent directory whose replace swaps the dependency for code that writes a forged marker.
func TestPostureIntegrationParentGoWorkCannotForgeAPass(t *testing.T) {
	f := postureDemo(t)
	forged := filepath.Join(t.TempDir(), "forged.marker")
	fakedep := filepath.Join(f.base, "fakedep")
	if err := os.MkdirAll(fakedep, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakedep, "go.mod"), []byte("module example.com/dep\n\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := "package dep\n\nimport \"os\"\n\nfunc init() { _ = os.WriteFile(\"" + forged + "\", []byte(\"x\"), 0o600) }\n\nfunc Val() int { return 1 }\n"
	if err := os.WriteFile(filepath.Join(fakedep, "dep.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.base, "go.work"), []byte("go 1.21\n\nuse ./repo\n\nreplace example.com/dep => ./fakedep\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	res := postureDemoRun(t, f, f.dir)
	if _, err := os.Stat(forged); err == nil {
		t.Fatalf("the parent go.work replace reached the test run: %q", res.stdout)
	}
	requireHonestPass(t, f, res, "parent go.work")
}

// Forge 4: the helper is run from a git-excluded fake module directory inside the repo; ./internal/ok must not resolve
// there. The tests run in a fresh worktree of HEAD, whatever the cwd.
func TestPostureIntegrationExcludedCwdModuleCannotForgeAPass(t *testing.T) {
	f := postureDemo(t)
	fakeDir := filepath.Join(f.dir, "fake")
	if err := os.MkdirAll(filepath.Join(fakeDir, "internal", "ok"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeDir, "go.mod"), []byte("module fake\n\ngo 1.21\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeDir, "internal", "ok", "ok_test.go"), []byte("package ok\n\nimport \"testing\"\n\nfunc TestRan(t *testing.T) {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, ".git", "info", "exclude"), []byte("fake/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireHonestPass(t, f, postureDemoRun(t, f, fakeDir), "excluded cwd module")
}

// A run that stops early (a failing runner, a dirty tree) leaves no worktree behind.
func TestPostureIntegrationLeavesNoWorktreeBehind(t *testing.T) {
	f := postureDemo(t)
	postureDemoRun(t, f, f.dir)
	out, err := exec.Command("git", "-c", "maintenance.auto=false", "-c", "gc.auto=0", "-C", f.dir, "worktree", "list", "--porcelain").Output()
	if err != nil || strings.Count(string(out), "worktree ") != 1 {
		t.Fatalf("worktrees after a run: %q err=%v", out, err)
	}
}

// A non-cone sparse-checkout in the caller repo is copied into a new worktree; the helper must disable it there so the
// tests of HEAD all run, not the subset the caller's patterns leave on disk.
func TestPostureIntegrationSparseCheckoutCannotHideATest(t *testing.T) {
	f := postureDemo(t)
	second := filepath.Join(t.TempDir(), "real.marker")
	body := "package ok\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\nfunc TestReal(t *testing.T) {\n\tif err := os.WriteFile(\"" + second + "\", []byte(\"x\"), 0o600); err != nil {\n\t\tt.Fatal(err)\n\t}\n}\n"
	if err := os.WriteFile(filepath.Join(f.dir, "internal", "ok", "real_test.go"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	f.git("add", ".")
	f.git("commit", "-q", "-m", "second test")
	f.git("sparse-checkout", "set", "--no-cone", "/*", "!/internal/ok/real_test.go")
	if st := f.git("status", "--porcelain"); st != "" {
		t.Fatalf("the plant must leave git status clean, got %q", st)
	}
	res := postureDemoRun(t, f, f.dir)
	row := postureRow(res, "SIMULATED internal/ok ")
	_, e1 := os.Stat(f.marker)
	_, e2 := os.Stat(second)
	if !strings.Contains(row, " PASS ") || !strings.Contains(row, "tests=2") || e1 != nil || e2 != nil {
		t.Fatalf("both committed tests must run: row=%q markers=%v/%v stdout=%q stderr=%q", row, e1, e2, res.stdout, res.stderr)
	}
}

// The list file is read strictly: a last line without a newline, a carriage return or a space makes the helper refuse
// (exit 3, no package line) instead of dropping or mangling a key.
func TestPostureIntegrationRefusesAMalformedList(t *testing.T) {
	for name, content := range map[string]string{
		"no final newline": "internal/empty\ninternal/ok",
		"carriage return":  "internal/empty\r\ninternal/ok\n",
		"trailing space":   "internal/empty\ninternal/ok \n",
	} {
		t.Run(name, func(t *testing.T) {
			f := postureDemo(t)
			if err := os.WriteFile(filepath.Join(f.dir, "ci", "posture_integration_packages.tsv"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			paths := posturePaths(t, "internal/ok/ok_test.go")
			res := cmdSpec{dir: f.dir, env: []string{shimBasePath()}}.run("bash", filepath.Join(f.dir, "ci", "posture_integration.sh"), "--paths", paths)
			if res.code != 3 || strings.Contains(res.stdout, "POSTURE ") || strings.Contains(res.stdout, "NONE ") {
				t.Fatalf("exit %d stdout=%q stderr=%q, want 3 and no verdict", res.code, res.stdout, res.stderr)
			}
		})
	}
}

// A run must not rewrite the caller's .git/config (a sparse-checkout disable in the worktree would add
// extensions.worktreeConfig to the shared config): the bytes are the same before and after, on a normal and a sparse clone.
func TestPostureIntegrationLeavesTheCallersGitConfigUntouched(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		name := "normal clone"
		if sparse {
			name = "sparse clone"
		}
		t.Run(name, func(t *testing.T) {
			f := postureDemo(t)
			if sparse {
				f.git("sparse-checkout", "set", "--no-cone", "/*", "!/internal/empty/empty.go")
			}
			cfg := filepath.Join(f.dir, ".git", "config")
			before, err := os.ReadFile(cfg)
			if err != nil {
				t.Fatal(err)
			}
			res := postureDemoRun(t, f, f.dir)
			after, err := os.ReadFile(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatalf("the caller's .git/config changed:\n--- before\n%s\n--- after\n%s\nstdout=%q stderr=%q", before, after, res.stdout, res.stderr)
			}
			requireHonestPass(t, f, res, name)
		})
	}
}

// Hosted runners install go through setup-go, not at /usr/local/go: the helper resolves go from the caller PATH (the
// receipt names the binary, its hash and its version) and must not hard-code a toolchain location. In real mode the
// missing-runner refusal comes before any go lookup, so a host without scripts/capped.sh fails closed with exit 3.
func TestPostureIntegrationHardCodesNoToolchainPath(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "posture_integration.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"/usr/local/go", "/usr/local/bin"} {
		if strings.Contains(string(b), bad) {
			t.Fatalf("ci/posture_integration.sh must not hard-code %q", bad)
		}
	}
	src := string(b)
	runnerCheck := strings.Index(src, `runner not found`)
	goResolve := strings.Index(src, `go_root="$(`)
	if runnerCheck < 0 || goResolve < 0 || runnerCheck > goResolve {
		t.Fatalf("the runner-not-found refusal must come before the go toolchain resolution (runner at %d, go at %d)", runnerCheck, goResolve)
	}
}

// CHAOS-11714: internal/pgmigrate needs about 705 s at host load 30-39, above go test's 10 m default, so the runner
// gives it -timeout 20m (and a capped.sh limit above it). internal/storage/postgres/authschema needs about 650 s
// under load and gets the same. Every other package keeps the default.
func TestPostureIntegrationGivesTheSlowPackagesALongTimeout(t *testing.T) {
	f := postureDemo(t)
	write := func(rel, body string) {
		p := filepath.Join(f.dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ci/posture_integration_packages.tsv", "internal/ok\ninternal/pgmigrate\ninternal/storage/postgres/authschema\n")
	write("internal/pgmigrate/pg_test.go", "package pgmigrate\n\nimport \"testing\"\n\nfunc TestRan(t *testing.T) {}\n")
	write("internal/storage/postgres/authschema/a_test.go", "package authschema\n\nimport \"testing\"\n\nfunc TestRan(t *testing.T) {}\n")
	f.git("add", ".")
	f.git("commit", "-q", "-m", "pgmigrate fixture")
	calls := filepath.Join(t.TempDir(), "calls.txt")
	runner := filepath.Join(t.TempDir(), "runner.sh")
	body := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + calls + "\nshift 5\nexec \"$@\"\n"
	if err := os.WriteFile(runner, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	paths := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(paths, []byte("internal/ok/ok_test.go\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := append(isolatedGoEnv(t), "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	res := cmdSpec{dir: f.dir, env: env}.run("bash", filepath.Join(f.dir, "ci", "posture_integration.sh"), "--run", "--paths", paths, "--test-runner", runner)
	if res.code == 0 || postureRow(res, "SIMULATED internal/ok ") == "" || postureRow(res, "SIMULATED internal/pgmigrate ") == "" ||
		postureRow(res, "SIMULATED internal/storage/postgres/authschema ") == "" {
		t.Fatalf("want all three packages run (exit is non-zero in test-runner mode): exit %d stdout=%q stderr=%q", res.code, res.stdout, res.stderr)
	}
	raw, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		switch {
		case strings.HasSuffix(line, " ./internal/pgmigrate"):
			seen++
			if !strings.Contains(line, " -timeout 20m ") || !strings.HasPrefix(line, "-t 1500 ") {
				t.Fatalf("internal/pgmigrate must run with -timeout 20m under a 1500 s cap, got %q", line)
			}
		case strings.HasSuffix(line, " ./internal/storage/postgres/authschema"):
			seen++
			if !strings.Contains(line, " -timeout 20m ") || !strings.HasPrefix(line, "-t 1500 ") {
				t.Fatalf("internal/storage/postgres/authschema must run with -timeout 20m under a 1500 s cap, got %q", line)
			}
		case strings.HasSuffix(line, " ./internal/ok"):
			seen++
			if strings.Contains(line, "-timeout") || !strings.HasPrefix(line, "-t 900 ") {
				t.Fatalf("every other package keeps go test's default timeout and the 900 s cap, got %q", line)
			}
		default:
			t.Fatalf("unexpected runner call %q", line)
		}
	}
	if seen != 3 {
		t.Fatalf("want one runner call per package, got %d: %q", seen, raw)
	}
}

// CHAOS-11829: a --test-runner run makes a GOMODCACHE, a GOCACHE and a GOTMPDIR with mktemp -d when the caller sets none.
// They must be gone when the script exits, on a pass, on a failing exit and when the module cache is read-only.
func postureStubRun(t *testing.T, f postureFixture, runnerBody string, env []string) cmdResult {
	t.Helper()
	runner := filepath.Join(t.TempDir(), "runner.sh")
	if err := os.WriteFile(runner, []byte(runnerBody), 0o700); err != nil {
		t.Fatal(err)
	}
	paths := filepath.Join(t.TempDir(), "changed.txt")
	if err := os.WriteFile(paths, []byte("internal/ok/ok_test.go\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	return cmdSpec{dir: f.dir, env: env}.run("bash", filepath.Join(f.dir, "ci", "posture_integration.sh"), "--run", "--paths", paths, "--test-runner", runner)
}

func postureTmpEnv(t *testing.T) (tmp string, env []string) {
	t.Helper()
	tmp = t.TempDir()
	makeWritable(t, tmp)
	env = append(withoutEnv(goListEnv(), "GOMODCACHE", "GOCACHE", "GOTMPDIR", "TMPDIR"), "TMPDIR="+tmp)
	return tmp, env
}

func requireEmptyDir(t *testing.T, dir, what string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("%s: TMPDIR holds %v after the script exited, want it empty", what, names)
	}
}

func TestPostureIntegrationStubRunLeavesNoTempDirsBehind(t *testing.T) {
	f := postureDemo(t)
	tmp, env := postureTmpEnv(t)
	env = append(env, "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	res := postureStubRun(t, f, "#!/bin/sh\nshift 5\nexec \"$@\"\n", env)
	if res.code != 5 || postureRow(res, "SIMULATED internal/ok ") == "" {
		t.Fatalf("stub run: exit %d stdout=%q stderr=%q, want exit 5 and the internal/ok row", res.code, res.stdout, res.stderr)
	}
	requireEmptyDir(t, tmp, "a stub run")
}

func TestPostureIntegrationStubRunLeavesNoTempDirsBehindOnAFailingExit(t *testing.T) {
	f := postureDemo(t)
	// An empty PATH dir: a hosted runner has go in /usr/bin, so the dirs /usr/bin:/bin cannot stand for "no go".
	tmp, env := postureTmpEnv(t)
	env = append(env, "PATH="+t.TempDir())
	res := postureStubRun(t, f, "#!/bin/sh\nexit 0\n", env)
	if res.code != 3 || !strings.Contains(res.stderr, "go not found") {
		t.Fatalf("no go on PATH: exit %d stderr=%q, want exit 3 and go not found", res.code, res.stderr)
	}
	requireEmptyDir(t, tmp, "a failing exit")
}

func TestPostureIntegrationStubRunRemovesAReadOnlyModuleCache(t *testing.T) {
	f := postureDemo(t)
	tmp, env := postureTmpEnv(t)
	env = append(env, "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	body := "#!/bin/sh\nmkdir -p \"$GOMODCACHE/m/x\" && echo a > \"$GOMODCACHE/m/x/f\" && chmod 0444 \"$GOMODCACHE/m/x/f\" && chmod 0555 \"$GOMODCACHE/m/x\" \"$GOMODCACHE/m\"\nexit 0\n"
	res := postureStubRun(t, f, body, env)
	if res.code != 5 {
		t.Fatalf("stub run: exit %d stdout=%q stderr=%q, want exit 5", res.code, res.stdout, res.stderr)
	}
	requireEmptyDir(t, tmp, "a read-only module cache")
}

func TestPostureIntegrationStubRunKeepsTheDirsTheCallerSupplied(t *testing.T) {
	f := postureDemo(t)
	tmp, env := postureTmpEnv(t)
	env = append(env, "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	var sentinels []string
	for _, name := range []string{"GOMODCACHE", "GOCACHE", "GOTMPDIR"} {
		d := t.TempDir()
		makeWritable(t, d)
		s := filepath.Join(d, "keep-"+name)
		if err := os.WriteFile(s, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		sentinels = append(sentinels, s)
		env = append(env, name+"="+d)
	}
	res := postureStubRun(t, f, "#!/bin/sh\nshift 5\nexec \"$@\"\n", env)
	if res.code != 5 {
		t.Fatalf("stub run: exit %d stdout=%q stderr=%q, want exit 5", res.code, res.stdout, res.stderr)
	}
	for _, s := range sentinels {
		if _, err := os.Stat(s); err != nil {
			t.Fatalf("a dir the caller supplied was removed: %v", err)
		}
	}
	requireEmptyDir(t, tmp, "a run with caller dirs")
}

// A stub run keeps the caller's TMPDIR: the worktree it makes (the runner's cwd) lies under it.
func TestPostureIntegrationStubRunForwardsTheCallersTmpdir(t *testing.T) {
	f := postureDemo(t)
	tmp, env := postureTmpEnv(t)
	env = append(env, "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	cwdFile := filepath.Join(t.TempDir(), "cwd")
	res := postureStubRun(t, f, "#!/bin/sh\npwd > "+cwdFile+"\nexit 0\n", env)
	if res.code != 5 {
		t.Fatalf("stub run: exit %d stdout=%q stderr=%q, want exit 5", res.code, res.stdout, res.stderr)
	}
	raw, err := os.ReadFile(cwdFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); !strings.HasPrefix(got, tmp+string(os.PathSeparator)) {
		t.Fatalf("the runner ran in %q, want a dir under the caller's TMPDIR %q", got, tmp)
	}
}

// One path that cannot be removed must not stop the removal of the others, and must not change the exit code.
func TestPostureIntegrationStubRunRemovesAllDirsPastAnUnreadableSubdir(t *testing.T) {
	f := postureDemo(t)
	tmp, env := postureTmpEnv(t)
	env = append(env, "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	body := "#!/bin/sh\nmkdir -p \"$GOMODCACHE/m/x\" && echo a > \"$GOMODCACHE/m/x/f\" && chmod 0000 \"$GOMODCACHE/m/x\"\nmkdir -p \"$GOCACHE/c\" \"$GOTMPDIR/t\"\nexit 7\n"
	res := postureStubRun(t, f, body, env)
	if res.code != 5 {
		t.Fatalf("stub run: exit %d stdout=%q stderr=%q, want exit 5 as without the unreadable subdir", res.code, res.stdout, res.stderr)
	}
	requireEmptyDir(t, tmp, "an unreadable subdir")
}

// A removal that fails (the parent dir is read-only) is reported for every dir the script made, not only the first.
func TestPostureIntegrationStubRunTriesEveryDirWhenRemovalFails(t *testing.T) {
	f := postureDemo(t)
	_, env := postureTmpEnv(t)
	env = append(env, "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	res := postureStubRun(t, f, "#!/bin/sh\nchmod 0555 \"${GOMODCACHE%/*}\"\nexit 0\n", env)
	if res.code != 5 {
		t.Fatalf("stub run: exit %d stdout=%q stderr=%q, want exit 5 although removal fails", res.code, res.stdout, res.stderr)
	}
	n := 0
	for _, l := range strings.Split(res.stderr, "\n") {
		if strings.HasPrefix(l, "cannot remove ") {
			n++
		}
	}
	if n != 4 {
		t.Fatalf("%d removal failures reported, want 4 (the worktree dir and the three made dirs): stderr=%q", n, res.stderr)
	}
}

// A relative TMPDIR that starts with "-" must not turn the script's own paths into options: the dirs are made, used and
// removed, on a pass and on a failing exit.
func TestPostureIntegrationStubRunHandlesADashRelativeTmpdir(t *testing.T) {
	f := postureDemo(t)
	scratch := filepath.Join(f.dir, "-scratch")
	if err := os.Mkdir(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	makeWritable(t, scratch)
	if err := os.WriteFile(filepath.Join(f.dir, ".git", "info", "exclude"), []byte("/-scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, env := postureTmpEnv(t)
	env = append(withoutEnv(env, "TMPDIR"), "TMPDIR=-scratch")
	pass := append(append([]string{}, env...), "PATH="+filepath.Dir(goBinary(t))+string(os.PathListSeparator)+"/usr/bin:/bin")
	res := postureStubRun(t, f, "#!/bin/sh\nshift 5\nexec \"$@\"\n", pass)
	if res.code != 5 {
		t.Fatalf("stub run: exit %d stdout=%q stderr=%q, want exit 5", res.code, res.stdout, res.stderr)
	}
	requireEmptyDir(t, scratch, "a dash-relative TMPDIR after a pass")
	res = postureStubRun(t, f, "#!/bin/sh\nexit 0\n", append(append([]string{}, env...), "PATH="+t.TempDir()))
	if res.code != 3 || !strings.Contains(res.stderr, "go not found") {
		t.Fatalf("no go on PATH: exit %d stderr=%q, want exit 3 and go not found", res.code, res.stderr)
	}
	requireEmptyDir(t, scratch, "a dash-relative TMPDIR after a failing exit")
}
