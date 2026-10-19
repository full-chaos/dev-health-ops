package cigates_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
)

// CHAOS-10174 PR E: Go port of tests/test_ci_closure_pins_match_uv_lock.py and
// tests/test_runtime_dependency_closure_has_the_redis_client.py. Both read uv.lock / pyproject.toml / ci/requirements-*.txt
// as data, so they outlive the Python test runner. Every check takes a root so a planted defect runs in a temp tree.

type uvDep struct {
	Name  string   `toml:"name"`
	Extra []string `toml:"extra"`
}

type uvPackage struct {
	Name         string             `toml:"name"`
	Version      string             `toml:"version"`
	Dependencies []uvDep            `toml:"dependencies"`
	Optional     map[string][]uvDep `toml:"optional-dependencies"`
}

type uvLock struct {
	Package []uvPackage `toml:"package"`
}

var nonNameRun = regexp.MustCompile(`[-_.]+`)

func canonicalName(name string) string {
	return strings.ToLower(nonNameRun.ReplaceAllString(strings.TrimSpace(name), "-"))
}

func readUVLock(root string) (uvLock, error) {
	var lock uvLock
	raw, err := os.ReadFile(filepath.Join(root, "uv.lock"))
	if err != nil {
		return lock, err
	}
	err = toml.Unmarshal(raw, &lock)
	return lock, err
}

func lockedVersions(lock uvLock) map[string]string {
	out := map[string]string{}
	for _, p := range lock.Package {
		if p.Version != "" {
			out[canonicalName(p.Name)] = p.Version
		}
	}
	return out
}

type pin struct{ name, version string }

func readPins(path string) ([]pin, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pins []pin
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, version, ok := strings.Cut(line, "==")
		if !ok {
			return nil, fmt.Errorf("%s: requirement must be exactly pinned: %s", filepath.Base(path), line)
		}
		pins = append(pins, pin{canonicalName(name), version})
	}
	return pins, nil
}

// closureProblems returns every way one ci/requirements-*.txt closure disagrees with the lock.
func closureProblems(closure string, locked map[string]string) []string {
	pins, err := readPins(closure)
	name := filepath.Base(closure)
	if err != nil {
		return []string{err.Error()}
	}
	if len(pins) == 0 {
		return []string{name + " pins nothing"}
	}
	var out []string
	seen := map[string]bool{}
	for _, p := range pins {
		if seen[p.name] {
			out = append(out, name+" pins a package twice: "+p.name)
		}
		seen[p.name] = true
		have, ok := locked[p.name]
		switch {
		case !ok:
			out = append(out, name+" pins a package uv.lock does not have: "+p.name)
		case have != p.version:
			out = append(out, fmt.Sprintf("%s drifts from uv.lock: %s==%s (uv.lock has %s)", name, p.name, p.version, have))
		}
	}
	sort.Strings(out)
	return out
}

func closureFiles(t *testing.T, root string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "ci", "requirements-*.txt"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	return files
}

func ciClosureProblems(t *testing.T, root string) []string {
	t.Helper()
	files := closureFiles(t, root)
	if len(files) == 0 {
		return []string{"no ci/requirements-*.txt found; the guard would pass vacuously"}
	}
	lock, err := readUVLock(root)
	if err != nil {
		t.Fatal(err)
	}
	locked := lockedVersions(lock)
	var out []string
	for _, f := range files {
		out = append(out, closureProblems(f, locked)...)
	}
	return out
}

func TestCIClosurePinsEqualUVLock(t *testing.T) {
	if problems := ciClosureProblems(t, repoRoot(t)); len(problems) != 0 {
		t.Fatalf("ci closure pins disagree with uv.lock:\n%s", strings.Join(problems, "\n"))
	}
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

const syntheticLock = `version = 1
[[package]]
name = "starlette"
version = "1.7.0"
[[package]]
name = "Some_Pkg"
version = "2.0"
`

// Planted defects: one per class the Python guard exists to catch (drift, unlocked pin, duplicate, loose pin, empty file, no files).
func TestCIClosurePlantedDefects(t *testing.T) {
	cases := []struct {
		name, closure, want string
	}{
		{"clean", "starlette==1.7.0\nsome-pkg==2.0\n", ""},
		{"comments and blanks are skipped", "# c\n\nstarlette==1.7.0\n", ""},
		{"drift", "starlette==1.3.1\n", "drifts from uv.lock: starlette==1.3.1 (uv.lock has 1.7.0)"},
		{"unlocked", "starlette==1.7.0\nghost==1\n", "pins a package uv.lock does not have: ghost"},
		{"duplicate", "starlette==1.7.0\nstarlette==1.7.0\n", "pins a package twice: starlette"},
		{"loose pin", "starlette>=1.7.0\n", "must be exactly pinned"},
		{"empty", "# only a comment\n", "pins nothing"},
		{"name canonicalisation", "SOME.PKG==9\n", "drifts from uv.lock: some-pkg==9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, filepath.Join(root, "uv.lock"), syntheticLock)
			writeFile(t, filepath.Join(root, "ci", "requirements-x.txt"), tc.closure)
			got := strings.Join(ciClosureProblems(t, root), "\n")
			if tc.want == "" && got != "" {
				t.Fatalf("clean closure flagged: %s", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("defect %q not caught; got %q", tc.want, got)
			}
		})
	}
	t.Run("no closure files", func(t *testing.T) {
		root := t.TempDir()
		writeFile(t, filepath.Join(root, "uv.lock"), syntheticLock)
		if got := ciClosureProblems(t, root); len(got) != 1 || !strings.Contains(got[0], "vacuously") {
			t.Fatalf("an empty ci/ must fail, got %v", got)
		}
	})
}

// runtimeClosure walks uv.lock the way an install without dev extras does: the project's base dependencies, following each
// dependency's requested extras. The project's own optional-dependencies (dev) are never entered.
func runtimeClosure(lock uvLock, project string) (map[string]bool, error) {
	pkgs := map[string]uvPackage{}
	for _, p := range lock.Package {
		pkgs[p.Name] = p
	}
	root, ok := pkgs[project]
	if !ok {
		return nil, fmt.Errorf("uv.lock has no package %q", project)
	}
	type item struct {
		name   string
		extras []string
	}
	var stack []item
	for _, d := range root.Dependencies {
		stack = append(stack, item{d.Name, d.Extra})
	}
	seen := map[string]bool{}
	names := map[string]bool{}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		extras := append([]string(nil), cur.extras...)
		sort.Strings(extras)
		key := cur.name + "[" + strings.Join(extras, ",") + "]"
		if seen[key] {
			continue
		}
		seen[key] = true
		names[cur.name] = true
		pkg, ok := pkgs[cur.name]
		if !ok {
			return nil, fmt.Errorf("uv.lock dependency %q has no package entry", cur.name)
		}
		edges := append([]uvDep(nil), pkg.Dependencies...)
		for _, e := range cur.extras {
			edges = append(edges, pkg.Optional[e]...)
		}
		for _, d := range edges {
			stack = append(stack, item{d.Name, d.Extra})
		}
	}
	return names, nil
}

var requirementHead = regexp.MustCompile(`^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[([^\]]*)\])?`)

// pyprojectDeclaresRedis reports whether [project].dependencies names redis or limits[redis]: the api image installs with
// `pip install .`, which reads pyproject.toml and never uv.lock.
func pyprojectDeclaresRedis(root string) (bool, error) {
	raw, err := os.ReadFile(filepath.Join(root, "pyproject.toml"))
	if err != nil {
		return false, err
	}
	var doc struct {
		Project struct {
			Dependencies []string `toml:"dependencies"`
		} `toml:"project"`
	}
	if err := toml.Unmarshal(raw, &doc); err != nil {
		return false, err
	}
	for _, line := range doc.Project.Dependencies {
		m := requirementHead.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name := canonicalName(m[1])
		if name == "redis" {
			return true, nil
		}
		if name == "limits" {
			for _, extra := range strings.Split(m[2], ",") {
				if strings.EqualFold(strings.TrimSpace(extra), "redis") {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func TestLockedRuntimeClosureIncludesRedis(t *testing.T) {
	lock, err := readUVLock(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	closure, err := runtimeClosure(lock, "dev-health-ops")
	if err != nil {
		t.Fatal(err)
	}
	if !closure["redis"] {
		t.Fatal("redis is not in the locked runtime closure of dev-health-ops: the api rate limiter's redis:// storage imports it, so an install without dev extras crashes the API at boot")
	}
	// A walk that cannot tell a dev-only package from a runtime one would pass for any lock.
	if closure["fakeredis"] {
		t.Fatal("fakeredis (a dev extra) is in the runtime closure: the walk is counting dev extras")
	}
	var project uvPackage
	for _, p := range lock.Package {
		if p.Name == "dev-health-ops" {
			project = p
		}
	}
	devOnly := false
	for _, d := range project.Optional["dev"] {
		if d.Name == "fakeredis" {
			devOnly = true
		}
	}
	if !devOnly {
		t.Fatal("expected fakeredis as a dev-only dependency in uv.lock; the vacuity check lost its anchor")
	}
}

func TestPyprojectRuntimeDependenciesDeclareTheRedisClient(t *testing.T) {
	ok, err := pyprojectDeclaresRedis(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("[project].dependencies declares neither redis nor limits[redis]: `pip install .` (the api image) would omit the client the rate limiter needs")
	}
}

const redisLockWith = `[[package]]
name = "app"
version = "1"
dependencies = [{ name = "limits", extra = ["redis"] }]
[package.optional-dependencies]
dev = [{ name = "fakeredis" }]
[[package]]
name = "limits"
version = "3"
[package.optional-dependencies]
redis = [{ name = "redis" }]
[[package]]
name = "redis"
version = "5"
[[package]]
name = "fakeredis"
version = "2"
dependencies = [{ name = "redis" }]
`

// Planted defects: redis only reachable through the dev extra (the real incident), and a stale pyproject.
func TestRedisClosurePlantedDefects(t *testing.T) {
	parse := func(body string) uvLock {
		var l uvLock
		if err := toml.Unmarshal([]byte(body), &l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	good, err := runtimeClosure(parse(redisLockWith), "app")
	if err != nil || !good["redis"] || good["fakeredis"] {
		t.Fatalf("extras walk wrong: %v %v", good, err)
	}
	// The extra is not requested: redis must vanish even though limits has the extra defined.
	noExtra := strings.Replace(redisLockWith, `, extra = ["redis"]`, "", 1)
	if c, _ := runtimeClosure(parse(noExtra), "app"); c["redis"] {
		t.Fatal("redis reached without the extra being requested: the walk follows unrequested extras")
	}
	// The incident shape: redis pulled only by the dev extra.
	devOnly := strings.Replace(redisLockWith, `dependencies = [{ name = "limits", extra = ["redis"] }]`, `dependencies = [{ name = "limits" }]`, 1)
	if c, _ := runtimeClosure(parse(devOnly), "app"); c["redis"] {
		t.Fatal("redis reached through the dev extra: the guard cannot see the api-image crash")
	}

	cases := []struct {
		name, deps string
		want       bool
	}{
		{"limits redis extra", `"limits[redis]>=3.0.0"`, true},
		{"limits extras list", `"limits[async-redis, redis]>=3"`, true},
		{"plain redis", `"redis>=5"`, true},
		{"limits without the extra", `"limits>=3.0.0"`, false},
		{"limits async-redis only", `"limits[async-redis]>=3"`, false},
		{"fakeredis is not redis", `"fakeredis>=2"`, false},
	}
	for _, tc := range cases {
		root := t.TempDir()
		writeFile(t, filepath.Join(root, "pyproject.toml"), "[project]\nname = \"x\"\ndependencies = [\n"+tc.deps+",\n]\n")
		got, err := pyprojectDeclaresRedis(root)
		if err != nil || got != tc.want {
			t.Fatalf("%s: got %v err %v, want %v", tc.name, got, err, tc.want)
		}
	}
}
