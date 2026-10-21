package nopython_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// testsPyAllowed is the closed list of Python files that may stay under tests/, as the path under tests/ without the
// ".py" suffix. Stems, not file names: a literal file name would make the go.yml path-filter guard treat each file as
// a path a Go test reads, and these files are not inputs of any Go test.
// Anything under tests/tooling/ and tests/docs/ is allowed as a whole.
var testsPyAllowed = map[string]bool{
	"__init__":                             true,
	"conftest":                             true,
	"_env_isolation":                       true,
	"test_go_api_routing_digest_checker":   true,
	"test_go_api_routing_operator_text":    true,
	"scripts/test_strip_agent_attribution": true,
}

func testsTreeRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

func testsTreePythonFiles(t *testing.T, root string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(filepath.Join(root, "tests"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".py") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		found = append(found, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func testsTreeStray(files []string) []string {
	var stray []string
	for _, f := range files {
		if testsPyAllowed[strings.TrimSuffix(strings.TrimPrefix(f, "tests/"), ".py")] || strings.HasPrefix(f, "tests/tooling/") || strings.HasPrefix(f, "tests/docs/") {
			continue
		}
		stray = append(stray, f)
	}
	return stray
}

func TestNoPythonUnderTests(t *testing.T) {
	root := testsTreeRoot(t)
	if _, err := os.Stat(filepath.Join(root, "tests")); err != nil {
		t.Fatal(err)
	}
	if stray := testsTreeStray(testsTreePythonFiles(t, root)); len(stray) > 0 {
		t.Fatalf("Python is dead; %d .py file(s) under tests/ outside the allow list, first: %s", len(stray), stray[0])
	}
}

func TestNoPythonUnderTestsFiresOnPlantedFile(t *testing.T) {
	stray := testsTreeStray([]string{"tests/tooling/x.py", "tests/test_planted.py"})
	if len(stray) != 1 || stray[0] != "tests/test_planted.py" {
		t.Fatalf("guard did not name the planted file: %v", stray)
	}
}

// pyOwnedEntry is one place a Python file may still live, with the leaf that deletes it. An entry ending in "/" is a
// directory prefix; any other entry is one file, written without the ".py" suffix (same reason as testsPyAllowed).
// A new entry needs a new owner leaf; an entry whose last .py is gone must be removed from the list.
type pyOwnedEntry struct {
	stem  string
	owner string
}

var pyOwnedEntries = []pyOwnedEntry{
	{"src/dev_health_ops/", "CHAOS-10375"},
	{"scripts/", "CHAOS-10386/10387/10388 (docs tooling leaves)"},
	{"tests/tooling/", "fcs tooling/docs leaves"},
	{"tests/docs/", "fcs tooling/docs leaves"},
	{"ci/bigboy/", "#4565"},
}

func (e pyOwnedEntry) covers(stem string) bool {
	if strings.HasSuffix(e.stem, "/") {
		return strings.HasPrefix(stem, e.stem)
	}
	return stem == e.stem
}

// pyDeadWalk lists every .py file under root (slash paths), skipping VCS and dependency directories.
func pyDeadWalk(t *testing.T, root string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", ".venv":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".py") {
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// pyDeadVerdict returns the .py files no entry covers and the entries no .py file is left under.
func pyDeadVerdict(files []string) (outside []string, emptied []string) {
	used := map[string]bool{}
	for _, f := range files {
		stem := strings.TrimSuffix(f, ".py")
		covered := false
		for _, e := range pyOwnedEntries {
			if e.covers(stem) {
				used[e.stem] = true
				covered = true
			}
		}
		if !covered && !(strings.HasPrefix(f, "tests/") && len(testsTreeStray([]string{f})) == 0) {
			outside = append(outside, f)
		}
	}
	for _, e := range pyOwnedEntries {
		if !used[e.stem] {
			emptied = append(emptied, e.stem+" ("+e.owner+")")
		}
	}
	return outside, emptied
}

func TestNoTrackedPythonOutsideOwnedEntries(t *testing.T) {
	outside, emptied := pyDeadVerdict(pyDeadWalk(t, testsTreeRoot(t)))
	if len(outside) > 0 {
		t.Fatalf("Python is dead; %d .py file(s) outside the owned entries, first: %s", len(outside), outside[0])
	}
	if len(emptied) > 0 {
		t.Fatalf("owned entry has no .py file left; remove it from pyOwnedEntries: %s", emptied[0])
	}
}

// pyPlantedTree writes one .py file per owned entry, plus the extra paths, under a temp dir that is not the real tree.
func pyPlantedTree(t *testing.T, skipStem string, extra ...string) []string {
	t.Helper()
	root := t.TempDir()
	paths := append([]string{}, extra...)
	for _, e := range pyOwnedEntries {
		if e.stem == skipStem {
			continue
		}
		if strings.HasSuffix(e.stem, "/") {
			paths = append(paths, e.stem+"a.py")
		} else {
			paths = append(paths, e.stem+".py")
		}
	}
	for _, p := range paths {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// skipped directories must not be walked
	for _, skip := range []string{".git", "node_modules", "vendor", ".venv"} {
		if err := os.MkdirAll(filepath.Join(root, skip), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, skip, "skipped.py"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return pyDeadWalk(t, root)
}

func TestNoTrackedPythonFiresOnPlantedRows(t *testing.T) {
	if outside, emptied := pyDeadVerdict(pyPlantedTree(t, "")); len(outside) != 0 || len(emptied) != 0 {
		t.Fatalf("a tree holding exactly the owned entries must pass: outside=%v emptied=%v", outside, emptied)
	}
	for _, planted := range []string{"internal/x.py", "brand/new/dir/y.py", "tests/test_planted.py", "ci/new_tool.py", "scripts_x/z.py"} {
		outside, _ := pyDeadVerdict(pyPlantedTree(t, "", planted))
		if len(outside) != 1 || outside[0] != planted {
			t.Fatalf("planted %s: outside = %v", planted, outside)
		}
	}
	for _, e := range pyOwnedEntries {
		_, emptied := pyDeadVerdict(pyPlantedTree(t, e.stem))
		if len(emptied) != 1 || !strings.HasPrefix(emptied[0], e.stem+" ") {
			t.Fatalf("emptied entry %s: emptied = %v", e.stem, emptied)
		}
	}
}

func ciRequirementsFiles(root string) ([]string, error) {
	return filepath.Glob(filepath.Join(root, "ci", "requirements-*.txt"))
}

func TestNoCIRequirementsFile(t *testing.T) {
	found, err := ciRequirementsFiles(testsTreeRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) > 0 {
		t.Fatalf("Python is dead; no ci/requirements-*.txt may exist, found %s", filepath.Base(found[0]))
	}
}

func TestNoCIRequirementsFileFiresOnPlantedFile(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "ci"), 0o755); err != nil {
		t.Fatal(err)
	}
	if found, _ := ciRequirementsFiles(root); len(found) != 0 {
		t.Fatalf("empty ci/ must pass: %v", found)
	}
	if err := os.WriteFile(filepath.Join(root, "ci", "requirements-x.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if found, _ := ciRequirementsFiles(root); len(found) != 1 {
		t.Fatalf("planted ci/requirements-x.txt not found: %v", found)
	}
}

// The scan reads what git tracks: an untracked .py (a tool cache, a scratch file) is not tracked Python, and must
// neither be reported nor keep an emptied owned entry alive.
func TestPythonScanReadsTrackedFilesOnly(t *testing.T) {
	both := func(t *testing.T, root string) (dead, tests []string) {
		t.Helper()
		return pyDeadWalk(t, root), testsTreePythonFiles(t, root)
	}
	t.Run("untracked .py is not reported", func(t *testing.T) {
		for _, untracked := range []string{".uv-cache/x.py", "scratch/y.py", "tests/.uv-cache/z.py"} {
			root := plantedRepo(t, map[string]string{"tests/test_a.py": "", untracked: ""}, "tests/test_a.py")
			dead, tests := both(t, root)
			if strings.Join(dead, ",") != "tests/test_a.py" || strings.Join(tests, ",") != "tests/test_a.py" {
				t.Errorf("untracked %s: pyDeadWalk=%v testsTreePythonFiles=%v, want only the tracked tests/test_a.py", untracked, dead, tests)
			}
		}
	})
	t.Run("tracked .py outside the owned entries is reported", func(t *testing.T) {
		for _, planted := range []string{"brand/new/dir/y.py", "internal/x.py"} {
			root := plantedRepo(t, map[string]string{planted: ""}, planted)
			if outside, _ := pyDeadVerdict(pyDeadWalk(t, root)); len(outside) != 1 || outside[0] != planted {
				t.Errorf("tracked %s: outside = %v", planted, outside)
			}
		}
	})
	t.Run("untracked .py in an owned prefix does not hide an emptied entry", func(t *testing.T) {
		for _, e := range pyOwnedEntries {
			if !strings.HasSuffix(e.stem, "/") {
				continue
			}
			files := map[string]string{}
			var track []string
			for _, o := range pyOwnedEntries {
				p := o.stem + "a.py"
				if !strings.HasSuffix(o.stem, "/") {
					p = o.stem + ".py"
				}
				if o.stem == e.stem {
					files[p] = "" // present on disk, never staged
					continue
				}
				files[p] = ""
				track = append(track, p)
			}
			root := plantedRepo(t, files, track...)
			_, emptied := pyDeadVerdict(pyDeadWalk(t, root))
			if len(emptied) != 1 || !strings.HasPrefix(emptied[0], e.stem+" ") {
				t.Errorf("entry %s holds only an untracked .py: emptied = %v, want it reported", e.stem, emptied)
			}
		}
	})
}
