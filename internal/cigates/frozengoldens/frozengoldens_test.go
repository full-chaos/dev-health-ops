package frozengoldens

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func mustLoad(t *testing.T) Manifest {
	t.Helper()
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) < 10 || len(m.Pins) < 5 {
		t.Fatalf("the manifest lists %d files and %d pin sources: it guards nothing", len(m.Files), len(m.Pins))
	}
	return m
}

func TestTheFrozenGoldensAndTheirPinsAreUnchanged(t *testing.T) {
	if problems := Check(repoRoot(t), mustLoad(t)); len(problems) > 0 {
		t.Errorf("the frozen goldens or their pins changed (they are never edited; %s is guarded too):\n%s", ManifestPath, strings.Join(problems, "\n"))
	}
}

// The git half: a change set that touches a guarded path is red. In CI it needs the base commit (the leg checks out
// with fetch-depth 0); without one it FAILS, off CI it skips.
func TestNoChangeSetTouchesAFrozenGoldenOrTheManifest(t *testing.T) {
	root := repoRoot(t)
	base, err := MergeBase(root)
	if err != nil {
		if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
			t.Fatalf("CI has no base commit to diff against, so the guard cannot clear this change set (the leg must check out with fetch-depth: 0): %v", err)
		}
		t.Skipf("no origin/main here: %v", err)
	}
	changed, err := Changed(root, base, mustLoad(t).GuardedPaths())
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) > 0 {
		t.Errorf("this change set edits a frozen golden, a pin source list or the manifest: %v\nThe frozen files are never rewritten; only a human-approved change to %s may do this", changed, ManifestPath)
	}
}

// copyTree copies the guarded files, the pinned test sources and the golden dirs into a temp dir.
func copyTree(t *testing.T, m Manifest) string {
	t.Helper()
	root, dest := repoRoot(t), t.TempDir()
	paths := map[string]bool{}
	for path := range m.Files {
		paths[path] = true
	}
	for path := range m.Pins {
		paths[path] = true
	}
	for _, dir := range append(append([]string(nil), PinnedDirs...), GoldenDirs...) {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !entry.IsDir() && (strings.HasSuffix(entry.Name(), "_test.go") || strings.HasPrefix(dir, "internal/pgmigrate/testdata") || strings.Contains(dir, "golden")) {
				paths[dir+"/"+entry.Name()] = true
			}
		}
	}
	for path := range paths {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(dest, path)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dest
}

func TestTheGuardIsRedOnEveryKindOfPlantedChange(t *testing.T) {
	m := mustLoad(t)
	if problems := Check(copyTree(t, m), m); len(problems) > 0 {
		t.Fatalf("an untouched copy is not clean: %v", problems)
	}
	var someGolden, someSource string
	for path := range m.Files {
		if strings.Contains(path, "testdata/golden/alias.json") {
			someGolden = path
		}
	}
	for path := range m.Pins {
		if strings.HasSuffix(path, "alias_integration_test.go") {
			someSource = path
		}
	}
	if someGolden == "" || someSource == "" {
		t.Fatal("the plant targets are not in the manifest")
	}
	for name, plant := range map[string]struct {
		apply func(root string)
		want  string
	}{
		"a flipped byte in a frozen golden": {func(root string) {
			path := filepath.Join(root, someGolden)
			data, _ := os.ReadFile(path)
			data[len(data)/2] ^= 1
			_ = os.WriteFile(path, data, 0o644)
		}, someGolden + ": content changed"},
		"a changed pin": {func(root string) {
			path := filepath.Join(root, someSource)
			data, _ := os.ReadFile(path)
			pin := pinLiteral.FindSubmatch(data)[1]
			swapped := strings.Replace(string(data), string(pin), strings.Repeat("0", 64), 1)
			_ = os.WriteFile(path, []byte(swapped), 0o644)
		}, someSource + ": sha256 pins are"},
		"a pin added to an unlisted test source": {func(root string) {
			_ = os.WriteFile(filepath.Join(root, "internal/pgmigrate/new_pin_test.go"), []byte("package pgmigrate\n\nconst x = \""+strings.Repeat("a", 64)+"\"\n"), 0o644)
		}, "internal/pgmigrate/new_pin_test.go: sha256 pins are"},
		"a new file in a golden dir": {func(root string) {
			_ = os.WriteFile(filepath.Join(root, "internal/pgmigrate/testdata/golden/extra.json"), []byte("{}"), 0o644)
		}, "extra.json: a golden file the manifest does not list"},
		"a deleted golden": {func(root string) { _ = os.Remove(filepath.Join(root, someGolden)) }, someGolden + ": missing"},
	} {
		t.Run(name, func(t *testing.T) {
			root := copyTree(t, m)
			plant.apply(root)
			if joined := strings.Join(Check(root, m), "\n"); !strings.Contains(joined, plant.want) {
				t.Errorf("Check did not report %q:\n%s", plant.want, joined)
			}
		})
	}
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func TestTheGitHalfSeesAPlantedEditAndFailsClosedWithoutABase(t *testing.T) {
	m := mustLoad(t)
	repo := t.TempDir()
	golden := "internal/pgmigrate/testdata/golden/alias.json"
	other := "other.txt"
	for path, body := range map[string]string{golden: "{}", other: "x", ManifestPath: "m"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, path)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, path), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git(t, repo, "init", "-q", "-b", "main")
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "base")
	if _, err := MergeBase(repo); err == nil {
		t.Fatal("a repository with no origin/main has a merge base: the guard would not fail closed")
	}
	git(t, repo, "update-ref", "refs/remotes/origin/main", "HEAD")
	git(t, repo, "checkout", "-q", "-b", "change")
	base, err := MergeBase(repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, other), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "commit", "-q", "-am", "unrelated")
	if got, err := Changed(repo, base, m.GuardedPaths()); err != nil || len(got) != 0 {
		t.Fatalf("an unrelated change reads as %v, %v", got, err)
	}
	for _, path := range []string{golden, ManifestPath} {
		if err := os.WriteFile(filepath.Join(repo, path), []byte("changed"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git(t, repo, "commit", "-q", "-am", "plant")
	got, err := Changed(repo, base, m.GuardedPaths())
	if err != nil || strings.Join(got, ",") != golden+","+ManifestPath && strings.Join(got, ",") != ManifestPath+","+golden {
		t.Fatalf("Changed = %v, %v, want the golden and the manifest", got, err)
	}
}
