package pgmigrate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// squashHead is the chain head D6430 moved the baseline to (CHAOS-11743): the baseline is the schema at this
// revision, and the files 0139..squashHead are history in archive/.
const squashHead = "0162"

func TestBaselineHeadsAreTheChainHead(t *testing.T) {
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"0066", squashHead}; !reflect.DeepEqual(baseline.Heads, want) {
		t.Errorf("baseline heads %v, want %v", baseline.Heads, want)
	}
	if !strings.Contains(baseline.Schema, "CREATE TABLE public.org_activity (") {
		t.Error("the baseline schema has no org_activity table (revision 0162)")
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range chain {
		if !revisionLess(squashHead, file.Revision) {
			t.Errorf("sql/%s is at or below the baseline %s: it belongs in archive/sql", file.Name, squashHead)
		}
	}
}

// Every revision between the old baseline and the squashed one is known to the build, so a database that records one
// is refused as below the baseline, never as ahead of the build.
func TestArchivedRevisionsAreKnown(t *testing.T) {
	history, err := LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	known := KnownRevisions(history, baseline, nil)
	for revision := 138; revision <= 162; revision++ {
		if id := fmt.Sprintf("%04d", revision); !known[id] {
			t.Errorf("revision %s is not known to the build", id)
		}
	}
}

// git keeps no empty directory: sql/down holds a placeholder so the embedded chain still has the dir LoadDownChain
// reads, and every downgrade does not fail as down_chain_unavailable while the live chain is empty.
func TestDownChainIsEmptyAfterTheSquash(t *testing.T) {
	if _, err := os.Stat("sql/down/README.md"); err != nil {
		t.Errorf("sql/down has no placeholder: %v", err)
	}
	down, err := LoadDownChain()
	if err != nil {
		t.Fatalf("LoadDownChain: %v", err)
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range down {
		if !revisionLess(applicationHead(baseline), file.Revision) {
			t.Errorf("sql/down/%s reverts a revision at or below the baseline", file.Name)
		}
	}
}

func TestDowngradeBelowTheSquashFloorIsRefused(t *testing.T) {
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	down, err := LoadDownChain()
	if err != nil {
		t.Fatal(err)
	}
	r := NewDownRange(baseline, chain, down)
	refusal := r.CheckTarget(DowngradeTarget{Revision: "0161"})
	if refusal == nil || refusal.Code != "below_baseline_floor" || !refusal.PreDatabase {
		t.Errorf("downgrade to 0161 = %+v, want a below_baseline_floor refusal before any database", refusal)
	}
	if refusal := r.CheckTarget(DowngradeTarget{Revision: squashHead}); refusal != nil {
		t.Errorf("downgrade to the baseline %s (nothing to revert) is refused: %+v", squashHead, refusal)
	}
}

func TestDowngradeTextNamesTheBuildsRange(t *testing.T) {
	_, refusal := ParseDowngradeTarget("base", map[string]bool{})
	if refusal == nil {
		t.Fatal("base is accepted as a downgrade target")
	}
	if strings.Contains(refusal.Detail, "0138-0145") || !strings.Contains(refusal.Detail, squashHead) {
		t.Errorf("the unsupported-target text names %q, want the build's range from %s", refusal.Detail, squashHead)
	}
}

// goTool finds the go binary: $GOROOT/bin/go, then the GOROOT the test binary was built with (empty under -trimpath),
// then PATH. It never returns a relative path.
func goTool(getenv func(string) string, goroot string, lookPath func(string) (string, error)) (string, error) {
	for _, root := range []string{getenv("GOROOT"), goroot} {
		if root == "" {
			continue
		}
		candidate := filepath.Join(root, "bin", "go")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	path, err := lookPath("go")
	if err != nil {
		return "", fmt.Errorf("no go binary: GOROOT=%q, built-in GOROOT=%q, and go is not on PATH: %w", getenv("GOROOT"), goroot, err)
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("go found on PATH as the relative path %q", path)
	}
	return path, nil
}

// archivedEmbeds returns every archived file the package in dir embeds, as the go tool resolves its //go:embed
// patterns (quotes, globs and all: included), except the archive manifest.
func archivedEmbeds(t *testing.T, dir string) []string {
	t.Helper()
	tool, err := goTool(os.Getenv, runtime.GOROOT(), exec.LookPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(tool, "list", "-json", ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v: %s", dir, err, stderr.String())
	}
	var pkg struct{ EmbedFiles []string }
	if err := json.Unmarshal(out, &pkg); err != nil {
		t.Fatalf("decode go list %s: %v", dir, err)
	}
	var found []string
	for _, file := range pkg.EmbedFiles {
		if strings.HasPrefix(file, "archive/") && file != "archive/chain_manifest.tsv" {
			found = append(found, file)
		}
	}
	sort.Strings(found)
	return found
}

func TestEmbedArchiveFixtures(t *testing.T) {
	both := []string{"archive/sql/a.sql", "archive/sql/b.sql"}
	for _, name := range []string{"quoted", "glob", "allprefix", "star", "bare"} {
		if got := archivedEmbeds(t, filepath.Join("testdata", "embedarchive", name)); !reflect.DeepEqual(got, both) {
			t.Errorf("fixture %s: archived embeds = %v, want %v", name, got, both)
		}
	}
	if got := archivedEmbeds(t, filepath.Join("testdata", "embedarchive", "clean")); len(got) != 0 {
		t.Errorf("fixture clean embeds no archived SQL and the check found %v", got)
	}
}

func TestGoToolResolution(t *testing.T) {
	root := func(dir string) string {
		bin := filepath.Join(dir, "bin")
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bin, "go"), []byte("x"), 0o755); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	envRoot, builtRoot, emptyRoot := root(t.TempDir()), root(t.TempDir()), t.TempDir()
	noPath := func(string) (string, error) { return "", errors.New("not found") }
	onPath := func(string) (string, error) { return "/usr/bin/go", nil }
	relPath := func(string) (string, error) { return "bin/go", nil }
	env := func(v string) func(string) string {
		return func(k string) string {
			if k == "GOROOT" {
				return v
			}
			return ""
		}
	}
	cases := []struct {
		name, goroot string
		getenv       func(string) string
		lookPath     func(string) (string, error)
		want         string
		fails        bool
	}{
		{"GOROOT env wins", builtRoot, env(envRoot), onPath, filepath.Join(envRoot, "bin", "go"), false},
		{"env GOROOT without a binary falls to built-in", builtRoot, env(emptyRoot), onPath, filepath.Join(builtRoot, "bin", "go"), false},
		{"built-in GOROOT", builtRoot, env(""), onPath, filepath.Join(builtRoot, "bin", "go"), false},
		{"trimpath: GOROOT empty and unset uses PATH", "", env(""), onPath, "/usr/bin/go", false},
		{"nothing resolves", "", env(""), noPath, "", true},
		{"relative PATH result is refused", "", env(""), relPath, "", true},
	}
	for _, c := range cases {
		got, err := goTool(c.getenv, c.goroot, c.lookPath)
		if c.fails {
			if err == nil {
				t.Errorf("%s: got %q, want an error", c.name, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v, want %q", c.name, got, err, c.want)
		}
	}
}

// The archive is history: nothing the binary embeds may be archived SQL, and the archive manifest names every archived
// file with its released sha256.
func TestNoEmbeddedSQLUnderArchive(t *testing.T) {
	for _, pattern := range archivedEmbeds(t, ".") {
		t.Errorf("embeds %s: the binary may embed only archive/chain_manifest.tsv", pattern)
	}
	data, err := os.ReadFile("archive/chain_manifest.tsv")
	if err != nil {
		t.Fatalf("read the archive manifest: %v", err)
	}
	listed := 0
	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			t.Fatalf("archive manifest line %q is not <revision><TAB><sha256><TAB><file name>", line)
		}
		for _, dir := range []string{"archive/sql/", "archive/sql/down/"} {
			body, err := os.ReadFile(dir + fields[2])
			if err != nil {
				t.Errorf("%s%s: %v", dir, fields[2], err)
				continue
			}
			if dir == "archive/sql/" {
				sum := sha256.Sum256(body)
				if hex.EncodeToString(sum[:]) != fields[1] {
					t.Errorf("archive/sql/%s changed after its release", fields[2])
				}
			}
		}
		listed++
	}
	if listed != 24 {
		t.Errorf("the archive manifest lists %d revisions, want 24 (0139..%s)", listed, squashHead)
	}
}
