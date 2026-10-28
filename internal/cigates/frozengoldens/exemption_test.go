package frozengoldens

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const (
	plantBaseline        = "internal/pgmigrate/baseline/head.json"
	plantArchiveManifest = "internal/pgmigrate/archive/chain_manifest.tsv"
	plantArchiveSQL      = "internal/pgmigrate/archive/sql/0139_a.sql"
	plantGolden          = "internal/pgmigrate/testdata/golden/alias.json"
	plantSource          = "internal/pgmigrate/alias_integration_test.go"
)

func sum(body string) string {
	s := sha256.Sum256([]byte(body))
	return hex.EncodeToString(s[:])
}

func writeAt(t *testing.T, repo, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, path)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, path), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// squashRepo is a repository whose base holds a manifest, a baseline and a pinned source with two golden-open calls,
// and whose change branch makes the squash's kind of change set: a new baseline, an edited manifest, a removed
// comparison and an added archive. It returns the repo, the merge base and the exemption that names that change set.
func squashRepo(t *testing.T) (string, string, Exemption) {
	t.Helper()
	repo := t.TempDir()
	baseManifest := "file\t" + plantGolden + "\t" + sum("{}") + "\npins\t" + plantSource + "\t" + strings.Repeat("a", 64) + "\topens=2,finishes=0,skips=0\n"
	for path, body := range map[string]string{
		ManifestPath:  baseManifest,
		plantBaseline: "old head",
		plantGolden:   "{}",
		plantSource:   "package x\nvar _ = venueoracle.OpenGolden(t, a)\nvar _ = venueoracle.OpenGolden(t, b)\n",
		"other.txt":   "x",
	} {
		writeAt(t, repo, path, body)
	}
	git(t, repo, "init", "-q", "-b", "main")
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "base")
	git(t, repo, "update-ref", "refs/remotes/origin/main", "HEAD")
	git(t, repo, "checkout", "-q", "-b", "change")
	base, err := MergeBase(repo)
	if err != nil {
		t.Fatal(err)
	}
	writeAt(t, repo, plantBaseline, "new head")
	writeAt(t, repo, ManifestPath, "file\t"+plantGolden+"\t"+sum("{}")+"\npins\t"+plantSource+"\t"+strings.Repeat("a", 64)+"\topens=1,finishes=0,skips=0\n")
	writeAt(t, repo, plantSource, "package x\nvar _ = venueoracle.OpenGolden(t, a)\n")
	writeAt(t, repo, plantArchiveManifest, "0139\tabc\t0139_a.sql\n")
	writeAt(t, repo, plantArchiveSQL, "-- Revision 0139 (down_revision 0138)\n")
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "squash")
	paths := []string{ManifestPath, plantBaseline, plantArchiveManifest, plantArchiveSQL}
	sort.Strings(paths)
	return repo, base, Exemption{
		BaseManifestSHA256:    sum(baseManifest),
		BaseHeadJSONSHA256:    sum("old head"),
		ArchiveManifestSHA256: sum("0139\tabc\t0139_a.sql\n"),
		Paths:                 paths,
		Cites:                 "D6430 / CHAOS-11743",
	}
}

func gitHalf(t *testing.T, repo, base string, exemptions []Exemption) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repo, ManifestPath))
	if err != nil {
		t.Fatal(err)
	}
	m, err := Parse(strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	problems, err := GitHalf(repo, base, m, exemptions)
	if err != nil {
		t.Fatal(err)
	}
	return problems
}

func TestTheSquashExemptionClearsExactlyItsChangeSet(t *testing.T) {
	repo, base, exemption := squashRepo(t)
	if problems := gitHalf(t, repo, base, nil); len(problems) == 0 {
		t.Fatal("the squash change set passes the git half with no exemption")
	}
	if problems := gitHalf(t, repo, base, []Exemption{exemption}); len(problems) > 0 {
		t.Fatalf("the squash change set with its exemption is refused:\n%s", strings.Join(problems, "\n"))
	}
	for name, change := range map[string]func(e *Exemption){
		"another base manifest":    func(e *Exemption) { e.BaseManifestSHA256 = sum("other manifest") },
		"another base head.json":   func(e *Exemption) { e.BaseHeadJSONSHA256 = sum("other head") },
		"another archive manifest": func(e *Exemption) { e.ArchiveManifestSHA256 = sum("other archive") },
		"a missing path":           func(e *Exemption) { e.Paths = e.Paths[1:] },
		"an extra path": func(e *Exemption) {
			e.Paths = append(append([]string(nil), e.Paths...), "internal/pgmigrate/archive/sql/0140_b.sql")
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := exemption
			changed.Paths = append([]string(nil), exemption.Paths...)
			change(&changed)
			if problems := gitHalf(t, repo, base, []Exemption{changed}); len(problems) == 0 {
				t.Errorf("an exemption with %s clears the change set", name)
			}
		})
	}
}

func TestTheSquashExemptionDoesNotClearAGoldenEdit(t *testing.T) {
	repo, base, exemption := squashRepo(t)
	writeAt(t, repo, plantGolden, "{\"edited\": true}")
	git(t, repo, "commit", "-q", "-am", "edit a golden")
	if problems := gitHalf(t, repo, base, []Exemption{exemption}); len(problems) == 0 {
		t.Fatal("the exemption clears a change set that also edits a frozen golden")
	}
}

// The key is content, not a commit: main moving by an unrelated commit (merged into the branch) leaves the base's
// guarded content as it was, and the exemption still clears the change set.
func TestTheSquashExemptionSurvivesAnUnrelatedMainMove(t *testing.T) {
	repo, _, exemption := squashRepo(t)
	git(t, repo, "checkout", "-q", "main")
	writeAt(t, repo, "other.txt", "moved")
	git(t, repo, "commit", "-q", "-am", "unrelated main move")
	git(t, repo, "update-ref", "refs/remotes/origin/main", "HEAD")
	git(t, repo, "checkout", "-q", "change")
	git(t, repo, "merge", "-q", "--no-edit", "main")
	base, err := MergeBase(repo)
	if err != nil {
		t.Fatal(err)
	}
	if problems := gitHalf(t, repo, base, []Exemption{exemption}); len(problems) > 0 {
		t.Fatalf("an unrelated main move breaks the exemption:\n%s", strings.Join(problems, "\n"))
	}
}

// An ADDED file under the baseline or the archive dir is a change too: the git half sees it.
func TestTheGitHalfSeesAFileAddedUnderTheBaselineOrTheArchive(t *testing.T) {
	for _, added := range []string{"internal/pgmigrate/baseline/extra.json", "internal/pgmigrate/archive/sql/0163_x.sql"} {
		t.Run(added, func(t *testing.T) {
			repo := t.TempDir()
			writeAt(t, repo, ManifestPath, "")
			writeAt(t, repo, plantBaseline, "head")
			writeAt(t, repo, "internal/pgmigrate/archive/README.md", "archive")
			git(t, repo, "init", "-q", "-b", "main")
			git(t, repo, "add", ".")
			git(t, repo, "commit", "-q", "-m", "base")
			git(t, repo, "update-ref", "refs/remotes/origin/main", "HEAD")
			git(t, repo, "checkout", "-q", "-b", "change")
			base, err := MergeBase(repo)
			if err != nil {
				t.Fatal(err)
			}
			writeAt(t, repo, added, "x")
			git(t, repo, "add", ".")
			git(t, repo, "commit", "-q", "-m", "add")
			if problems := gitHalf(t, repo, base, nil); !strings.Contains(strings.Join(problems, "\n"), added) {
				t.Errorf("the git half does not see the added %s: %v", added, problems)
			}
		})
	}
}
