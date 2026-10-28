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
	plantSource2         = "internal/api/health/frozen_python_test.go"
	plantUnpinned        = "internal/pgmigrate/runner_test.go"
	plantListedElsewhere = "internal/api/health/testdata/fixture.json"
	plantLegacySource    = "internal/legacy/old_test.go"
)

// squashTipManifest is the manifest the squash fixture's change branch holds.
func squashTipManifest() string {
	return "file\t" + plantGolden + "\t" + sum("{}") + "\nfile\t" + plantListedElsewhere + "\t" + sum("[]") + "\n" +
		"pins\t" + plantSource + "\t" + strings.Repeat("a", 64) + "\topens=1,finishes=0,skips=0\n" +
		"pins\t" + plantSource2 + "\t" + strings.Repeat("b", 64) + "\topens=1,finishes=0,skips=0\n"
}

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
	baseManifest := "file\t" + plantGolden + "\t" + sum("{}") + "\nfile\t" + plantListedElsewhere + "\t" + sum("[]") + "\n" +
		"pins\t" + plantSource + "\t" + strings.Repeat("a", 64) + "\topens=2,finishes=0,skips=0\n" +
		"pins\t" + plantSource2 + "\t" + strings.Repeat("b", 64) + "\topens=1,finishes=0,skips=0\n" +
		"pins\t" + plantLegacySource + "\t" + strings.Repeat("d", 64) + "\topens=1,finishes=0,skips=0\n"
	for path, body := range map[string]string{
		ManifestPath:         baseManifest,
		plantBaseline:        "old head",
		plantGolden:          "{}",
		plantSource:          "package x\nvar _ = venueoracle.OpenGolden(t, a)\nvar _ = venueoracle.OpenGolden(t, b)\n",
		plantSource2:         "package y\nvar _ = venueoracle.OpenGolden(t, c)\n",
		plantUnpinned:        "package x\n",
		plantListedElsewhere: "[]",
		plantLegacySource:    "package z\nvar _ = venueoracle.OpenGolden(t, d)\n",
		"other.txt":          "x",
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
	writeAt(t, repo, ManifestPath, squashTipManifest())
	git(t, repo, "rm", "-q", plantLegacySource)
	writeAt(t, repo, plantSource, "package x\nvar _ = venueoracle.OpenGolden(t, a)\n")
	writeAt(t, repo, plantArchiveManifest, "0139\tabc\t0139_a.sql\n")
	writeAt(t, repo, plantArchiveSQL, "-- Revision 0139 (down_revision 0138)\n")
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "squash")
	paths := []string{ManifestPath, plantBaseline, plantArchiveManifest, plantArchiveSQL}
	sort.Strings(paths)
	content := inputDigest(t, repo, baseManifest)
	return repo, base, Exemption{
		BaseManifestSHA256:    sum(baseManifest),
		BaseHeadJSONSHA256:    sum("old head"),
		ArchiveManifestSHA256: sum("0139\tabc\t0139_a.sql\n"),
		Paths:                 paths,
		ContentSHA256:         content,
		WaivedBelowBase: []string{
			plantLegacySource + ": a test source the base manifest pins is missing",
			plantSource + ": golden-open calls went from 2 to 1 against the base",
		},
		Cites: "D6430 / CHAOS-11743",
	}
}

func inputDigest(t *testing.T, repo, baseManifest string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repo, ManifestPath))
	if err != nil {
		t.Fatal(err)
	}
	head, err := Parse(strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	base, err := Parse(strings.NewReader(baseManifest))
	if err != nil {
		t.Fatal(err)
	}
	content, err := InputDigest(repo, head, base)
	if err != nil {
		t.Fatal(err)
	}
	return content
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
		"another content digest":   func(e *Exemption) { e.ContentSHA256 = sum("other content") },
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

// The exemption pins the content of the change set too: the same guarded paths with other bytes (an archived down
// file edited, or a changed baseline re-pinned in the manifest) is another change set, and the guard refuses it.
func TestTheSquashExemptionDoesNotClearOtherContentUnderTheSamePaths(t *testing.T) {
	for name, edit := range map[string]func(t *testing.T, repo string){
		"an edited archived file": func(t *testing.T, repo string) {
			writeAt(t, repo, plantArchiveSQL, "-- Revision 0139 (down_revision 0138)\nDROP TABLE users;\n")
		},
		"a changed baseline, re-pinned in the manifest": func(t *testing.T, repo string) {
			writeAt(t, repo, plantBaseline, "new head with another seed row")
			writeAt(t, repo, ManifestPath, "file\t"+plantBaseline+"\t"+sum("new head with another seed row")+"\n"+squashTipManifest())
		},
	} {
		t.Run(name, func(t *testing.T) {
			repo, base, exemption := squashRepo(t)
			if problems := gitHalf(t, repo, base, []Exemption{exemption}); len(problems) > 0 {
				t.Fatalf("the squash change set itself is refused: %v", problems)
			}
			edit(t, repo)
			git(t, repo, "commit", "-q", "-am", "same paths, other content")
			if problems := gitHalf(t, repo, base, []Exemption{exemption}); len(problems) == 0 {
				t.Errorf("the exemption clears %s", name)
			}
		})
	}
}

// The exemption waives only its own findings. Every other check of the git half still runs when it matches: a frozen
// comparison removed beyond the squash's own (a pinned source the squash lowered, lowered again; another pinned source
// lowered) and a guarded path the squash does not name (an added baseline file) are still refused.
func TestTheSquashExemptionWaivesOnlyItsOwnFindings(t *testing.T) {
	for name, edit := range map[string]func(t *testing.T, repo string){
		"the squash's pinned source rewritten (reviewer's repro)": func(t *testing.T, repo string) {
			writeAt(t, repo, plantSource, "package x\n")
		},
		"another pinned source loses its comparison": func(t *testing.T, repo string) {
			writeAt(t, repo, plantSource2, "package y\n")
		},
		"an added baseline file beside the squash": func(t *testing.T, repo string) {
			writeAt(t, repo, "internal/pgmigrate/baseline/extra.json", "{}")
		},
	} {
		t.Run(name, func(t *testing.T) {
			repo, base, exemption := squashRepo(t)
			if problems := gitHalf(t, repo, base, []Exemption{exemption}); len(problems) > 0 {
				t.Fatalf("the squash change set itself is refused: %v", problems)
			}
			edit(t, repo)
			git(t, repo, "add", ".")
			git(t, repo, "commit", "-q", "-m", "one more change beside the squash")
			if problems := gitHalf(t, repo, base, []Exemption{exemption}); len(problems) == 0 {
				t.Errorf("the exemption hides %s", name)
			}
		})
	}
}

// Every file the guard reads is an input of the exemption: a byte change, an added, deleted or renamed file in any
// input class beside the squash is another change set, and the guard refuses it. Only a file the guard never reads
// (other.txt) leaves the exemption matching.
func TestEveryGuardInputChangeBesideTheSquashIsRefused(t *testing.T) {
	type op struct{ write, body, remove, from, to string }
	skip := "package x\nvar _ = venueoracle.OpenGolden(t, a)\nfunc f() { t.Skip() }\n"
	for _, cell := range []struct {
		name  string
		ops   []op
		clear bool
	}{
		{name: "manifest/count-preserving edit (a comment line)", ops: []op{{write: ManifestPath, body: "# note\n" + squashTipManifest()}}},
		{name: "manifest/edited (a pins row dropped)", ops: []op{{write: ManifestPath, body: strings.SplitAfter(squashTipManifest(), "\n")[0]}}},
		{name: "manifest-listed file outside the guarded dirs/edited", ops: []op{{write: plantListedElsewhere, body: "[1]"}}},
		{name: "manifest-listed file outside the guarded dirs/deleted", ops: []op{{remove: plantListedElsewhere}}},
		{name: "base-pinned source outside the pinned dirs/added back", ops: []op{{write: plantLegacySource, body: "package z\nvar _ = venueoracle.OpenGolden(t, d)\n"}}},
		{name: "golden/edited", ops: []op{{write: plantGolden, body: "{\"x\":1}"}}},
		{name: "golden/added", ops: []op{{write: "internal/pgmigrate/testdata/golden/new.json", body: "{}"}}},
		{name: "golden/deleted", ops: []op{{remove: plantGolden}}},
		{name: "golden/renamed", ops: []op{{from: plantGolden, to: "internal/pgmigrate/testdata/golden/alias2.json"}}},
		{name: "waived source/edited (an extra t.Skip, opens unchanged)", ops: []op{{write: plantSource, body: skip}}},
		{name: "waived source/count-preserving edit (a comment)", ops: []op{{write: plantSource, body: "package x\n// note\nvar _ = venueoracle.OpenGolden(t, a)\n"}}},
		{name: "waived source/deleted", ops: []op{{remove: plantSource}}},
		{name: "waived source/renamed", ops: []op{{from: plantSource, to: "internal/pgmigrate/alias2_integration_test.go"}}},
		{name: "pinned source/edited (an extra t.Skip, opens unchanged)", ops: []op{{write: plantSource2, body: "package y\nvar _ = venueoracle.OpenGolden(t, c)\nfunc f() { t.Skip() }\n"}}},
		{name: "pinned source/count-preserving edit (a comment)", ops: []op{{write: plantSource2, body: "package y\n// note\nvar _ = venueoracle.OpenGolden(t, c)\n"}}},
		{name: "pinned source/deleted", ops: []op{{remove: plantSource2}}},
		{name: "pinned source/renamed", ops: []op{{from: plantSource2, to: "internal/api/health/frozen_python2_test.go"}}},
		{name: "unpinned source in a pinned dir/edited", ops: []op{{write: plantUnpinned, body: "package x\nconst pin = \"" + strings.Repeat("c", 64) + "\"\n"}}},
		{name: "unpinned source in a pinned dir/count-preserving edit (a comment)", ops: []op{{write: plantUnpinned, body: "package x\n// note\n"}}},
		{name: "unpinned source in a pinned dir/added", ops: []op{{write: "internal/pgmigrate/new_test.go", body: "package x\n"}}},
		{name: "unpinned source in a pinned dir/deleted", ops: []op{{remove: plantUnpinned}}},
		{name: "unpinned source in a pinned dir/renamed", ops: []op{{from: plantUnpinned, to: "internal/pgmigrate/runner2_test.go"}}},
		{name: "baseline/edited", ops: []op{{write: plantBaseline, body: "new head, edited"}}},
		{name: "baseline/added", ops: []op{{write: "internal/pgmigrate/baseline/extra.json", body: "{}"}}},
		{name: "baseline/deleted", ops: []op{{remove: plantBaseline}}},
		{name: "baseline/renamed", ops: []op{{from: plantBaseline, to: "internal/pgmigrate/baseline/head2.json"}}},
		{name: "archive/edited", ops: []op{{write: plantArchiveSQL, body: "-- Revision 0139 (down_revision 0138)\n-- note\n"}}},
		{name: "archive/added", ops: []op{{write: "internal/pgmigrate/archive/sql/0140_b.sql", body: "x"}}},
		{name: "archive/deleted", ops: []op{{remove: plantArchiveSQL}}},
		{name: "archive/renamed", ops: []op{{from: plantArchiveSQL, to: "internal/pgmigrate/archive/sql/0139_b.sql"}}},
		{name: "archive manifest/count-preserving edit", ops: []op{{write: plantArchiveManifest, body: "0139\tabc\t0139_a.sql\n\n"}}},
		{name: "control: a file the guard never reads", ops: []op{{write: "other.txt", body: "y"}}, clear: true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			repo, base, exemption := squashRepo(t)
			before := gitHalf(t, repo, base, []Exemption{exemption})
			if len(before) > 0 {
				t.Fatalf("the squash change set itself is refused: %v", before)
			}
			data, err := os.ReadFile(filepath.Join(repo, ManifestPath))
			if err != nil {
				t.Fatal(err)
			}
			m, err := Parse(strings.NewReader(string(data)))
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range cell.ops {
				switch {
				case o.write != "":
					writeAt(t, repo, o.write, o.body)
				case o.remove != "":
					git(t, repo, "rm", "-q", o.remove)
				default:
					git(t, repo, "mv", o.from, o.to)
				}
			}
			git(t, repo, "add", "-A")
			git(t, repo, "commit", "-q", "-m", cell.name)
			if data, err := os.ReadFile(filepath.Join(repo, ManifestPath)); err == nil {
				if m, err = Parse(strings.NewReader(string(data))); err != nil {
					t.Fatal(err)
				}
			}
			problems, err := GitHalf(repo, base, m, []Exemption{exemption})
			if err != nil {
				t.Fatal(err)
			}
			if cell.clear && len(problems) > 0 {
				t.Errorf("%s breaks the exemption: %v", cell.name, problems)
			}
			if !cell.clear && len(problems) == 0 {
				t.Errorf("the exemption clears %s", cell.name)
			}
		})
	}
}
