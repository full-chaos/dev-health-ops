package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/goldenscan"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedfiles"
)

func write(t *testing.T, file, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The verb as a contributor runs it, from a directory inside the repository:
// its exit code is 1 while the guard still refuses something.
func TestTheVerbFromInsideTheRepository(t *testing.T) {
	repo := t.TempDir()
	write(t, filepath.Join(repo, "go.mod"), "module example\n")
	write(t, filepath.Join(repo, "a", "testdata", "case.json"), "{}")
	write(t, filepath.Join(repo, "a", "testdata", "recorded.json"), `{"python": 1}`)
	write(t, filepath.Join(repo, filepath.FromSlash(recordedfiles.DayOneList)), "# none\n")
	t.Chdir(filepath.Join(repo, "a"))
	// The day-one count of the real repository is not this tree's: the verb
	// reports it, so every run here ends with that one problem.
	steps := []struct {
		args []string
		code int
		file string // a manifest text that must be there afterwards
	}{
		{[]string{"-check"}, 1, ""},
		{[]string{"-kind", "hand-written", "a/testdata/case.json"}, 1, "\thand-written\tcase.json\n"},
		{[]string{"-kind", "python-recorded", "a/testdata/recorded.json"}, 1, "\tpython-recorded\trecorded.json\n"},
		{[]string{"-kind", "fixture", "a/testdata/case.json"}, 1, "\thand-written\tcase.json\n"},
		{[]string{}, 2, ""},
		{[]string{"-no-such-flag"}, 2, ""},
	}
	for _, step := range steps {
		if code := run(step.args); code != step.code {
			t.Errorf("manifest %v: exit %d, want %d", step.args, code, step.code)
		}
		if step.file != "" {
			raw, err := os.ReadFile(filepath.Join(repo, "a", "testdata"+recordedfiles.ManifestSuffix))
			if err != nil || !strings.Contains(string(raw), step.file) {
				t.Errorf("manifest %v: the manifest does not hold %q (%v):\n%s", step.args, step.file, err, raw)
			}
		}
	}
	// A recorded answer that changed: refused without -recorded-again, written with it.
	write(t, filepath.Join(repo, "a", "testdata", "recorded.json"), `{"python": 2}`)
	before, _ := os.ReadFile(filepath.Join(repo, "a", "testdata"+recordedfiles.ManifestSuffix))
	if code := run([]string{"-kind", "python-recorded", "a/testdata/recorded.json"}); code != 1 {
		t.Errorf("a changed recorded answer with no -recorded-again: exit %d, want 1", code)
	}
	if after, _ := os.ReadFile(filepath.Join(repo, "a", "testdata"+recordedfiles.ManifestSuffix)); string(after) != string(before) {
		t.Error("the manifest was written on a refusal")
	}
	run([]string{"-recorded-again", "-kind", "python-recorded", "a/testdata/recorded.json"})
	if after, _ := os.ReadFile(filepath.Join(repo, "a", "testdata"+recordedfiles.ManifestSuffix)); string(after) == string(before) {
		t.Error("with -recorded-again the row did not change")
	}
}

// CHAOS-7890: `manifest -check` runs the unpacked secret scan on every golden of the tree. A golden with a keyed high-entropy value
// and no allowlist row is a problem the check prints; a file with no golden header is not scanned.
func TestTheCheckScansEveryGoldenOfTheTree(t *testing.T) {
	repo := t.TempDir()
	write(t, filepath.Join(repo, "go.mod"), "module example\n")
	// made at run time: no secret-shaped literal is in the source
	value := strings.Repeat("aB3x", 2) + "Zq9Lm2Pw7Rt5Yk8Nc4Vd6Hs" + "J1"
	write(t, filepath.Join(repo, "a", "testdata", "bad.json"), `{"header":{"python_build":"x"},"requests":[{"body":"{\"client_secret\":\"`+value+`\"}"}]}`)
	write(t, filepath.Join(repo, "a", "testdata", "clean.json"), `{"header":{"python_build":"x"},"requests":[{"body":"ok"}]}`)
	write(t, filepath.Join(repo, "a", "testdata", "notagolden.json"), `{"client_secret":"`+value+`"}`)
	problems, err := goldenscan.TreeProblems(repo, nil)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(problems, "\n")
	if len(problems) != 1 || !strings.Contains(joined, "a/testdata/bad.json") || strings.Contains(joined, value) {
		t.Fatalf("the tree scan found %d problem(s), want exactly the one of bad.json, with no value: %v", len(problems), problems)
	}
	stale := []goldenscan.Row{{Path: "a/testdata/clean.json", Key: "credential_id", Shape: "uuid", Count: 1, Triage: "row 1"}}
	problems, err = goldenscan.TreeProblems(repo, stale)
	if err != nil || len(problems) != 2 || !strings.Contains(strings.Join(problems, "\n"), "no hit any more") {
		t.Fatalf("a stale allowlist row was not found: %v %v", problems, err)
	}
}
