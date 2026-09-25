package localgit

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestModeText(t *testing.T) {
	for mode, want := range map[string]string{
		"100644": "33188", "100755": "33261", "120000": "40960", "160000": "57344",
		"000000": "000000", "": "000000", "0644": "420",
	} {
		if got := modeText(mode); got != want {
			t.Errorf("modeText(%q) = %q, want %q", mode, got, want)
		}
	}
}

func TestRepoIDIsTheDigestOfTheRemoteOrPath(t *testing.T) {
	if got := digestUUID("/tmp/some/repo").String(); got != "2ee1ea9e-70bb-62c6-d667-c117ed6ad99a" {
		t.Errorf("path digest = %s", got)
	}
	if got := digestUUID("https://example.com/acme/widgets.git").String(); got != "84803486-d353-a48d-6b0e-6a3eee4cd210" {
		t.Errorf("url digest = %s", got)
	}
}

func TestParseRawDiff(t *testing.T) {
	raw := ":100644 100644 aaa bbb M\x00a.txt\x00:100644 100755 aaa bbb R100\x00old.txt\x00new.txt\x00:000000 100644 0 bbb A\x00added.txt\x00"
	diffs, ok := parseRawDiff([]byte(raw))
	if !ok || len(diffs) != 3 {
		t.Fatalf("diffs = %+v, ok = %v", diffs, ok)
	}
	if diffs[1].APath != "old.txt" || diffs[1].BPath != "new.txt" || diffs[1].NewMode != "100755" {
		t.Errorf("rename = %+v", diffs[1])
	}
	if diffs[2].OldMode != "000000" || diffs[2].APath != "added.txt" {
		t.Errorf("add = %+v", diffs[2])
	}
	// A newline in a path cuts the entry there (readline chunks), and the rest is dropped.
	diffs, ok = parseRawDiff([]byte(":100644 100644 a b A\x00new\nline.txt\x00"))
	if !ok || len(diffs) != 1 || diffs[0].BPath != "new" {
		t.Errorf("newline path = %+v, ok = %v", diffs, ok)
	}
	// A path that is not UTF-8 raises in Python: the whole commit has no rows.
	if _, ok := parseRawDiff([]byte(":100644 100644 a b A\x00bad\xff.txt\x00")); ok {
		t.Error("invalid UTF-8 was accepted")
	}
	// A chunk that cannot be unpacked raises too.
	if _, ok := parseRawDiff([]byte(":100644 100644 a b\x00x\x00")); ok {
		t.Error("a short header was accepted")
	}
}

func TestPullRequestNumbers(t *testing.T) {
	cases := []struct {
		message string
		number  int
		ok      bool
		gitlab  bool
	}{
		{"Merge pull request #12 from a/b\n\nTitle", 12, true, false},
		{"Merge pull request #12abc from a/b", 0, false, false},
		{"Merge pull request #12é from a/b", 0, false, false},
		{"Merge pull request #12_ from a/b", 0, false, false},
		{"Merge pull request #９９ from a/b", 99, true, false},
		{"Title\n\nMerge pull request #7 later", 7, true, false},
		{"prefix Merge pull request #7", 0, false, false},
		{"x\nSee merge request g/p!41 and g/o!42", 42, true, true},
		{"See merge request g/p!41abc", 0, false, true},
		{"See merge requests !5", 0, false, true},
		{"aSee merge request !5", 0, false, true},
		{"See merge request\n!5", 0, false, true},
	}
	for _, tc := range cases {
		var number int
		var ok bool
		if tc.gitlab {
			number, ok = gitlabNumber(tc.message)
		} else {
			number, ok = prNumber(githubMergeRE, tc.message)
		}
		if number != tc.number || ok != tc.ok {
			t.Errorf("%q = %d, %v; want %d, %v", tc.message, number, ok, tc.number, tc.ok)
		}
	}
}

func TestFirstMeaningfulTitleLine(t *testing.T) {
	for message, want := range map[string]string{
		"Merge pull request #1 from a/b\n\n  Real title  \n":   "Real title",
		"Merge branch 'x'\nSee merge request g/p!2\n\t\nThird": "Third",
		"   spaced  ": "spaced",
	} {
		got := firstMeaningfulTitleLine(message)
		if got == nil || *got != want {
			t.Errorf("%q = %v, want %q", message, got, want)
		}
	}
	for _, message := range []string{"", "Merge branch 'a'\n", "  \n \n"} {
		if got := firstMeaningfulTitleLine(message); got != nil {
			t.Errorf("%q = %q, want nil", message, *got)
		}
	}
}

func TestParseActorAndDate(t *testing.T) {
	cases := []struct {
		line, name, email string
		hasEmail          bool
		epoch             int64
	}{
		{"author Jane Doe <jane@example.com> 1700000000 +0530", "Jane Doe", "jane@example.com", true, 1700000000},
		{"author Just A Name 1700000000 +0000", "Just A Name", "", false, 1700000000},
		{"author <only@example.com> 1700000000 +0000", "", "only@example.com", true, 1700000000},
		{"committer   Spaced   <c@example.com> 1700003600 -0700", "  Spaced", "c@example.com", true, 1700003600},
		{"author A <a@b> <c@d> 5 +0000", "A", "a@b", true, 5},
	}
	for _, tc := range cases {
		name, email, epoch := parseActorAndDate(tc.line)
		if name == nil || *name != tc.name || (email != nil) != tc.hasEmail || (tc.hasEmail && *email != tc.email) || epoch != tc.epoch {
			t.Errorf("%q = %v %v %d", tc.line, name, email, epoch)
		}
	}
}

func TestPythonJSONString(t *testing.T) {
	for in, want := range map[string]string{
		"plain": `"plain"`, `q"uote`: `"q\"uote"`, "tab\t": `"tab\t"`, "\x01": `"\u0001"`, "é✓": `"é✓"`, `back\slash`: `"back\\slash"`, "\x7f": "\"\x7f\"",
	} {
		if got := pythonJSONString(in); got != want {
			t.Errorf("pythonJSONString(%q) = %s, want %s", in, got, want)
		}
	}
}

// git in a temporary repository: numstat pairing, binary files, root commits.
func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=T", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=T", "GIT_COMMITTER_EMAIL=t@example.com",
		"GIT_AUTHOR_DATE=1700000000 +0000", "GIT_COMMITTER_DATE=1700000000 +0000")
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestCommitStatsOfARealRepository(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git binary")
	}
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a.txt", "1\n2\n")
	write("bin.dat", "\x00\x01")
	write("keep.txt", "same\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "root")
	write("a.txt", "1\n2\n3\n")
	write("bin.dat", "\x00\x02")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "second")

	repo, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	commits, err := repo.IterCommitsSince(context.Background(), nil)
	if err != nil || len(commits) != 2 {
		t.Fatalf("commits = %d, %v", len(commits), err)
	}
	stats := map[string]CommitStat{}
	for _, row := range repo.CommitStats(context.Background(), commits[0]) {
		stats[row.FilePath] = row
	}
	if got := stats["a.txt"]; got.Additions != 1 || got.Deletions != 0 || got.OldFileMode != "33188" || got.NewFileMode != "33188" {
		t.Errorf("a.txt = %+v", got)
	}
	if got := stats["bin.dat"]; got.Additions != 0 || got.Deletions != 0 {
		t.Errorf("a binary file counts 0/0, got %+v", got)
	}
	// The root commit is diffed against the WORKING TREE: its rows are the files
	// that differ from it now (keep.txt does not), and the counts are the ones
	// `git diff-tree --root` gives for the root commit itself.
	root := map[string]CommitStat{}
	for _, row := range repo.CommitStats(context.Background(), commits[1]) {
		root[row.FilePath] = row
	}
	if len(root) != 2 || root["a.txt"].Additions != 2 || root["bin.dat"].Additions != 0 {
		t.Errorf("root rows = %v", root)
	}
	if _, ok := root["keep.txt"]; ok {
		t.Errorf("an unchanged file must not show against the root commit, got %v", root)
	}
}

func TestIterCommitsSinceStopsAtTheFirstOldCommit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git binary")
	}
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	commit := func(date string, name string) {
		if err := os.WriteFile(filepath.Join(dir, "f"), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
		gitIn(t, dir, "add", "-A")
		command := exec.Command("git", "commit", "-q", "-m", name)
		command.Dir = dir
		command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=T", "GIT_AUTHOR_EMAIL=t@e", "GIT_COMMITTER_NAME=T", "GIT_COMMITTER_EMAIL=t@e", "GIT_AUTHOR_DATE="+date, "GIT_COMMITTER_DATE="+date)
		if out, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
	}
	commit("1600000000 +0000", "old")
	commit("1700000000 +0000", "new")
	commit("1600000100 +0000", "skewed")
	commit("1700100000 +0000", "newest")
	repo, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	since := time.Unix(1_650_000_000, 0).UTC()
	commits, err := repo.IterCommitsSince(context.Background(), &since)
	if err != nil {
		t.Fatal(err)
	}
	// Newest first: the walk stops at "skewed", which hides "new" behind it.
	if len(commits) != 1 || strings.TrimSpace(commits[0].Message) != "newest" {
		t.Fatalf("commits = %d, first %q", len(commits), commits[0].Message)
	}
}

func TestOpenRefusesAFolderWithoutGit(t *testing.T) {
	if _, err := Open(t.TempDir()); err == nil || !strings.Contains(err.Error(), "no git repository") {
		t.Fatalf("err = %v", err)
	}
}
