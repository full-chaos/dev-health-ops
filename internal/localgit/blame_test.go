package localgit

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestIsSkippable(t *testing.T) {
	for path, want := range map[string]bool{
		"a.png": true, "a.PNG": true, "x.tar.gz": true, "noext": false, ".gitignore": false, "trailing.": false,
		"dots..txt": false, "vendor/lib.go": true, "src/build/out.txt": true, "bin/tool": true, "a/./b.jpeg": true,
		"UPPER.SVG": true, "node_modules": true, "src/main.go": false, "png": false, "a.b/c": false,
	} {
		if got := isSkippable(path); got != want {
			t.Errorf("isSkippable(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestDecodeUTF8Ignore(t *testing.T) {
	for in, want := range map[string]string{
		"plain": "plain", "bad \xff\xfe bytes": "bad  bytes", "trunc \xe2\x82 x": "trunc  x", "ok é": "ok é", "\xef\xbf\xbd": "�", "lone \xc3": "lone ",
	} {
		if got := decodeUTF8Ignore([]byte(in)); got != want {
			t.Errorf("decodeUTF8Ignore(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSplitWhitespaceOnceAndLines(t *testing.T) {
	for in, want := range map[string][]string{
		"author Tom Preston": {"author", "Tom Preston"},
		"\tcode line":        {"", "code line"},
		"single":             {"single"},
		"a \t b  c":          {"a", "b  c"},
		"x\u0085y":           {"x", "y"},
		"":                   {""},
		"sha 1 2 3":          {"sha", "1 2 3"},
	} {
		if got := splitWhitespaceOnce(in); !reflect.DeepEqual(got, want) {
			t.Errorf("splitWhitespaceOnce(%q) = %q, want %q", in, got, want)
		}
	}
	got := bytesSplitLines([]byte("a\nb\r\nc\rd"))
	if len(got) != 4 || string(got[1]) != "b\r\n" || string(got[2]) != "c\r" || string(got[3]) != "d" {
		t.Errorf("bytesSplitLines = %q", got)
	}
}

const sha1 = "0123456789abcdef0123456789abcdef01234567"
const sha2 = "fedcba9876543210fedcba9876543210fedcba98"

func header(sha, author, email string, when int) string {
	return "author " + author + "\nauthor-mail <" + email + ">\nauthor-time " + itoa(when) + "\nauthor-tz +0000\n" +
		"committer " + author + "\ncommitter-mail <" + email + ">\ncommitter-time " + itoa(when) + "\ncommitter-tz +0000\nsummary s\nfilename f.txt\n"
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestParseBlame(t *testing.T) {
	porcelain := sha1 + " 1 1 2\n" + header(sha1, "Alice", "a@e", 1700000000) + "\tfirst  \n" +
		sha1 + " 2 2\n\tsecond\n" +
		sha2 + " 3 3 1\n" + header(sha2, "Bob", "b@e", 1700000005) + "\t\n" +
		sha1 + " 4 4 1\n\tagain\n"
	groups, ok := parseBlame([]byte(porcelain))
	if !ok || len(groups) != 3 {
		t.Fatalf("groups = %d, ok = %v", len(groups), ok)
	}
	if got := groups[0].lines; !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Errorf("group 0 lines = %q (trailing whitespace of a content line is stripped)", got)
	}
	if got := groups[1].lines; !reflect.DeepEqual(got, []string{""}) || groups[1].commit.author != "Bob" {
		t.Errorf("group 1 = %+v", groups[1])
	}
	if groups[2].commit == nil || groups[2].commit.author != "Alice" || groups[2].hash != sha1 {
		t.Errorf("a repeated commit reuses its header: %+v", groups[2])
	}
	// A header that is missing raises in Python: no rows.
	if _, ok := parseBlame([]byte(sha1 + " 1 1 1\nauthor A\n\tline\n")); ok {
		t.Error("a missing author-mail must fail the whole blame")
	}
	if _, ok := parseBlame([]byte(sha1 + " 1 1 1\n" + strings.Replace(header(sha1, "A", "a@e", 1), "author-time 1", "author-time x", 1) + "\tl\n")); ok {
		t.Error("a non-numeric time must fail the whole blame")
	}
	if _, ok := parseBlame([]byte(sha1 + " 1 1\n")); ok {
		t.Error("a group continuation with no group must fail")
	}
}

func TestReadFileContentsLikePythonsTextMode(t *testing.T) {
	dir := t.TempDir()
	repo := Repo{Root: dir}
	write := func(name, content string, mode os.FileMode) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	crlf := repo.ReadFile(write("crlf.txt", "a\r\nb\rc\n", 0o644))
	if crlf.Contents == nil || *crlf.Contents != "a\nb\nc\n" || crlf.Path != "crlf.txt" {
		t.Errorf("crlf = %+v", crlf)
	}
	if bad := repo.ReadFile(write("bad.txt", "x\xffy", 0o644)); bad.Contents == nil || *bad.Contents != "xy" {
		t.Errorf("invalid bytes are dropped: %+v", bad)
	}
	if small := repo.ReadFile(write("small.txt", strings.Repeat("s", 999_999), 0o644)); small.Contents == nil {
		t.Error("999999 bytes is read")
	}
	if big := repo.ReadFile(write("big.txt", strings.Repeat("b", 1_000_000), 0o644)); big.Contents != nil {
		t.Error("1000000 bytes is not read")
	}
	if exe := repo.ReadFile(write("run.sh", "#!/bin/sh\n", 0o755)); !exe.Executable {
		t.Error("an executable file")
	}
	if plain := repo.ReadFile(write("plain.txt", "x", 0o600)); plain.Executable {
		t.Error("a plain file")
	}
}

func TestAllFilesFollowsOsWalk(t *testing.T) {
	dir := t.TempDir()
	repo := Repo{Root: dir}
	for name, content := range map[string]string{
		"a.txt": "a", "sub/b.txt": "b", ".git/config": "c", "sub/.git/config": "d", "gitfile/.git": "gitdir: x", "node_modules/x.js": "x",
	} {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{"link-file": "a.txt", "link-dir": "sub", "broken": "nowhere"} {
		if err := os.Symlink(target, filepath.Join(dir, link)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	repo.Root = root
	var rel []string
	for _, path := range repo.AllFiles() {
		r, _ := filepath.Rel(root, path)
		rel = append(rel, r)
	}
	sort.Strings(rel)
	// .git directories' files are skipped (the top .git and sub/.git), a `.git`
	// FILE is kept, a symlink to a directory is neither listed nor entered,
	// a file symlink lists its target, a broken one its (missing) target.
	want := []string{"a.txt", "a.txt", "gitfile/.git", "node_modules/x.js", "nowhere", "sub/b.txt"}
	if !reflect.DeepEqual(rel, want) {
		t.Errorf("files = %q, want %q", rel, want)
	}
}

func TestBlameOfARealRepository(t *testing.T) {
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a.txt", "one\ntwo  \n\tthree\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "first")
	repo, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	rows := repo.Blame(context.Background(), "a.txt")
	if len(rows) != 3 || rows[1].Line != "two" || rows[2].Line != "\tthree" || rows[0].LineNo != 1 || rows[2].LineNo != 3 {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0].AuthorName == nil || *rows[0].AuthorName != "T" || rows[0].AuthorWhen.Unix() != 1700000000 {
		t.Errorf("author = %+v", rows[0])
	}
	if got := repo.Blame(context.Background(), "untracked.txt"); len(got) != 0 {
		t.Errorf("a file git does not know has no blame, got %d rows", len(got))
	}
}

func TestBlameRowsSkipAGroupWithNoCommitWithoutAdvancingTheLineNumber(t *testing.T) {
	commit := &blameCommit{author: "A", authorEmail: "<a@e>", committerDate: 1700000000}
	rows := blameRows([]blameGroup{
		{hash: sha1, lines: []string{"never a row"}},
		{commit: commit, hash: sha2, lines: []string{"x\n\n", "y"}},
	}, "f.txt")
	if len(rows) != 2 || rows[0].LineNo != 1 || rows[1].LineNo != 2 || rows[0].Line != "x" || rows[1].Line != "y" || rows[0].CommitHash != sha2 {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0].AuthorEmail == nil || *rows[0].AuthorEmail != "a@e" || *rows[0].AuthorName != "A" {
		t.Errorf("author = %+v", rows[0])
	}
}
