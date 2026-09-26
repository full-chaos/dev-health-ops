//go:build integration

package synccli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "embed"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/localgit"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

//go:embed testdata/local_sync_oracle.py
var localSyncOracleProgram string

// The tables `sync git|prs|blame --provider local` writes. Compared columns come from
// the real table schema (system.columns), never a hand list; the only ones left
// out are last_synced and, for repos, created_at (both are the sync time and
// differ between the two runs): for repos, created_at = last_synced is compared
// instead, and every last_synced must be inside the run's window.
var localTables = []string{"repos", "git_commits", "git_commit_stats", "git_pull_requests", "git_files", "git_blame"}

// fixture builds one repository with fixed dates, so both planes read the same
// bytes.
type fixture struct {
	t     *testing.T
	dir   string
	clock int64
}

func newFixture(t *testing.T, name string) *fixture {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, dir: dir, clock: 1_700_000_000}
	f.git("init", "-q", "-b", "main")
	f.git("config", "user.name", "Fixture")
	f.git("config", "user.email", "fixture@example.com")
	f.git("config", "commit.gpgsign", "false")
	return f
}

func (f *fixture) run(env []string, stdin string, args ...string) string {
	f.t.Helper()
	command := exec.Command("git", args...)
	command.Dir = f.dir
	command.Env = append(os.Environ(), env...)
	if stdin != "" {
		command.Stdin = strings.NewReader(stdin)
	}
	out, err := command.CombinedOutput()
	if err != nil {
		f.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func (f *fixture) git(args ...string) string { return f.run(nil, "", args...) }

func (f *fixture) write(path, content string, mode os.FileMode) {
	f.t.Helper()
	full := filepath.Join(f.dir, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), mode); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chmod(full, mode); err != nil {
		f.t.Fatal(err)
	}
}

type commitAt struct {
	author         string // "Name <email>"; empty = the fixture identity
	authorDate     int64
	committerDate  int64
	tz             string
	allowEmpty     bool
	emptyMessage   bool
	noEditMessage  bool
	committerNamed string
	noAdd          bool // commit the index as it is
}

// commit stages everything and commits; every date is fixed.
func (f *fixture) commit(message string, opt ...commitAt) string {
	f.t.Helper()
	var o commitAt
	if len(opt) > 0 {
		o = opt[0]
	}
	f.clock += 3600
	authorDate, committerDate := o.authorDate, o.committerDate
	if authorDate == 0 {
		authorDate = f.clock
	}
	if committerDate == 0 {
		committerDate = f.clock
	}
	tz := o.tz
	if tz == "" {
		tz = "+0000"
	}
	env := []string{
		fmt.Sprintf("GIT_AUTHOR_DATE=%d %s", authorDate, tz),
		fmt.Sprintf("GIT_COMMITTER_DATE=%d %s", committerDate, tz),
	}
	if o.author != "" {
		name, email, _ := strings.Cut(o.author, " <")
		env = append(env, "GIT_AUTHOR_NAME="+name, "GIT_AUTHOR_EMAIL="+strings.TrimSuffix(email, ">"))
	}
	if o.committerNamed != "" {
		env = append(env, "GIT_COMMITTER_NAME="+o.committerNamed, "GIT_COMMITTER_EMAIL=committer@example.com")
	}
	if !o.noAdd {
		f.run(nil, "", "add", "-A")
	}
	args := []string{"commit", "-q", "--cleanup=verbatim", "-F", "-"}
	if o.allowEmpty {
		args = append(args, "--allow-empty")
	}
	if o.emptyMessage {
		args = []string{"commit", "-q", "--allow-empty-message", "--cleanup=verbatim", "-m", ""}
		if o.allowEmpty {
			args = append(args, "--allow-empty")
		}
		f.run(env, "", args...)
	} else {
		f.run(env, message, args...)
	}
	return f.git("rev-parse", "HEAD")
}

// merge merges branch with --no-ff and the message.
func (f *fixture) merge(branch, message string) string {
	f.t.Helper()
	f.clock += 3600
	env := []string{fmt.Sprintf("GIT_AUTHOR_DATE=%d +0000", f.clock), fmt.Sprintf("GIT_COMMITTER_DATE=%d +0000", f.clock)}
	f.run(env, "", "merge", "--no-ff", "-q", "-m", message, branch)
	return f.git("rev-parse", "HEAD")
}

// setRef writes a loose ref file directly, so a name or target git's own
// commands would refuse can still be put in the repository.
func (f *fixture) setRef(name, target string) {
	f.t.Helper()
	path := filepath.Join(f.dir, ".git", filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(target+"\n"), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

// object writes a git object of any type byte for byte (no validation).
func (f *fixture) object(kind, content string) string {
	f.t.Helper()
	return f.run(nil, content, "hash-object", "-t", kind, "-w", "--literally", "--stdin")
}

// tagObject writes an annotated tag object naming target.
func (f *fixture) tagObject(target, targetKind, name string) string {
	return f.object("tag", "object "+target+"\ntype "+targetKind+"\ntag "+name+"\ntagger T <t@e> 1700000000 +0000\n\nmessage\n")
}

// rawCommit writes a commit object byte for byte (header lines, message) so a
// header shape git itself would not write can be read, and points main at it
// by writing the ref file: `git update-ref` refuses a commit git cannot parse.
func (f *fixture) rawCommit(parents []string, headers []string, message []byte, author, committer string) string {
	f.t.Helper()
	tree := f.git("write-tree")
	var body strings.Builder
	body.WriteString("tree " + tree + "\n")
	for _, parent := range parents {
		body.WriteString("parent " + parent + "\n")
	}
	body.WriteString("author " + author + "\n")
	body.WriteString("committer " + committer + "\n")
	for _, header := range headers {
		body.WriteString(header + "\n")
	}
	body.WriteString("\n")
	body.Write(message)
	hash := f.object("commit", body.String())
	f.setRef("refs/heads/main", hash)
	return hash
}

type localScenario struct {
	name    string
	build   func(f *fixture)
	args    []string // after `--provider local --repo-path <dir> --analytics-db <dsn>`
	env     map[string]string
	targets []string // default git and prs
	noOrg   bool
	// pathForm is how --repo-path names the repository: "" as created, "symlink",
	// "relative" (to the working directory) or "dots" (a trailing /./).
	pathForm string
	// envFn adds variables that depend on the fixtures (paths of other repositories);
	// every variable reaches both planes: the Python child and this process.
	envFn func(f *fixture) map[string]string
	// goRefusesOn names the target ("git" or "prs") on which a named divergence holds:
	// Python succeeds and the port refuses (exit 1, "outside the range").
	goRefusesOn string
}

func localScenarios() []localScenario {
	basic := func(f *fixture) {
		f.write("a.txt", "one\ntwo\nthree\n", 0o644)
		f.write("tool.sh", "#!/bin/sh\necho hi\n", 0o755)
		f.write("dir/c.txt", "c1\nc2\n", 0o644)
		f.write("bin.dat", "\x00\x01\x02binary\x00", 0o644)
		f.commit("root commit\n")
		f.write("a.txt", "one\nTWO\nthree\nfour\n", 0o644)
		f.write("my file.txt", "spaces in the name\n", 0o644)
		f.git("rm", "-q", "tool.sh")
		f.commit("second: modify, add, delete\n\nbody line\n")
		f.git("mv", "dir/c.txt", "dir/d.txt")
		f.commit("third: pure rename\n")
		f.write("dir/d.txt", "c1\nc2\nc3 changed\n", 0o644)
		f.git("mv", "dir/d.txt", "dir/e.txt")
		f.write("bin.dat", "\x00\x01\x02binary changed\x00", 0o644)
		f.commit("fourth: rename with edit and a binary change\n")
		f.write("a.txt", "one\nTWO\nthree\nfour\n", 0o755)
		f.commit("fifth: mode change only\n")
	}
	scenarios := []localScenario{
		{name: "basic history", build: basic},
		{name: "basic history, no org", build: basic, noOrg: true},
		{name: "unicode and odd paths", build: func(f *fixture) {
			f.write("é.txt", "accent\n", 0o644)
			f.write("日本語/ファイル.txt", "cjk\n", 0o644)
			f.write("tab\tname.txt", "tab\n", 0o644)
			f.write("quote\"name.txt", "quote\n", 0o644)
			f.commit("unicode paths\n")
			f.write("é.txt", "accent changed\nmore\n", 0o644)
			f.commit("änderung: ünïcode message ✓\n")
		}},
		{name: "typechange, empty file, symlink", build: func(f *fixture) {
			f.write("plain", "text\n", 0o644)
			f.write("empty.txt", "", 0o644)
			f.commit("files\n")
			if err := os.Remove(filepath.Join(f.dir, "plain")); err != nil {
				f.t.Fatal(err)
			}
			if err := os.Symlink("empty.txt", filepath.Join(f.dir, "plain")); err != nil {
				f.t.Fatal(err)
			}
			f.commit("file becomes a symlink\n")
		}},
		{name: "merge commits and PR inference", build: func(f *fixture) {
			f.write("base.txt", "base\n", 0o644)
			f.commit("base\n")
			for i, msg := range []string{
				"Merge pull request #12 from acme/feature-a\n\nAdd the first thing\n",
				"Merge pull request #34 from acme/feature-b\n\n  \n  Second thing with leading blanks\n",
				"Merge branch 'fix' into 'main'\n\nA GitLab merge\n\nSee merge request group/project!7\n",
				"Merge branch 'plain' into main\n",
				"Merge pull request #12 from acme/feature-a-again\n\nDuplicate number, newer\n",
				"Merge pull request #９９ from acme/fullwidth\n\nFullwidth digits\n",
				"Merge pull request #56abc from acme/glued\n\nNot a boundary\n",
				"Merge pull request #78é from acme/glued2\n\nUnicode word char after the digits\n",
				"See merge request group/project!41 and later group/other!42\n",
				"Seeing merge requests !5\n",
				"Title first\n\nMerge pull request #61 from acme/not-at-start\n",
				"prefix Merge pull request #62 from acme/mid-line\n",
			} {
				branch := fmt.Sprintf("topic-%d", i)
				f.git("checkout", "-q", "-b", branch, "main")
				f.write(fmt.Sprintf("topic%d.txt", i), fmt.Sprintf("topic %d\n", i), 0o644)
				f.commit(fmt.Sprintf("work on topic %d\n", i))
				f.git("checkout", "-q", "main")
				f.merge(branch, msg)
			}
		}},
		{name: "open PR refs, loose and packed", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			first := f.commit("first\n")
			f.write("b.txt", "b\n", 0o644)
			second := f.commit("second\n", commitAt{author: "Other Person <other@example.com>", tz: "+0530"})
			f.git("update-ref", "refs/pull/5/head", first)
			f.git("update-ref", "refs/pull/6/head", second)
			f.git("update-ref", "refs/merge-requests/9/head", second)
			f.git("update-ref", "refs/pull/6x/head", first) // not a PR ref
			f.git("update-ref", "refs/tags/v1", first)
			f.git("pack-refs", "--all")
			f.git("update-ref", "refs/pull/7/head", first) // loose after the pack
		}},
		{name: "open and merged with the same number", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.commit("first\n")
			f.git("checkout", "-q", "-b", "t")
			f.write("t.txt", "t\n", 0o644)
			topic := f.commit("topic\n")
			f.git("checkout", "-q", "main")
			f.merge("t", "Merge pull request #5 from acme/t\n")
			f.git("update-ref", "refs/pull/5/head", topic)
		}},
		{name: "since in the middle", args: []string{"--since", "2023-11-15"}, build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			for i := 0; i < 6; i++ {
				f.write("a.txt", fmt.Sprintf("a %d\n", i), 0o644)
				f.commit(fmt.Sprintf("commit %d\n", i), commitAt{committerDate: 1_699_000_000 + int64(i)*86400})
			}
		}},
		{name: "since after every commit", args: []string{"--since", "2030-01-01"}, build: basic},
		{name: "since before every commit", args: []string{"--since", "2001-01-01"}, build: basic},
		{name: "a skewed committer date ends the walk", args: []string{"--since", "2023-11-14"}, build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.commit("old root\n", commitAt{committerDate: 1_600_000_000})
			f.write("a.txt", "b\n", 0o644)
			f.commit("new\n", commitAt{committerDate: 1_700_000_000})
			f.write("a.txt", "c\n", 0o644)
			f.commit("skewed older than its parent\n", commitAt{committerDate: 1_600_000_100})
			f.write("a.txt", "d\n", 0o644)
			f.commit("newest\n", commitAt{committerDate: 1_700_100_000})
		}},
		{name: "author and committer differ, time zones", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.commit("one\n", commitAt{author: "Jane Doe <jane@example.com>", committerNamed: "Committer Person", authorDate: 1_600_000_000, committerDate: 1_700_000_000, tz: "-0700"})
			f.write("a.txt", "b\n", 0o644)
			f.commit("two\n", commitAt{author: "José Ñandú <jose@example.com>", tz: "+0545"})
		}},
		{name: "message shapes", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.commit("no trailing newline")
			f.write("a.txt", "b\n", 0o644)
			f.commit("crlf line\r\nsecond line\r\n\r\nthird\r\n")
			f.write("a.txt", "c\n", 0o644)
			f.commit("\n\n  leading blank lines\n")
			f.write("a.txt", "d\n", 0o644)
			f.commit("", commitAt{emptyMessage: true})
			f.write("a.txt", "e\n", 0o644)
			f.commit("trailing spaces   \n\n\n\n")
			f.write("a.txt", "f\n", 0o644)
			f.commit(strings.Repeat("long line ", 500) + "\n")
		}},
		{name: "empty commit", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.commit("root\n")
			f.commit("empty second\n", commitAt{allowEmpty: true})
		}},
		{name: "dirty working tree against a root commit", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.write("b.txt", "b\n", 0o644)
			f.write("c.txt", "c\n", 0o644)
			f.commit("root\n")
			f.write("a.txt", "a changed in the working tree\n", 0o644)
			f.write("untracked.txt", "untracked\n", 0o644)
			if err := os.Remove(filepath.Join(f.dir, "b.txt")); err != nil {
				f.t.Fatal(err)
			}
			f.write("c.txt", "c staged\n", 0o644)
			f.git("add", "c.txt")
		}},
		{name: "root commit, clean tree", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.commit("root only\n")
		}},
		{name: "remote origin url", build: func(f *fixture) {
			basic(f)
			f.git("remote", "add", "origin", "https://example.com/acme/widgets.git")
		}},
		{name: "two remotes, no origin: the first in the config", build: func(f *fixture) {
			basic(f)
			f.git("remote", "add", "zeta", "https://example.com/zeta.git")
			f.git("remote", "add", "alpha", "https://example.com/alpha.git")
		}},
		{name: "origin among others", build: func(f *fixture) {
			basic(f)
			f.git("remote", "add", "upstream", "https://example.com/up.git")
			f.git("remote", "add", "origin", "https://example.com/origin.git")
		}},
		{name: "remote with several urls", build: func(f *fixture) {
			basic(f)
			f.git("remote", "add", "origin", "https://example.com/first.git")
			f.git("remote", "set-url", "--add", "origin", "https://example.com/second.git")
		}},
		{name: "remote without a url", build: func(f *fixture) {
			basic(f)
			f.git("config", "remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*")
		}},
		{name: "insteadOf rewrites the url", build: func(f *fixture) {
			basic(f)
			f.git("remote", "add", "origin", "gh:acme/widgets.git")
			f.git("config", "url.https://github.com/.insteadOf", "gh:")
		}},
		{name: "path through a symlink", build: basic, pathForm: "symlink"},
		{name: "path relative to the working directory", build: basic, pathForm: "relative"},
		{name: "path with dots and a trailing slash", build: basic, pathForm: "dots"},
		{name: "REPO_UUID", build: basic, env: map[string]string{"REPO_UUID": "12345678-1234-5678-1234-567812345678"}},
		{name: "REPO_UUID in a shape uuid.UUID accepts", build: basic, env: map[string]string{"REPO_UUID": "{12345678123456781234567812345678}"}},
		{name: "REPO_UUID invalid", build: basic, env: map[string]string{"REPO_UUID": "not-a-uuid"}},
		{name: "REPO_UUID empty is unset", build: basic, env: map[string]string{"REPO_UUID": ""}},
		{name: "no .git directory", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			if err := os.RemoveAll(filepath.Join(f.dir, ".git")); err != nil {
				f.t.Fatal(err)
			}
		}},
		{name: "unborn HEAD", build: func(f *fixture) { f.write("a.txt", "a\n", 0o644) }},
		{name: "crafted: author without an email, empty name, odd headers", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.git("add", "-A")
			root := f.rawCommit(nil, nil, []byte("root\n"), "Just A Name 1700000000 +0000", "Committer <c@example.com> 1700000000 +0000")
			f.write("a.txt", "b\n", 0o644)
			f.git("add", "-A")
			second := f.rawCommit([]string{root}, nil, []byte("empty name\n"), "<only@example.com> 1700003600 +0000", "  Spaced Name   <c@example.com> 1700003600 +0100")
			f.write("a.txt", "c\n", 0o644)
			f.git("add", "-A")
			f.rawCommit([]string{second}, []string{"gpgsig -----BEGIN PGP SIGNATURE-----", " ", " abcdef", " -----END PGP SIGNATURE-----"},
				[]byte("signed\n"), "Author <a@example.com> 1700007200 +0000", "Committer <c@example.com> 1700007200 +0000")
		}},
		{name: "crafted: latin-1 encoding header", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.git("add", "-A")
			f.rawCommit(nil, []string{"encoding ISO-8859-1"}, []byte("caf\xe9 message\n"), "Andr\xe9 <a@example.com> 1700000000 +0000", "Andr\xe9 <a@example.com> 1700000000 +0000")
		}},
		{name: "crafted: invalid utf-8 message", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.git("add", "-A")
			f.rawCommit(nil, nil, []byte("bad \xff\xfe bytes\n"), "Author <a@example.com> 1700000000 +0000", "Committer <c@example.com> 1700000000 +0000")
		}},
		{name: "crafted: mergetag header", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.git("add", "-A")
			root := f.rawCommit(nil, nil, []byte("root\n"), "A <a@example.com> 1700000000 +0000", "C <c@example.com> 1700000000 +0000")
			f.write("a.txt", "b\n", 0o644)
			f.git("add", "-A")
			f.rawCommit([]string{root}, []string{"mergetag object " + root, " type commit", " tag v1", " tagger T <t@example.com> 1700000000 +0000", " ", " tag message"},
				[]byte("with a mergetag\n"), "A <a@example.com> 1700003600 +0000", "C <c@example.com> 1700003600 +0000")
		}},
		{name: "many files in one commit", build: func(f *fixture) {
			f.write("seed.txt", "seed\n", 0o644)
			f.commit("seed\n")
			for i := 0; i < 60; i++ {
				f.write(fmt.Sprintf("many/f%02d.txt", i), strings.Repeat(fmt.Sprintf("line %d\n", i), i+1), 0o644)
			}
			f.commit("many files\n")
		}},
		{name: "tags: nested, unicode, quoted, annotated, packed and loose", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			first := f.commit("first\n")
			f.git("tag", "release/v1.0", first)
			f.git("tag", "é-tag", first)
			f.git("tag", "with\"quote", first)
			f.git("tag", "-a", "annotated", "-m", "annotated tag", first)
			f.git("pack-refs", "--all")
			f.write("b.txt", "b\n", 0o644)
			f.commit("second\n")
			f.git("tag", "loose-after-pack")
			f.git("tag", "Zebra")
			f.git("tag", "alpha")
		}},
		{name: "gitlink (submodule) entry", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			first := f.commit("first\n")
			f.git("update-index", "--add", "--cacheinfo", "160000,"+first+",sub")
			f.commit("adds a gitlink\n", commitAt{noAdd: true})
			f.git("update-index", "--force-remove", "sub")
			f.commit("removes the gitlink\n", commitAt{noAdd: true})
		}},
		{name: "newline and backslash in a path", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.write("new\nline.txt", "n\n", 0o644)
			f.write("back\\slash.txt", "b\n", 0o644)
			f.write("-dash.txt", "d\n", 0o644)
			f.commit("odd names\n")
			f.write("new\nline.txt", "n2\n", 0o644)
			f.commit("touch the newline path\n")
		}},
		{name: "path that is not valid utf-8", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			if err := os.WriteFile(filepath.Join(f.dir, "bad\xff\xfename.txt"), []byte("x\n"), 0o644); err != nil {
				f.t.Fatal(err)
			}
			f.commit("invalid utf-8 path\n")
			if err := os.WriteFile(filepath.Join(f.dir, "bad\xff\xfename.txt"), []byte("y\n"), 0o644); err != nil {
				f.t.Fatal(err)
			}
			f.write("b.txt", "b\n", 0o644)
			f.commit("touch it\n")
		}},
		{name: "more than one insert batch (1100 commits)", build: func(f *fixture) {
			var stream strings.Builder
			for i := 0; i < 1100; i++ {
				stream.WriteString(fmt.Sprintf("commit refs/heads/main\nmark :%d\ncommitter Fixture <fixture@example.com> %d +0000\ndata 12\ncommit %05d\n", i+1, 1_700_000_000+i*60, i))
				if i > 0 {
					stream.WriteString(fmt.Sprintf("from :%d\n", i))
				}
				body := fmt.Sprintf("file %d\n", i)
				stream.WriteString(fmt.Sprintf("M 100644 inline f%d.txt\ndata %d\n%s\n", i%7, len(body), body))
			}
			f.run(nil, stream.String(), "fast-import", "--quiet")
			f.git("reset", "-q", "--hard", "HEAD")
		}},
		{name: "blame: line endings, whitespace, tabs, empty and no-newline files", build: func(f *fixture) {
			f.write("crlf.txt", "one\r\ntwo\r\nthree\r\n", 0o644)
			f.write("cr.txt", "a\rb\rc", 0o644)
			f.write("trailing.txt", "keep  \n\tindented\t\n   \n\n", 0o644)
			f.write("empty.txt", "", 0o644)
			f.write("nonewline.txt", "no newline at the end", 0o644)
			f.write("unicode.txt", "héllo ✓\n日本語\n\u2028sep\n\x1cfs\n", 0o644)
			f.write("form\ffeed.txt", "a\fb\n\x0bvt\n", 0o644)
			f.commit("line shapes\n")
		}},
		{name: "blame: header-looking content and carriage returns inside lines", build: func(f *fixture) {
			f.write("tricky.txt", "author Mallory\nfilename evil\nsummary evil\n0123456789012345678901234567890123456789 1 1 1\nx\rauthor evil\rauthor-mail <e@e>\rauthor-time 5\n", 0o644)
			f.commit("header lookalikes\n")
			f.write("tricky.txt", "author Mallory\nfilename evil2\nsummary evil\n0123456789012345678901234567890123456789 1 1 1\ny\r\rz\n", 0o644)
			f.commit("second\n")
		}},
		{name: "blame: invalid UTF-8, NUL, binary and large files", build: func(f *fixture) {
			f.write("invalid.txt", "ok line\nbad \xff\xfe bytes\nmore \xe2\x82 trunc\n", 0o644)
			f.write("nul.dat", "a\x00b\nc\x00\n", 0o644)
			f.write("big-999999.txt", strings.Repeat("x", 999_998)+"\n", 0o644)
			f.write("big-1000000.txt", strings.Repeat("y", 999_999)+"\n", 0o644)
			f.write("big-1000001.txt", strings.Repeat("z", 1_000_000)+"\n", 0o644)
			f.commit("odd contents\n")
		}},
		{name: "blame: interleaved authors and repeated commit groups", build: func(f *fixture) {
			f.write("a.txt", "l1\nl2\nl3\nl4\nl5\nl6\n", 0o644)
			f.commit("first\n", commitAt{author: "Alice <alice@example.com>", tz: "-0800"})
			f.write("a.txt", "l1\nL2 bob\nl3\nl4\nL5 bob\nl6\n", 0o644)
			f.commit("second\n", commitAt{author: "Bob B <bob@example.com>", tz: "+0530"})
			f.write("a.txt", "l1\nL2 bob\nl3\nL4 carol\nL5 bob\nl6\n", 0o644)
			f.commit("third\n", commitAt{author: "Carol <>", committerNamed: "Somebody Else"})
			f.write("b.txt", "only\n", 0o755)
			f.commit("fourth\n", commitAt{author: "No Email"})
		}},
		{name: "blame: --since selects the changed files only", args: []string{"--since", "2023-11-01"}, build: func(f *fixture) {
			f.write("old.txt", "old\n", 0o644)
			f.write("also-old.txt", "old too\n", 0o644)
			f.write(".png", "dotfile named like an extension\n", 0o644)
			f.write("shot.PNG", "an image\n", 0o644)
			f.write("notes.txt", "notes\n", 0o644)
			f.commit("old\n", commitAt{committerDate: 1_690_000_000})
			f.write("new.txt", "new\n", 0o644)
			f.write("old.txt", "old changed\n", 0o644)
			f.write(".png", "dotfile changed\n", 0o644)
			f.write("shot.PNG", "image changed\n", 0o644)
			f.commit("recent\n", commitAt{committerDate: 1_700_000_000})
		}},
		{name: "blame: changed files skippable or deleted fall back to all files", args: []string{"--since", "2023-11-01"}, build: func(f *fixture) {
			f.write("keep.txt", "keep\n", 0o644)
			f.write("image.png", "png\n", 0o644)
			f.write("gone.txt", "gone\n", 0o644)
			f.commit("base\n", commitAt{committerDate: 1_690_000_000})
			f.write("image.png", "png changed\n", 0o644)
			f.write("node_modules/dep/index.js", "dep\n", 0o644)
			f.git("rm", "-q", "gone.txt")
			f.commit("recent: skippable and deleted only\n", commitAt{committerDate: 1_700_000_000})
		}},
		{name: "blame: working tree differs from HEAD", build: func(f *fixture) {
			f.write("a.txt", "committed 1\ncommitted 2\n", 0o644)
			f.commit("root\n")
			f.write("a.txt", "committed 1\nedited in tree\nextra\n", 0o644)
			f.write("untracked.txt", "untracked\n", 0o644)
			f.write("staged.txt", "staged\n", 0o644)
			f.git("add", "staged.txt")
		}},
		{name: "blame: symlinks (file, directory, broken, outside) and dotfiles", build: func(f *fixture) {
			f.write("real.txt", "real\n", 0o644)
			f.write(".hidden", "hidden\n", 0o644)
			f.write("dir/inner.txt", "inner\n", 0o644)
			mustSymlink := func(target, link string) {
				if err := os.Symlink(target, filepath.Join(f.dir, link)); err != nil {
					f.t.Fatal(err)
				}
			}
			mustSymlink("real.txt", "link-to-file")
			mustSymlink("dir", "link-to-dir")
			mustSymlink("nowhere.txt", "broken-link")
			outside := filepath.Join(filepath.Dir(f.dir), "outside.txt")
			if err := os.WriteFile(outside, []byte("outside\n"), 0o644); err != nil {
				f.t.Fatal(err)
			}
			mustSymlink(outside, "link-outside")
			f.commit("links\n")
		}},
		{name: "blame: a .git file and a nested .git directory", build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.commit("root\n")
			f.write("nested/.git/config", "[core]\n", 0o644)
			f.write("nested/file.txt", "nested\n", 0o644)
			f.write("gitfile/.git", "gitdir: ../elsewhere\n", 0o644)
		}},
		{name: "blame: skippable extensions and suffix corners", build: func(f *fixture) {
			for _, name := range []string{"a.PNG", "b.Jpeg", "noext", ".gitignore", "trailing.", "dots..txt", "x.tar.gz", "vendor/lib.go", "bin/tool", "src/build/out.txt", "UPPER.SVG"} {
				f.write(name, "content of "+name+"\n", 0o644)
			}
			f.commit("suffix corners\n")
		}},
		{name: "blame: many files", build: func(f *fixture) {
			for i := 0; i < 120; i++ {
				f.write(fmt.Sprintf("d%d/f%03d.txt", i%5, i), strings.Repeat(fmt.Sprintf("line %d\n", i), 1+i%4), 0o644)
			}
			f.commit("many\n")
		}},
		{name: "packed objects", build: func(f *fixture) {
			basic(f)
			f.git("gc", "-q")
		}},
		{name: "detached HEAD", build: func(f *fixture) {
			basic(f)
			f.git("checkout", "-q", "--detach", "HEAD~2")
		}},
	}
	return scenarios
}

// tablesSnapshot reads every compared column of a table as text, typed by the
// real schema, sorted.
func tablesSnapshot(ctx context.Context, t *testing.T, conn driver.Conn, database string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, table := range localTables {
		rows, err := conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = ? AND table = ? ORDER BY position", database, table)
		if err != nil {
			t.Fatal(err)
		}
		var selects, names []string
		for rows.Next() {
			var name, typ string
			if err := rows.Scan(&name, &typ); err != nil {
				t.Fatal(err)
			}
			if name == "last_synced" || (table == "repos" && name == "created_at") {
				continue
			}
			names = append(names, name)
			selects = append(selects, fmt.Sprintf("concat('%s=', ifNull(toString(%s), '<NULL>'))", name, name))
		}
		_ = rows.Close()
		if len(selects) == 0 {
			t.Fatalf("no columns found for %s.%s", database, table)
		}
		if table == "repos" {
			selects = append(selects, "concat('created_at=last_synced:', toString(created_at = last_synced))")
		}
		data, err := conn.Query(ctx, fmt.Sprintf("SELECT concat(%s) FROM %s.%s ORDER BY 1", strings.Join(joinWith(selects, "'\\x1f'"), ", "), database, table))
		if err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		var lines []string
		for data.Next() {
			var line string
			if err := data.Scan(&line); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, line)
		}
		_ = data.Close()
		sort.Strings(lines)
		out[table] = lines
	}
	return out
}

func joinWith(parts []string, sep string) []string {
	var out []string
	for i, part := range parts {
		if i > 0 {
			out = append(out, sep)
		}
		out = append(out, part)
	}
	return out
}

// localOracle is the two planes of the local-sync oracle: a real Python verb in a
// long-lived child, and one ClickHouse container holding the database Python writes
// (migrated by the real chain) and a clone the Go verb writes.
type localOracle struct {
	t                    *testing.T
	ctx                  context.Context
	admin                driver.Conn
	pythonDatabase, goDB string
	httpDSN, goDSN       string
	ask                  func(map[string]any) map[string]any
}

func newLocalOracle(t *testing.T) *localOracle {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	// Both planes read the same repositories with the same git configuration.
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	t.Setenv("GIT_CONFIG_SYSTEM", "/dev/null")
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	nativeURL, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	pythonDatabase := strings.TrimPrefix(nativeURL.Path, "/")
	const goDatabase = "dho_local_go"
	admin, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+goDatabase); err != nil {
		t.Fatal(err)
	}
	for _, table := range localTables {
		if err := admin.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s AS %s.%s", goDatabase, table, pythonDatabase, table)); err != nil {
			t.Fatal(err)
		}
	}
	httpDSN, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	goURL := *nativeURL
	goURL.Path = "/" + goDatabase

	command := exec.Command(python, "-c", localSyncOracleProgram)
	command.Env = append(os.Environ(), "PYTHONHASHSEED=0", "PYTHONPATH="+filepath.Join(root, "src"))
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = command.Wait() })
	reader := bufio.NewReaderSize(stdout, 1<<20)
	ask := func(request map[string]any) map[string]any {
		raw, _ := json.Marshal(request)
		if _, err := io.WriteString(stdin, string(raw)+"\n"); err != nil {
			t.Fatalf("python stdin: %v", err)
		}
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatalf("python answer: %v\n%s", err, stderr.String())
		}
		var answer map[string]any
		if err := json.Unmarshal(line, &answer); err != nil {
			t.Fatalf("python answer %q: %v", line, err)
		}
		return answer
	}
	return &localOracle{t: t, ctx: ctx, admin: admin, pythonDatabase: pythonDatabase, goDB: goDatabase, httpDSN: httpDSN, goDSN: goURL.String(), ask: ask}
}

func (o *localOracle) truncate() {
	o.t.Helper()
	for _, database := range []string{o.pythonDatabase, o.goDB} {
		for _, table := range localTables {
			if err := o.admin.Exec(o.ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s", database, table)); err != nil {
				o.t.Fatal(err)
			}
		}
	}
}

// setProcessEnv puts the scenario's variables in the environment of THIS process
// (the git subprocesses of the Go plane inherit it, as the Python child's do) and
// returns the restore.
func setProcessEnv(env map[string]string) func() {
	type saved struct {
		value string
		set   bool
	}
	before := map[string]saved{}
	for key, value := range env {
		old, set := os.LookupEnv(key)
		before[key] = saved{old, set}
		_ = os.Setenv(key, value)
	}
	return func() {
		for key, was := range before {
			if was.set {
				_ = os.Setenv(key, was.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
}

func TestLocalSyncMatchesLivePython(t *testing.T) {
	oracle := newLocalOracle(t)
	ctx, admin, ask := oracle.ctx, oracle.admin, oracle.ask
	pythonDatabase, goDatabase, httpDSN, goDSN, truncate := oracle.pythonDatabase, oracle.goDB, oracle.httpDSN, oracle.goDSN, oracle.truncate
	compared, mismatches, rowsSeen := 0, 0, 0
	perTable := map[string]int{}
	for _, scenario := range append(localScenarios(), generatedScenarios()...) {
		// DHO_ORACLE_SCENARIOS narrows a run to the scenarios whose name contains it
		// (a debugging aid for kill proofs; a full run leaves it unset).
		if only := os.Getenv("DHO_ORACLE_SCENARIOS"); only != "" && !strings.Contains(scenario.name, only) {
			continue
		}
		f := newFixture(t, strings.NewReplacer(" ", "-", ",", "", ":", "", "'", "").Replace(scenario.name))
		scenario.build(f)
		targets := scenario.targets
		if len(targets) == 0 {
			targets = []string{"git", "prs", "blame"}
		}
		for _, target := range targets {
			truncate()
			repoPath := f.dir
			switch scenario.pathForm {
			case "symlink":
				repoPath = filepath.Join(t.TempDir(), "link-to-repo")
				if err := os.Symlink(f.dir, repoPath); err != nil {
					t.Fatal(err)
				}
			case "relative":
				cwd, err := os.Getwd()
				if err != nil {
					t.Fatal(err)
				}
				if repoPath, err = filepath.Rel(cwd, f.dir); err != nil {
					t.Fatal(err)
				}
			case "dots":
				repoPath = f.dir + "/./"
			}
			base := append([]string{target, "--provider", "local", "--repo-path", repoPath}, scenario.args...)
			if !scenario.noOrg {
				base = append(base, "--org", "oracle-org")
			}
			pyArgs := append(append([]string{}, base...), "--analytics-db", httpDSN)
			goArgs := append(append([]string{}, base[1:]...), "--analytics-db", goDSN)
			env := map[string]string{}
			for key, value := range scenario.env {
				env[key] = value
			}
			if scenario.envFn != nil {
				for key, value := range scenario.envFn(f) {
					env[key] = value
				}
			}
			want := ask(map[string]any{"args": pyArgs, "env": env})

			restoreEnv := setProcessEnv(env)
			code, stdoutText, stderrText := runVerb(t, target, InlineExecutor(InlineDeps{}), goArgs, env)
			restoreEnv()

			// A named divergence: Python writes an instant the ClickHouse client cannot, the
			// port refuses (exit 1, "outside the range"). Whether Python gets that far can
			// depend on the git in use (a git that rejects the commit fails both planes), so
			// the row is asserted only where Python succeeded and compared normally otherwise.
			if scenario.goRefusesOn == target && fmt.Sprint(want["stage"].(map[string]any)["v"]) == "ok" {
				if code == cli.ExitOK || !strings.Contains(stderrText, "outside the range") {
					mismatches++
					t.Errorf("%s / %s: a named divergence must be Python ok and a port refusal, got python %v, go exit %d (%s)", scenario.name, target, want, code, stderrText)
				}
				compared++
				continue
			}
			wantStage := fmt.Sprint(want["stage"].(map[string]any)["v"])
			gotStage := "ok"
			if code != cli.ExitOK {
				gotStage = "failed"
			}
			label := scenario.name + " / " + target
			switch {
			case wantStage == "ok" && gotStage == "ok":
			case wantStage != "ok" && gotStage != "ok":
			default:
				mismatches++
				t.Errorf("%s: python ended %v, go exit %d (%s%s)", label, want, code, stdoutText, stderrText)
			}

			pythonRows := tablesSnapshot(ctx, t, admin, pythonDatabase)
			goRows := tablesSnapshot(ctx, t, admin, goDatabase)
			compared++
			if strings.HasPrefix(scenario.name, "crafted") && target == "git" && false {
				for _, line := range pythonRows["git_commits"] {
					t.Logf("python %s: %s", scenario.name, strings.ReplaceAll(line, "\x1f", " | "))
				}
			}
			for _, table := range localTables {
				rowsSeen += len(pythonRows[table])
				perTable[table] += len(pythonRows[table])
				if !equalLines(pythonRows[table], goRows[table]) {
					mismatches++
					t.Errorf("%s: table %s differs\n%s", label, table, lineDiff(pythonRows[table], goRows[table]))
				}
			}
		}
	}
	t.Logf("%d scenario runs compared, %d rows seen in the Python tables %v, %d mismatches", compared, rowsSeen, perTable, mismatches)
	if os.Getenv("DHO_ORACLE_SCENARIOS") != "" {
		return // a narrowed debugging run: the coverage checks below need the whole corpus
	}
	for _, table := range localTables {
		if perTable[table] == 0 {
			t.Errorf("no scenario wrote a row to %s: the corpus does not reach it", table)
		}
	}
	if rowsSeen == 0 {
		t.Fatal("no scenario wrote a row: the corpus does not reach the sink")
	}
	writeVenueProof(t)
}

func equalLines(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// lineDiff shows the rows only one side has (at most eight each).
func lineDiff(python, goRows []string) string {
	count := func(lines []string) map[string]int {
		m := map[string]int{}
		for _, l := range lines {
			m[l]++
		}
		return m
	}
	py, gr := count(python), count(goRows)
	var out []string
	shown := 0
	for _, l := range python {
		if py[l] > gr[l] && shown < 8 {
			out = append(out, "  only python: "+strings.ReplaceAll(l, "\x1f", " | "))
			py[l]--
			shown++
		}
	}
	shown = 0
	for _, l := range goRows {
		if gr[l] > py[l] && shown < 8 {
			out = append(out, "  only go:     "+strings.ReplaceAll(l, "\x1f", " | "))
			gr[l]--
			shown++
		}
	}
	return fmt.Sprintf("python %d rows, go %d rows\n%s", len(python), len(goRows), strings.Join(out, "\n"))
}

// A RERUN over a git_pull_requests row another writer filled: the stored-version
// contract of the port's writer keeps the columns a local source has no field for
// (R1) where `dev-hops` inserts blindly and CLEARS them. This is a named divergence
// (decided: the invariant wins, D2598); the test pins both sides so it cannot drift.
func TestLocalSyncRerunKeepsHeldColumnsUnlikePython(t *testing.T) {
	oracle := newLocalOracle(t)
	ctx := oracle.ctx
	f := newFixture(t, "rerun")
	richRepository(f, "12")
	repo, err := localgit.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := repo.RepoID(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, database := range []string{oracle.pythonDatabase, oracle.goDB} {
		seed := fmt.Sprintf("INSERT INTO %s.git_pull_requests (repo_id, number, title, body, state, created_at, additions, deletions, changed_files, "+
			"changes_requested_count, reviews_count, comments_count, last_synced, org_id) VALUES (toUUID('%s'), 5, 'held title', 'external body', 'open', "+
			"toDateTime64('2021-01-01 00:00:00', 3), 7, 2, 3, 1, 4, 5, toDateTime64('2020-01-01 00:00:00', 3), 'oracle-org')", database, id)
		if err := oracle.admin.Exec(ctx, seed); err != nil {
			t.Fatal(err)
		}
	}
	base := []string{"--provider", "local", "--repo-path", f.dir, "--org", "oracle-org"}
	if want := oracle.ask(map[string]any{"args": append(append([]string{"prs"}, base...), "--analytics-db", oracle.httpDSN)}); fmt.Sprint(want["stage"].(map[string]any)["v"]) != "ok" {
		t.Fatalf("python did not finish: %v", want)
	}
	if code, out, errText := runVerb(t, "prs", InlineExecutor(InlineDeps{}), append(append([]string{}, base...), "--analytics-db", oracle.goDSN), map[string]string{}); code != cli.ExitOK {
		t.Fatalf("go exit %d: %s%s", code, out, errText)
	}
	read := func(database string) (body *string, additions *uint32, reviews uint32, title *string, state string) {
		query := fmt.Sprintf("SELECT body, additions, reviews_count, title, state FROM %s.git_pull_requests FINAL WHERE number = 5 AND org_id = 'oracle-org'", database)
		if err := oracle.admin.QueryRow(ctx, query).Scan(&body, &additions, &reviews, &title, &state); err != nil {
			t.Fatalf("%s: %v", database, err)
		}
		return
	}
	pyBody, pyAdditions, pyReviews, pyTitle, pyState := read(oracle.pythonDatabase)
	goBody, goAdditions, goReviews, goTitle, goState := read(oracle.goDB)
	if pyBody != nil || pyAdditions != nil || pyReviews != 0 || pyTitle != nil || pyState != "open" {
		t.Errorf("python clears what it has no field for: body=%v additions=%v reviews=%d title=%v state=%s", pyBody, pyAdditions, pyReviews, pyTitle, pyState)
	}
	if goBody == nil || *goBody != "external body" || goAdditions == nil || *goAdditions != 7 || goReviews != 4 || goTitle != nil || goState != "open" {
		t.Errorf("the port keeps the held columns and states the rest: body=%v additions=%v reviews=%d title=%v state=%s", goBody, goAdditions, goReviews, goTitle, goState)
	}
	writeVenueProof(t)
}
