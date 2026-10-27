package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11670: ci/auth_corpus_queue_relevance.sh is relevant when a changed file sits in a directory of the LIVE
// `go list -deps` closure of the auth route packages (the repo module only, embedded-file directories included), besides
// the rows of ci/auth_corpus_paths.txt. It runs go with GOFLAGS=-mod=readonly, GOTOOLCHAIN=local and LC_ALL=C, and it
// fails closed: a go failure, an empty list, a malformed directory or a missing root prints corpus=true.

var corpusLiveRoots = []string{
	"./internal/api/session", "./internal/api/sso", "./internal/api/policy", "./internal/apiservice", "./cmd/dho", "./cmd/auth-service",
}

// corpusLiveRun runs the script over a group that changes paths, with the fake go answering FAKE_GO_LIST etc. (extra env).
func corpusLiveRun(t *testing.T, extra []string, paths ...string) string {
	t.Helper()
	dir := corpusQueueRepo(t, true)
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, paths...)
	return corpusQueueRunEnv(t, dir, extra, tip, head)
}

func TestAuthCorpusQueueLiveClosure(t *testing.T) {
	list := "FAKE_GO_LIST=P example.com/x/internal/api/sessionissue\nP example.com/x/cmd/dho\nE example.com/x/internal/mail/templates/a.html\nE example.com/x/internal/mail/templates/sub/b.html"
	cases := []struct {
		name  string
		paths []string
		want  string
	}{
		{"a Go file of a closure package", []string{"internal/api/sessionissue/issue.go"}, "corpus=true"},
		{"a test file of a closure package", []string{"internal/api/sessionissue/issue_test.go"}, "corpus=true"},
		{"a non-Go file of a closure package", []string{"internal/api/sessionissue/testdata.json"}, "corpus=true"},
		{"a file of a second package", []string{"cmd/dho/other.go"}, "corpus=true"},
		{"a file of an embed directory", []string{"internal/mail/templates/new.html"}, "corpus=true"},
		{"a file of a nested embed directory", []string{"internal/mail/templates/sub/new.html"}, "corpus=true"},
		{"a file name with a newline in a closure package", []string{"internal/api/sessionissue/a\nb.go"}, "corpus=true"},
		{"a hostile file name in a closure package", []string{"internal/api/sessionissue/a'\"$(x)`y\\z[1]*?.go"}, "corpus=true"},
		{"a file of a package outside the closure", []string{"internal/providersync/sync.go"}, "corpus=false"},
		{"a subdirectory of a closure package is another package", []string{"internal/api/sessionissue/sub/x.go"}, "corpus=false"},
		{"a sibling that shares the prefix of a closure directory", []string{"internal/api/sessionissue2/x.go"}, "corpus=false"},
		{"the parent of a closure directory", []string{"internal/api/x.go"}, "corpus=false"},
		{"a subdirectory of an embed directory that is not embedded", []string{"internal/mail/templates/other/x.html"}, "corpus=false"},
		{"a file at the repository root", []string{"README.md"}, "corpus=false"},
		{"docs only", []string{"docs/index.md"}, "corpus=false"},
		{"outside and inside in one group", []string{"docs/a.md", "cmd/dho/x.go"}, "corpus=true"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := corpusLiveRun(t, []string{list}, c.paths...); got != c.want {
				t.Fatalf("change %q: got %q, want %q", c.paths, got, c.want)
			}
		})
	}
}

func TestAuthCorpusQueueLiveClosureFailsClosed(t *testing.T) {
	good := "FAKE_GO_LIST=P example.com/x/internal/good"
	cases := []struct {
		name  string
		extra []string
	}{
		{"go list fails", []string{"FAKE_GO_MODE=fail"}},
		{"go list prints nothing", []string{"FAKE_GO_MODE=empty"}},
		{"go list -m fails", []string{"FAKE_GO_MODE=nomod", good}},
		{"go list prints only blank lines", []string{"FAKE_GO_LIST=\n\n"}},
		{"a line that is neither P nor E", []string{"FAKE_GO_LIST=P example.com/x/internal/good\nX example.com/x/internal/a"}},
		{"a package of another module", []string{"FAKE_GO_LIST=P example.com/x/internal/good\nP other.com/y/internal/a"}},
		{"the module root itself", []string{"FAKE_GO_LIST=P example.com/x/internal/good\nP example.com/x"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := corpusLiveRun(t, c.extra, "docs/a.md"); got != "corpus=true" {
				t.Fatalf("got %q, want corpus=true (fails closed)", got)
			}
		})
	}
	bad := []string{"internal/a/*", "internal/a?", "internal/[a]", "internal\\a", "../a", "internal/../a", "internal/./a", "internal//a",
		"internal/a b", "internal/a\r", "internal/a\v", "internal/a\f", "\u00a0internal/a", "\ufeffinternal/a", "internal/{a,b}", "!(internal)",
		"internal/\u00e9", "internal/\u00c9", "internal/\u0430", "internal/$(x)", "internal/`x`", "internal/a'b", "internal/a\"b"}
	for _, row := range bad {
		for _, kind := range []string{"P", "E"} {
			t.Run(fmt.Sprintf("%s %q", kind, row), func(t *testing.T) {
				path := "example.com/x/" + row
				if kind == "E" {
					path += "/file.html"
				}
				extra := []string{"FAKE_GO_LIST=P example.com/x/internal/good\n" + kind + " " + path}
				if got := corpusLiveRun(t, extra, "docs/a.md"); got != "corpus=true" {
					t.Fatalf("row %q: got %q, want corpus=true (refused, fails closed)", row, got)
				}
			})
		}
	}
	t.Run("a well-formed list and an unrelated change", func(t *testing.T) {
		if got := corpusLiveRun(t, []string{good}, "docs/a.md"); got != "corpus=false" {
			t.Fatalf("got %q, want corpus=false", got)
		}
	})
}

func TestAuthCorpusQueueLiveClosureWithoutGoOrWithATimeout(t *testing.T) {
	// no go on PATH at all
	dir := corpusQueueRepo(t, true)
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, "docs/a.md")
	if err := os.Remove(filepath.Join(corpusQueueBinDir(dir), "go")); err != nil {
		t.Fatal(err)
	}
	if got := corpusQueueRunEnv(t, dir, nil, tip, head); got != "corpus=true" {
		t.Errorf("no go on PATH: got %q, want corpus=true", got)
	}
	// a timeout that fires (rc 124) is a failure of go list
	dir = corpusQueueRepo(t, true)
	tip = corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head = corpusQueueCommit(t, dir, "docs/a.md")
	fakeTimeout := "#!/bin/sh\nexit 124\n"
	if err := os.WriteFile(filepath.Join(corpusQueueBinDir(dir), "timeout"), []byte(fakeTimeout), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := corpusQueueRunEnv(t, dir, nil, tip, head); got != "corpus=true" {
		t.Errorf("go list timed out: got %q, want corpus=true", got)
	}
}

func TestAuthCorpusQueueLiveClosureNamesItsFailureOnce(t *testing.T) {
	dir := corpusQueueRepo(t, true)
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, "docs/a.md")
	cmd := exec.Command("bash", filepath.Join(dir, "ci", "auth_corpus_queue_relevance.sh"), tip, head)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + corpusQueueBinDir(dir) + ":/usr/bin:/bin:/usr/local/bin", "HOME=" + t.TempDir(), "FAKE_GO_MODE=fail"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	var named int
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "closure") && strings.Contains(line, "the corpus runs") {
			named++
		}
	}
	if named != 1 {
		t.Errorf("want exactly one named closure failure line, got %d:\n%s", named, out)
	}
}

// The script pins its own environment for go, whatever the caller exports, and passes the six roots.
func TestAuthCorpusQueueLiveClosurePinsTheGoEnvironmentAndRoots(t *testing.T) {
	dir := corpusQueueRepo(t, true)
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, "docs/a.md")
	envlog := filepath.Join(t.TempDir(), "env")
	arglog := filepath.Join(t.TempDir(), "args")
	extra := []string{"GOFLAGS=-mod=mod", "GOTOOLCHAIN=auto", "LC_ALL=en_US.UTF-8", "FAKE_GO_ENVLOG=" + envlog, "FAKE_GO_ARGLOG=" + arglog}
	if got := corpusQueueRunEnv(t, dir, extra, tip, head); got != "corpus=false" {
		t.Fatalf("got %q", got)
	}
	raw, err := os.ReadFile(envlog)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line != "GOFLAGS=-mod=readonly GOTOOLCHAIN=local LC_ALL=C" {
			t.Errorf("go ran with %q, want GOFLAGS=-mod=readonly GOTOOLCHAIN=local LC_ALL=C", line)
		}
	}
	args, err := os.ReadFile(arglog)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "list -deps -f ") || !strings.HasSuffix(strings.TrimSpace(string(args)), strings.Join(corpusLiveRoots, " ")) {
		t.Errorf("go list arguments: %s", args)
	}
}

// The floor is judged first: a floor match never runs go.
func TestAuthCorpusQueueFloorMatchDoesNotRunGo(t *testing.T) {
	dir := corpusQueueRepo(t, true)
	tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
	head := corpusQueueCommit(t, dir, "internal/auth/x.go")
	arglog := filepath.Join(t.TempDir(), "args")
	if got := corpusQueueRunEnv(t, dir, []string{"FAKE_GO_ARGLOG=" + arglog}, tip, head); got != "corpus=true" {
		t.Fatalf("got %q", got)
	}
	if _, err := os.Stat(arglog); err == nil {
		t.Error("go list ran although the floor already matched")
	}
}

func TestAuthCorpusQueueLiveClosureRowIsAsciiInEveryLocale(t *testing.T) {
	for _, locale := range []string{"C", "C.UTF-8", "en_US.UTF-8"} {
		for _, row := range []string{"internal/\u00e9", "internal/\u00c9", "internal/a\u00e9b", "internal/\u0430"} {
			t.Run(fmt.Sprintf("%s %q", locale, row), func(t *testing.T) {
				extra := []string{"LC_ALL=" + locale, "FAKE_GO_LIST=P example.com/x/internal/good\nP example.com/x/" + row}
				if got := corpusLiveRun(t, extra, "docs/a.md"); got != "corpus=true" {
					t.Fatalf("LC_ALL=%s row %q: got %q, want corpus=true", locale, row, got)
				}
			})
		}
	}
}

// The script pins the C locale itself, before its first pattern match or read.
func TestAuthCorpusQueueRelevanceScriptPinsTheCLocale(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "auth_corpus_queue_relevance.sh"))
	if err != nil {
		t.Fatal(err)
	}
	pin := -1
	for i, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "export LC_ALL=C" {
			pin = i
			break
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "set -") {
			continue
		}
		if strings.Contains(trimmed, "[[") || strings.Contains(trimmed, "case ") || strings.Contains(trimmed, "read ") || strings.Contains(trimmed, "$(") {
			t.Fatalf("line %d reaches a pattern match, a read or a command before `export LC_ALL=C`: %s", i+1, line)
		}
	}
	if pin < 0 {
		t.Fatal("ci/auth_corpus_queue_relevance.sh never runs `export LC_ALL=C`")
	}
}

// A hermetic fixture module with the six roots: the script runs the REAL go list on it.
func corpusFixtureRepo(t *testing.T) (dir string, env []string) {
	t.Helper()
	dir = corpusQueueRepo(t, true)
	if err := os.Remove(filepath.Join(corpusQueueBinDir(dir), "go")); err != nil {
		t.Fatal(err)
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go on PATH")
	}
	files := map[string]string{
		"go.mod":                            "module example.com/fix\n\ngo 1.21\n",
		"internal/api/session/s.go":         "package session\n\nimport _ \"example.com/fix/internal/liba\"\n",
		"internal/api/sso/s.go":             "package sso\n\nimport \"embed\"\n\n//go:embed templates/*.html\nvar T embed.FS\n",
		"internal/api/sso/templates/a.html": "<p>a</p>\n",
		"internal/api/policy/p.go":          "package policy\n",
		"internal/apiservice/a.go":          "package apiservice\n",
		"cmd/dho/main.go":                   "package main\n\nimport _ \"example.com/fix/internal/libb\"\n\nfunc main() {}\n",
		"cmd/auth-service/main.go":          "package main\n\nfunc main() {}\n",
		"internal/liba/a.go":                "package liba\n",
		"internal/libb/b.go":                "package libb\n",
		"internal/unrelated/u.go":           "package unrelated\n",
	}
	for name, body := range files {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	corpusQueueGit(t, dir, "add", "-A")
	corpusQueueGit(t, dir, "commit", "-q", "-m", "fixture")
	tmp := t.TempDir()
	env = []string{"PATH=" + filepath.Dir(goBin) + ":/usr/bin:/bin:/usr/local/bin", "GOCACHE=" + filepath.Join(tmp, "cache"), "GOPATH=" + filepath.Join(tmp, "path"), "GOMODCACHE=" + filepath.Join(tmp, "mod")}
	return dir, env
}

func TestAuthCorpusQueueLiveClosureOnAFixtureModule(t *testing.T) {
	cases := []struct {
		name, path, want string
	}{
		{"a package a root imports", "internal/liba/new.go", "corpus=true"},
		{"a package a binary imports", "internal/libb/new.go", "corpus=true"},
		{"a root itself", "internal/api/policy/new.go", "corpus=true"},
		{"an embedded-file directory", "internal/api/sso/templates/new.html", "corpus=true"},
		{"a package no root imports", "internal/unrelated/new.go", "corpus=false"},
		{"docs", "docs/a.md", "corpus=false"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir, env := corpusFixtureRepo(t)
			tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
			head := corpusQueueCommit(t, dir, c.path)
			if got := corpusQueueRunEnv(t, dir, env, tip, head); got != c.want {
				t.Fatalf("change %s: got %q, want %q", c.path, got, c.want)
			}
		})
	}
	t.Run("a missing root", func(t *testing.T) {
		dir, env := corpusFixtureRepo(t)
		corpusQueueGit(t, dir, "rm", "-q", "-r", "cmd/auth-service")
		corpusQueueGit(t, dir, "commit", "-q", "-m", "drop a root")
		tip := corpusQueueGit(t, dir, "rev-parse", "HEAD")
		head := corpusQueueCommit(t, dir, "docs/a.md")
		if got := corpusQueueRunEnv(t, dir, env, tip, head); got != "corpus=true" {
			t.Fatalf("a root that does not exist: got %q, want corpus=true", got)
		}
	})
}

// The real tree: the closure is computed here by go list, and the script, run over a temp repository whose go is a
// wrapper for the real go in the real tree, agrees for sampled closure, embed and non-closure directories. No golden.
func TestAuthCorpusQueueLiveClosureOnTheRealTree(t *testing.T) {
	root := repoRoot(t)
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go on PATH")
	}
	tmpl := `{{if not .Standard}}{{with .Module}}{{if eq .Path "github.com/full-chaos/dev-health-ops"}}P {{$.ImportPath}}{{"\n"}}{{range $.EmbedFiles}}E {{$.ImportPath}}/{{.}}{{"\n"}}{{end}}{{end}}{{end}}{{end}}`
	run := func(args ...string) string {
		cmd := exec.Command(goBin, args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=readonly", "GOTOOLCHAIN=local")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go %v: %v", args, err)
		}
		return string(out)
	}
	const mod = "github.com/full-chaos/dev-health-ops/"
	in := map[string]bool{}
	var pkgs, embeds []string
	for _, line := range strings.Split(run(append([]string{"list", "-deps", "-f", tmpl}, corpusLiveRoots...)...), "\n") {
		kind, path, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			continue
		}
		path = strings.TrimPrefix(path, mod)
		if kind == "E" {
			d := filepath.Dir(path)
			if !in[d] {
				embeds = append(embeds, d)
			}
			in[d] = true
			continue
		}
		in[path] = true
		pkgs = append(pkgs, path)
	}
	sort.Strings(pkgs)
	sort.Strings(embeds)
	if len(pkgs) < 50 || len(embeds) == 0 {
		t.Fatalf("the closure looks wrong: %d packages, %d embed dirs", len(pkgs), len(embeds))
	}
	var outside []string
	for _, line := range strings.Split(run("list", "-f", "{{.ImportPath}}", "./..."), "\n") {
		p := strings.TrimPrefix(strings.TrimSpace(line), mod)
		if p == "" || in[p] || strings.HasPrefix(p, "internal/auth") || strings.HasPrefix(p, "internal/audit") || strings.HasPrefix(p, "internal/testsupport") ||
			strings.HasPrefix(p, "cmd/auth-") || strings.HasPrefix(p, "contracts/auth") || strings.HasPrefix(p, "internal/storage/postgres/authschema") {
			continue
		}
		outside = append(outside, p)
	}
	sort.Strings(outside)
	if len(outside) < 3 {
		t.Fatalf("too few packages outside the closure: %d", len(outside))
	}
	wrapper := "#!/bin/sh\ncd '" + root + "' || exit 1\nexec '" + goBin + "' \"$@\"\n"
	env := []string{"GOCACHE=" + strings.TrimSpace(run("env", "GOCACHE")), "GOMODCACHE=" + strings.TrimSpace(run("env", "GOMODCACHE")), "GOPATH=" + strings.TrimSpace(run("env", "GOPATH"))}
	for k, v := range map[string][]string{"closure": {pkgs[0], pkgs[len(pkgs)/2], pkgs[len(pkgs)-1]}, "embed": {embeds[0], embeds[len(embeds)-1]}} {
		for _, d := range v {
			t.Run(k+" "+d, func(t *testing.T) {
				d2 := corpusQueueRepo(t, true)
				if err := os.WriteFile(filepath.Join(corpusQueueBinDir(d2), "go"), []byte(wrapper), 0o755); err != nil {
					t.Fatal(err)
				}
				tip := corpusQueueGit(t, d2, "rev-parse", "HEAD")
				head := corpusQueueCommit(t, d2, d+"/zz_live_probe.txt")
				if got := corpusQueueRunEnv(t, d2, env, tip, head); got != "corpus=true" {
					t.Fatalf("%s %s: got %q, want corpus=true", k, d, got)
				}
			})
		}
	}
	for _, d := range []string{outside[0], outside[len(outside)/2], outside[len(outside)-1]} {
		t.Run("outside "+d, func(t *testing.T) {
			d2 := corpusQueueRepo(t, true)
			if err := os.WriteFile(filepath.Join(corpusQueueBinDir(d2), "go"), []byte(wrapper), 0o755); err != nil {
				t.Fatal(err)
			}
			tip := corpusQueueGit(t, d2, "rev-parse", "HEAD")
			head := corpusQueueCommit(t, d2, d+"/zz_live_probe.txt")
			if got := corpusQueueRunEnv(t, d2, env, tip, head); got != "corpus=false" {
				t.Fatalf("outside %s: got %q, want corpus=false", d, got)
			}
		})
	}
}
