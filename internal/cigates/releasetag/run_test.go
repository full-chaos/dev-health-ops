package releasetag_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/releasetag"
)

func runVerb(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := releasetag.Run(args, func(name string) string { return env[name] }, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// initRepo makes a repository with one tagged commit and makes it the working directory.
func initRepo(t *testing.T, tag string) string {
	t.Helper()
	dir := t.TempDir()
	env := gitEnv(filepath.Dir(dir))
	git(t, dir, env, "init", "-q")
	git(t, dir, env, "commit", "--allow-empty", "-q", "-m", "c")
	if tag != "" {
		git(t, dir, env, "tag", tag)
	}
	for _, kv := range env {
		name, value, _ := strings.Cut(kv, "=")
		t.Setenv(name, value)
	}
	t.Chdir(dir)
	return dir
}

func TestVerbNeedsNextTag(t *testing.T) {
	for _, args := range [][]string{nil, {"bogus"}, {"--package-json", "x"}} {
		if code, stdout, _ := runVerb(t, nil, args...); code != 2 || stdout != "" {
			t.Errorf("%v: exit %d, stdout %q; want exit 2 and no stdout", args, code, stdout)
		}
	}
}

// Python 3.14.7: -h exits 0 whatever follows it; an unknown option, a stray word or an ambiguous abbreviation after it is
// never looked at, and one before it ends the run with exit 2 first. The usage text is Go's own.
func TestHelpWinsOverLaterArguments(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {"--he"}, {"-hh"}, {"-hx"}, {"-h", "--p"}, {"--bogus", "-h"}, {"x", "--help"}, {"-h", "--pre-release-suffix"}} {
		code, stdout, stderr := runVerb(t, nil, append([]string{"next-tag"}, args...)...)
		if code != 0 || !strings.HasPrefix(stdout, "usage: ") || stderr != "" {
			t.Errorf("%v: exit %d, stdout %q, stderr %q; want exit 0 and the usage line", args, code, stdout, stderr)
		}
	}
	for _, args := range [][]string{{"--p", "-h"}, {"--pre-release-suffix", "-h"}, {"--help=x"}, {"-h=h"}} {
		if code, stdout, _ := runVerb(t, nil, append([]string{"next-tag"}, args...)...); code != 2 || stdout != "" {
			t.Errorf("%v: exit %d, stdout %q; want exit 2", args, code, stdout)
		}
	}
}

func TestGitMissingRefuses(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	code, stdout, stderr := runVerb(t, nil, "next-tag")
	if code != 1 || stdout != "" || stderr != "FileNotFoundError: [Errno 2] No such file or directory: 'git'\n" {
		t.Errorf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

func TestGitOutputNotUTF8Refuses(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\nprintf 'v1.2.3\\377\\n'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	code, stdout, stderr := runVerb(t, nil, "next-tag")
	if code != 1 || stdout != "" || !strings.HasPrefix(stderr, "UnicodeDecodeError: ") {
		t.Errorf("exit %d, stdout %q, stderr %q; want exit 1, no stdout, a UnicodeDecodeError line", code, stdout, stderr)
	}
}

func TestGitFailingIsNoTagAndItsComplaintReachesStderr(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\necho 'fatal: nope' >&2\nexit 128\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Chdir(t.TempDir())
	code, stdout, stderr := runVerb(t, nil, "next-tag")
	if code != 0 || stderr != "fatal: nope\n" || !strings.HasPrefix(stdout, "Latest tag: None\n") {
		t.Errorf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

// Python 3.14.7 under LC_ALL=C.UTF-8 writes a suffix byte that is not UTF-8 to stdout as it is (surrogateescape) and fails
// when it writes the same tag to the GITHUB_OUTPUT file (strict UTF-8), after the file was opened and before it got a byte.
func TestInvalidUTF8SuffixPassesStdoutAndRefusesTheOutputFile(t *testing.T) {
	dir := initRepo(t, "v1.2.3")
	code, stdout, stderr := runVerb(t, nil, "next-tag", "--pre-release-suffix", "a\xffb")
	if code != 0 || stderr != "" || stdout != "Latest tag: v1.2.3\nPackage.json version: None\nNext tag determined: v1.2.4-a\xffb.1\n" {
		t.Errorf("no output file: exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
	out := filepath.Join(dir, "out")
	code, stdout, stderr = runVerb(t, map[string]string{"GITHUB_OUTPUT": out}, "next-tag", "--pre-release-suffix", "a\xffb")
	if code != 1 || !strings.HasSuffix(stdout, "Next tag determined: v1.2.4-a\xffb.1\n") || !strings.HasPrefix(stderr, "UnicodeEncodeError: ") {
		t.Errorf("output file: exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
	if info, err := os.Stat(out); err != nil || info.Size() != 0 {
		t.Errorf("output file: %v, %v; want an empty file", info, err)
	}
}

func TestGithubOutputWriteFails(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("no /dev/full")
	}
	initRepo(t, "v1.2.3")
	code, stdout, stderr := runVerb(t, map[string]string{"GITHUB_OUTPUT": "/dev/full"}, "next-tag")
	if code != 1 || !strings.HasSuffix(stdout, "Next tag determined: v1.2.4-beta.1\n") || !strings.HasPrefix(stderr, "OSError: ") {
		t.Errorf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

func TestPackageJSONUnreadableRefuses(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode 000 file")
	}
	dir := initRepo(t, "v1.2.3")
	path := filepath.Join(dir, "p.json")
	if err := os.WriteFile(path, []byte(`{"version":"2.0.0"}`), 0o000); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runVerb(t, nil, "next-tag", "--package-json", "p.json")
	if code != 1 || stdout != "" || stderr != "PermissionError: [Errno 13] Permission denied: 'p.json'\n" {
		t.Errorf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }

// Python 3.14.7 held stdout in a buffer, flushed it once at exit and, when that failed, printed two lines to stderr and exited
// 120 whatever the status was before; with nothing in the buffer the status stood.
func TestStdoutFlushFailureExits120(t *testing.T) {
	initRepo(t, "v1.2.3")
	for _, c := range []struct {
		name string
		err  error
		line string
	}{
		{"no space", syscall.ENOSPC, "OSError: [Errno 28] No space left on device"},
		{"broken pipe", syscall.EPIPE, "BrokenPipeError: [Errno 32] Broken pipe"},
		{"bad descriptor", syscall.EBADF, "OSError: [Errno 9] Bad file descriptor"},
		{"not an errno", errors.New("boom"), "OSError: boom"},
	} {
		var stderr bytes.Buffer
		code := releasetag.Run([]string{"next-tag"}, func(string) string { return "" }, failingWriter{c.err}, &stderr)
		want := "Exception ignored while flushing sys.stdout:\n" + c.line + "\n"
		if code != 120 || !strings.HasSuffix(stderr.String(), want) {
			t.Errorf("%s: exit %d, stderr %q; want exit 120 ending %q", c.name, code, stderr.String(), want)
		}
	}
	var stderr bytes.Buffer
	if code := releasetag.Run([]string{"next-tag"}, func(string) string { return "" }, shortWriter{}, &stderr); code != 120 {
		t.Errorf("short write: exit %d, want 120", code)
	}
	if code := releasetag.Run([]string{"next-tag", "--bogus"}, func(string) string { return "" }, failingWriter{syscall.ENOSPC}, &stderr); code != 2 {
		t.Errorf("nothing printed: exit %d, want 2 (no flush, no failure)", code)
	}
}

// GITHUB_OUTPUT is written before stdout is flushed, so a dead stdout leaves the file complete.
func TestStdoutFlushFailureStillWritesGithubOutput(t *testing.T) {
	dir := initRepo(t, "v1.2.3")
	out := filepath.Join(dir, "out")
	var stderr bytes.Buffer
	code := releasetag.Run([]string{"next-tag"}, func(n string) string {
		if n == "GITHUB_OUTPUT" {
			return out
		}
		return ""
	}, failingWriter{syscall.ENOSPC}, &stderr)
	if code != 120 || mustRead(t, out) != "new_tag=v1.2.4-beta.1\n" {
		t.Errorf("exit %d, file %q", code, mustRead(t, out))
	}
}
