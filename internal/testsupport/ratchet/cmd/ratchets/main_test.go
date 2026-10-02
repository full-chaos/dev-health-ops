package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The built command passes the status of ratchet.Run to the shell: a grown list exits 1, an unchanged tree exits 0.
func TestTheCommandExitsWithTheStatusOfRun(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) string {
		command := exec.Command("git", append([]string{"-C", repo}, args...)...)
		command.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@e.test", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@e.test", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	write := func(path, text string) {
		full := filepath.Join(repo, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ci/ratchets.tsv", "closed\tlists/closed.txt\tt\n")
	write("lists/closed.txt", "a\nb\n")
	git("add", "-A")
	git("commit", "-q", "-m", "base")
	base := git("rev-parse", "HEAD")
	exit := func() int {
		command := exec.Command("go", "run", ".", "-repo", repo, "-base", base)
		err := command.Run()
		if err == nil {
			return 0
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		t.Fatal(err)
		return -1
	}
	if code := exit(); code != 0 {
		t.Fatalf("an unchanged tree exited %d", code)
	}
	write("lists/closed.txt", "a\nb\nc\n")
	if code := exit(); code != 1 {
		t.Fatalf("a grown list exited %d, want 1", code)
	}
}
