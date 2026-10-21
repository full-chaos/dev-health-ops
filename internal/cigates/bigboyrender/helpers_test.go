package bigboyrender_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/bigboyrender"
)

// run calls the verb in-process and returns exit code, stdout and stderr.
func run(args ...string) (int, string, string) {
	var so, se bytes.Buffer
	code := bigboyrender.Run(args, &so, &se)
	return code, so.String(), se.String()
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// gitRepo makes a repository with one commit per text: each is written to rel in turn. It returns the repo and the
// commit shas (a commit with unchanged text is still made).
func gitRepo(t *testing.T, rel string, texts ...string) (string, []string) {
	t.Helper()
	repo := t.TempDir()
	env := append(os.Environ(), "HOME="+repo, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_DATE=2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2026-01-01T00:00:00Z")
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo, "-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	var shas []string
	for _, text := range texts {
		writeFile(t, filepath.Join(repo, rel), text)
		git("add", "-A")
		git("commit", "-q", "--allow-empty", "-m", "m")
		shas = append(shas, git("rev-parse", "HEAD"))
	}
	return repo, shas
}

func repoRootDir(t *testing.T) string { return repoRoot(t) }
