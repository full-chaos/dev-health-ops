package releasetag_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Recorded Python/Go parity pairs. Each directory of testdata/parity is one run of the former scripts/auto_tag.py
// (CPython 3.14.7, python3.14 -I -B) in a temporary git repository; the test replays the case with real git and the built
// cigate binary and compares exit code, stdout and the GITHUB_OUTPUT file byte for byte. See testdata/README.md.

var (
	buildOnce sync.Once
	buildPath string
	buildErr  error
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func cigate(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "cigate-releasetag-")
		if err != nil {
			buildErr = err
			return
		}
		buildPath = filepath.Join(dir, "cigate")
		cmd := exec.Command("go", "build", "-trimpath", "-mod=readonly", "-o", buildPath, "./cmd/cigate")
		cmd.Dir = repoRoot(t)
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = errors.New(err.Error() + ": " + string(out))
		}
	})
	if buildErr != nil {
		t.Fatalf("build cigate: %v", buildErr)
	}
	return buildPath
}

func TestMain(m *testing.M) {
	code := m.Run()
	if buildPath != "" {
		_ = os.RemoveAll(filepath.Dir(buildPath))
	}
	os.Exit(code)
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// gitEnv pins what the recording pinned: no user configuration, a C.UTF-8 locale, and a ceiling so a repository above the
// temporary directory is never found.
func gitEnv(ceiling string, extra ...string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=/nonexistent", "LC_ALL=C.UTF-8",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CEILING_DIRECTORIES=" + ceiling,
	}
	return append(env, extra...)
}

func git(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	full := append([]string{"-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false"}, args...)
	cmd := exec.Command("git", full...)
	cmd.Dir = dir
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

type outcome struct {
	code   int
	stdout string
	stderr string
	output *string
}

// runCase builds the temporary repository of a case and runs the binary in it. The work directory is returned so a caller
// can read what the run left.
func runCase(t *testing.T, bin, dir string) (outcome, string) {
	t.Helper()
	root := t.TempDir()
	work := filepath.Join(root, "w")
	if err := os.Mkdir(work, 0o755); err != nil {
		t.Fatal(err)
	}
	env := gitEnv(root)
	repo := strings.Fields(mustRead(t, filepath.Join(dir, "repo")))
	if len(repo) == 0 || repo[0] != "none" {
		git(t, work, env, "init", "-q")
		for _, line := range strings.Split(strings.TrimRight(mustRead(t, filepath.Join(dir, "repo")), "\n"), "\n") {
			if line == "" {
				continue
			}
			git(t, work, env, "commit", "--allow-empty", "-q", "-m", "c")
			if name, ok := strings.CutPrefix(line, "tag "); ok {
				git(t, work, env, "tag", name)
			}
		}
	}
	in := filepath.Join(dir, "in")
	if exists(in) {
		err := filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
			if err != nil || !info.Mode().IsRegular() {
				return err
			}
			rel, _ := filepath.Rel(in, path)
			dst := filepath.Join(work, strings.TrimSuffix(rel, ".fixture"))
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(dst, data, 0o644)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var args []string
	if err := json.Unmarshal([]byte(mustRead(t, filepath.Join(dir, "args"))), &args); err != nil {
		t.Fatal(err)
	}
	ghPath := filepath.Join(work, "github_output")
	switch {
	case exists(filepath.Join(dir, "no-github-output")):
		ghPath = ""
	case exists(filepath.Join(dir, "github-output-path")):
		if rel := mustRead(t, filepath.Join(dir, "github-output-path")); rel == "" {
			ghPath = ""
		} else {
			ghPath = filepath.Join(work, rel)
		}
	}
	if ghPath != "" {
		env = append(env, "GITHUB_OUTPUT="+ghPath)
	}
	cmd := exec.Command(bin, append([]string{"release", "next-tag"}, args...)...)
	cmd.Dir = work
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	got := outcome{}
	if err := cmd.Run(); err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatal(err)
		}
		got.code = exit.ExitCode()
	}
	sub := func(s string) string { return strings.ReplaceAll(s, work, "{WORK}") }
	got.stdout, got.stderr = sub(stdout.String()), sub(stderr.String())
	if ghPath != "" {
		if info, err := os.Stat(ghPath); err == nil && info.Mode().IsRegular() {
			text := sub(mustRead(t, ghPath))
			got.output = &text
		}
	}
	return got, work
}

func parityCases(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join("testdata", "parity"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	if len(names) < 30 {
		t.Fatalf("only %d recorded cases: the recordings are gone", len(names))
	}
	return names
}

func checkCase(t *testing.T, bin, name string) {
	t.Helper()
	dir := filepath.Join("testdata", "parity", name)
	got, _ := runCase(t, bin, dir)
	wantRC := strings.TrimSpace(mustRead(t, filepath.Join(dir, "rc")))
	if wantRC != strconv.Itoa(got.code) {
		t.Errorf("%s: exit %d, recorded %s", name, got.code, wantRC)
	}
	if want := mustRead(t, filepath.Join(dir, "stdout")); got.stdout != want {
		t.Errorf("%s: stdout %q, recorded %q", name, got.stdout, want)
	}
	switch {
	case exists(filepath.Join(dir, "stderr.traceback")):
		if want := mustRead(t, filepath.Join(dir, "stderr.traceback")); got.stderr != want {
			t.Errorf("%s: stderr %q, recorded %q", name, got.stderr, want)
		}
	case exists(filepath.Join(dir, "stderr.argparse")):
		msg := strings.TrimSpace(mustRead(t, filepath.Join(dir, "stderr.argparse")))
		lines := strings.Split(strings.TrimRight(got.stderr, "\n"), "\n")
		if len(lines) < 2 || !strings.HasPrefix(lines[0], "usage: ") || !strings.HasSuffix(lines[len(lines)-1], "error: "+msg) {
			t.Errorf("%s: stderr %q, recorded argparse message %q", name, got.stderr, msg)
		}
	default:
		if want := mustRead(t, filepath.Join(dir, "stderr")); got.stderr != want {
			t.Errorf("%s: stderr %q, recorded %q", name, got.stderr, want)
		}
	}
	wantFile := filepath.Join(dir, "github_output")
	switch {
	case exists(wantFile) && got.output == nil:
		t.Errorf("%s: GITHUB_OUTPUT file not written, recorded %q", name, mustRead(t, wantFile))
	case exists(wantFile) && *got.output != mustRead(t, wantFile):
		t.Errorf("%s: GITHUB_OUTPUT %q, recorded %q", name, *got.output, mustRead(t, wantFile))
	case !exists(wantFile) && got.output != nil:
		t.Errorf("%s: GITHUB_OUTPUT %q written, none recorded", name, *got.output)
	}
}

func TestEveryRecordedCaseReplaysThroughTheCLI(t *testing.T) {
	bin := cigate(t)
	for _, name := range parityCases(t) {
		t.Run(name, func(t *testing.T) { checkCase(t, bin, name) })
	}
}

// A case with a "stricter" file is not a Python recording: Python accepted the input (or refused after printing) and Go
// refuses before it prints anything. Each is named in the refusal inventory and in RISK-NOTES.
func TestStricterCasesRefuseWithNoStdout(t *testing.T) {
	seen := 0
	for _, name := range parityCases(t) {
		dir := filepath.Join("testdata", "parity", name)
		if !exists(filepath.Join(dir, "stricter")) {
			continue
		}
		seen++
		if rc := strings.TrimSpace(mustRead(t, filepath.Join(dir, "rc"))); rc != "1" {
			t.Errorf("%s: stricter case has exit %s, want 1", name, rc)
		}
		if out := mustRead(t, filepath.Join(dir, "stdout")); out != "" {
			t.Errorf("%s: stricter case has stdout %q, want none", name, out)
		}
		if exists(filepath.Join(dir, "github_output")) {
			t.Errorf("%s: stricter case wrote GITHUB_OUTPUT", name)
		}
	}
	if seen < 8 {
		t.Errorf("only %d stricter cases", seen)
	}
}

var tracebackLine = regexp.MustCompile(`^[A-Za-z]+Error: `)

func TestEveryRefusalCaseNamesAnExceptionLikePython(t *testing.T) {
	for _, name := range parityCases(t) {
		dir := filepath.Join("testdata", "parity", name)
		if !exists(filepath.Join(dir, "stderr.traceback")) {
			continue
		}
		lines := strings.Split(strings.TrimRight(mustRead(t, filepath.Join(dir, "stderr.traceback")), "\n"), "\n")
		last := lines[len(lines)-1]
		if !tracebackLine.MatchString(last) {
			t.Errorf("%s: last stderr line %q is not a Kind: message line", name, last)
		}
		if exists(filepath.Join(dir, "python-exception")) {
			want := strings.TrimSpace(mustRead(t, filepath.Join(dir, "python-exception")))
			if want[strings.LastIndex(want, ".")+1:] != last[:strings.Index(last, ":")] {
				t.Errorf("%s: Go refuses with %q, Python raised %s", name, last, want)
			}
		}
	}
}
