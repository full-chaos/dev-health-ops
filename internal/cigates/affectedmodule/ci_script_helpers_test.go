package affectedmodule_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// CHAOS-10174 PR C: shared plumbing for the guards that execute checked-in ci scripts. A guard is a function of the
// SCRIPT it runs (path in, error out), so the same scenarios run against the real script (must pass) and against a
// copy with one planted defect (must fail with the named message). That pair is the red proof of each guard.

type cmdResult struct {
	code   int
	stdout string
	stderr string
}

type cmdSpec struct {
	dir   string
	env   []string
	stdin []byte
}

// subprocessSlots caps how many child processes of this package run at once: every guard is parallel, and an uncapped
// package starves the other packages of the same race leg (internal/fixturescli timed out at 10 minutes beside it).
var subprocessSlots = make(chan struct{}, 1)

func (c cmdSpec) run(name string, args ...string) cmdResult {
	subprocessSlots <- struct{}{}
	defer func() { <-subprocessSlots }()
	cmd := exec.Command(name, args...)
	cmd.Dir = c.dir
	cmd.Env = withNoPythonShims(c.env)
	if c.stdin != nil {
		cmd.Stdin = bytes.NewReader(c.stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		} else {
			code = -1
			errb.WriteString(err.Error())
		}
	}
	return cmdResult{code: code, stdout: out.String(), stderr: errb.String()}
}

const basePath = "PATH=/usr/bin:/bin:/usr/local/bin"

// mutatedCopy writes src with exactly one occurrence of old replaced by repl into a fresh temp dir and returns the
// new path. A mutation whose anchor text is absent or ambiguous is an error: a plant that did not happen proves nothing.
func mutatedCopy(t *testing.T, src, old, repl string) string {
	t.Helper()
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), old); n != 1 {
		t.Fatalf("planted-defect anchor %q occurs %d times in %s, want exactly 1: the script changed, re-derive the plant", old, n, src)
	}
	dst := filepath.Join(t.TempDir(), filepath.Base(src))
	if err := os.WriteFile(dst, []byte(strings.Replace(string(raw), old, repl, 1)), 0o755); err != nil {
		t.Fatal(err)
	}
	return dst
}

type scenario struct {
	name string
	run  func(script string, work string) error
}

// assertAllPass runs the scenarios on the real script as subtests.
func assertAllPass(t *testing.T, script string, scenarios []scenario) {
	t.Helper()
	for _, s := range scenarios {
		s := s
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			if err := s.run(script, t.TempDir()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type plant struct {
	name       string
	old, repl  string
	wantFail   string // scenario that must fail
	wantInText string // substring of that scenario's error
	clone      bool   // plant into a copy of the whole tracked tree (scripts that locate the repo from their own path)
}

func p(name, old, repl, wantFail, wantInText string) plant {
	return plant{name: name, old: old, repl: repl, wantFail: wantFail, wantInText: wantInText}
}

func pc(name, old, repl, wantFail, wantInText string) plant {
	return plant{name: name, old: old, repl: repl, wantFail: wantFail, wantInText: wantInText, clone: true}
}

// assertPlantsCaught copies the script once per plant and requires the named scenario to fail with the named message.
func assertPlantsCaught(t *testing.T, script string, scenarios []scenario, plants []plant) {
	t.Helper()
	// One copy of the tracked tree serves every clone plant of this call: the plants take turns, each restores the
	// file by content digest before the next, and a restore that does not match fails the test.
	var cloneDir string
	var cloneMu sync.Mutex
	for _, p := range plants {
		if p.clone {
			cloneDir = cloneRepo(t)
			break
		}
	}
	rel, err := filepath.Rel(rootOf(t), script)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range plants {
		p := p
		t.Run("plant/"+p.name, func(t *testing.T) {
			t.Parallel()
			var planted string
			if p.clone {
				cloneMu.Lock()
				defer cloneMu.Unlock()
				target := filepath.Join(cloneDir, rel)
				original := readText(t, target)
				before := sha256.Sum256([]byte(original))
				if n := strings.Count(original, p.old); n != 1 {
					t.Fatalf("planted-defect anchor %q occurs %d times in %s, want exactly 1", p.old, n, rel)
				}
				if err := os.WriteFile(target, []byte(strings.Replace(original, p.old, p.repl, 1)), 0o755); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.WriteFile(target, []byte(original), 0o755); err != nil {
						t.Fatal(err)
					}
					if after := sha256.Sum256([]byte(readText(t, target))); after != before {
						t.Fatalf("the clone of %s was not restored byte for byte", rel)
					}
				}()
				planted = target
			} else {
				planted = mutatedCopy(t, script, p.old, p.repl)
			}
			var target *scenario
			for i := range scenarios {
				if scenarios[i].name == p.wantFail {
					target = &scenarios[i]
				}
			}
			if target == nil {
				t.Fatalf("plant %q names unknown scenario %q", p.name, p.wantFail)
			}
			err := target.run(planted, t.TempDir())
			if err == nil {
				t.Fatalf("planted defect %q: scenario %q still passed (the guard cannot catch it)", p.name, p.wantFail)
			}
			if !strings.Contains(err.Error(), p.wantInText) {
				t.Fatalf("planted defect %q: scenario %q failed with %q, want it to mention %q", p.name, p.wantFail, err, p.wantInText)
			}
		})
	}
}

func scriptPath(t *testing.T, rel string) string {
	t.Helper()
	p := filepath.Join(rootOf(t), filepath.FromSlash(rel))
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
	return p
}

func readText(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

type gitRepo struct {
	dir string
	err error
}

var gitEnv = []string{
	basePath, "HOME=/nonexistent",
	"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid",
	"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid",
	"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
}

func newGitRepo(dir string) *gitRepo {
	r := &gitRepo{dir: dir}
	r.err = os.MkdirAll(dir, 0o755)
	r.git("init", "-q", "-b", "main")
	return r
}

func (r *gitRepo) git(args ...string) string {
	if r.err != nil {
		return ""
	}
	res := cmdSpec{dir: r.dir, env: gitEnv}.run("git", append([]string{"-c", "commit.gpgsign=false"}, args...)...)
	if res.code != 0 {
		r.err = fmt.Errorf("git %s: %s%s", strings.Join(args, " "), res.stderr, res.stdout)
	}
	return strings.TrimSpace(res.stdout)
}

func (r *gitRepo) write(name, content string) {
	if r.err != nil {
		return
	}
	path := filepath.Join(r.dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		r.err = err
		return
	}
	r.err = os.WriteFile(path, []byte(content), 0o644)
}

func (r *gitRepo) commit(name, content string) string {
	r.write(name, content)
	r.git("add", name)
	r.git("commit", "-q", "-m", "add "+name)
	return r.git("rev-parse", "HEAD")
}

// cloneRepo copies every tracked file of the repository into a fresh git repository (index only, no commit) so a ci
// script that derives its root from its own path and runs git against that root can execute a planted-defect copy.
func cloneRepo(t *testing.T) string {
	t.Helper()
	root := rootOf(t)
	listed := cmdSpec{dir: root, env: gitEnv}.run("git", "ls-files", "-z", "--cached")
	if listed.code != 0 {
		t.Fatalf("git ls-files: %s", listed.stderr)
	}
	dst := t.TempDir()
	copied := 0
	for _, rel := range strings.Split(listed.stdout, "\x00") {
		if rel == "" {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, rel))
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, raw, info.Mode().Perm()); err != nil {
			t.Fatal(err)
		}
		copied++
	}
	if copied < 1000 {
		t.Fatalf("cloned only %d files: the tracked-tree copy measured nothing", copied)
	}
	r := newGitRepo(dst)
	r.git("add", "-A")
	if r.err != nil {
		t.Fatal(r.err)
	}
	return dst
}

// inheritedEnv is the test process environment without the named variables, plus the extra assignments.
func inheritedEnv(drop []string, extra ...string) []string {
	skip := map[string]bool{}
	for _, d := range drop {
		skip[d] = true
	}
	var env []string
	for _, kv := range os.Environ() {
		if !skip[strings.SplitN(kv, "=", 2)[0]] {
			env = append(env, kv)
		}
	}
	return append(env, extra...)
}

var recordOnce sync.Once

// rootOf is repoRoot plus a read of every tracked file. Go's test cache keys a result on the files the test process
// opened; a ci script run through os/exec reads its inputs in a child process the cache cannot see, so a cached pass
// would outlive an edit to the script. Opening every tracked file here makes any tracked-file change a cache miss.
func rootOf(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	recordOnce.Do(func() {
		listed := cmdSpec{dir: root, env: gitEnv}.run("git", "ls-files", "-z", "--cached")
		if listed.code != 0 {
			t.Fatalf("git ls-files: %s", listed.stderr)
		}
		read := 0
		for _, rel := range strings.Split(listed.stdout, "\x00") {
			if rel == "" {
				continue
			}
			if info, err := os.Lstat(filepath.Join(root, rel)); err == nil && info.Mode().IsRegular() {
				if _, err := os.ReadFile(filepath.Join(root, rel)); err == nil {
					read++
				}
			}
		}
		if read < 1000 {
			t.Fatalf("recorded only %d tracked files as test inputs", read)
		}
	})
	return root
}

// repoRootNoT is repoRoot without a *testing.T, for process-wide once-only work.
func repoRootNoT() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// withNoPythonShims puts the directory named by CIGATES_NO_PYTHON_DIR (set by a package's TestMain to a directory of python,
// python3, uv and pip shims that exit 127) first in the PATH of a child environment: every PATH= entry is rewritten, and one
// is added when there is none. Without the variable, or with an inherited (nil) environment, nothing changes.
func withNoPythonShims(env []string) []string {
	dir := os.Getenv("CIGATES_NO_PYTHON_DIR")
	if dir == "" || env == nil {
		return env
	}
	out := make([]string, 0, len(env)+1)
	found := false
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			out = append(out, "PATH="+dir+string(os.PathListSeparator)+v)
			found = true
			continue
		}
		out = append(out, kv)
	}
	if !found {
		out = append(out, "PATH="+dir+string(os.PathListSeparator)+"/usr/bin:/bin")
	}
	return out
}
