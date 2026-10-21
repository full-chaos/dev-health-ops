package proofteardown_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Each guard is a function of a repo root: green on the real tree, red on a temp copy with ONE planted defect.

const (
	libRel    = "ci/lib/go_worker_fixture.sh"
	scriptRel = "ci/run_metrics_executed_proof.sh"
)

var subject = []string{libRel, scriptRel}

// A `go test &` inherits SIGINT ignored and bash cannot trap an ignored-on-entry signal (CHAOS-6922): reset it.
var (
	defaultSignals = []string{"env", "--default-signal=INT,QUIT"}
	ignoredSIGINT  = []string{"env", "--ignore-signal=INT"}
)

func realRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// guard accumulates the violations of one check run against one root.
type guard struct {
	t        *testing.T
	root     string
	tmp      string
	launch   []string // launcher of every shell under test
	ignoring []string // launcher that leaves SIGINT ignored, as a backgrounded `cmd &` does
	v        []string
}

func (g *guard) failf(format string, args ...any) { g.v = append(g.v, fmt.Sprintf(format, args...)) }

func (g *guard) path(rel string) string { return filepath.Join(g.root, filepath.FromSlash(rel)) }

func (g *guard) read(rel string) string {
	b, err := os.ReadFile(g.path(rel))
	if err != nil {
		g.failf("cannot read %s: %v", rel, err)
		return ""
	}
	return string(b)
}

// expect records a violation unless cond holds.
func (g *guard) expect(cond bool, format string, args ...any) {
	if !cond {
		g.failf(format, args...)
	}
}

// --- real-tree and planted-defect harness ---

type check struct {
	name string
	run  func(g *guard)
}

func newGuard(t *testing.T, root string) *guard {
	return &guard{t: t, root: root, tmp: t.TempDir(), launch: defaultSignals, ignoring: ignoredSIGINT}
}

func runReal(t *testing.T, checks ...check) {
	t.Helper()
	root := realRoot(t)
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			g := newGuard(t, root)
			c.run(g)
			if len(g.v) > 0 {
				t.Fatalf("%d violation(s):\n  %s", len(g.v), strings.Join(g.v, "\n  "))
			}
		})
	}
}

// planted is one defect (in the copied files, or in a launcher when the launch is the subject); check must report want.
type planted struct {
	name     string
	copy     []string
	mutate   func(t *testing.T, root string)
	launch   []string
	ignoring []string
	check    func(g *guard)
	want     string
}

func runPlanted(t *testing.T, cases []planted) {
	t.Helper()
	real := realRoot(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, p := range c.copy {
				copyOne(t, filepath.Join(real, filepath.FromSlash(p)), filepath.Join(root, filepath.FromSlash(p)))
			}
			if c.mutate != nil {
				c.mutate(t, root)
			}
			g := newGuard(t, root)
			if c.launch != nil {
				g.launch = c.launch
			}
			if c.ignoring != nil {
				g.ignoring = c.ignoring
			}
			c.check(g)
			if len(g.v) == 0 {
				t.Fatalf("planted defect %q was not caught: the guard passed", c.name)
			}
			for _, v := range g.v {
				if strings.Contains(v, c.want) {
					t.Logf("caught: %s", v)
					return
				}
			}
			t.Fatalf("planted defect %q was caught, but no violation contains %q:\n  %s", c.name, c.want, strings.Join(g.v, "\n  "))
		})
	}
}

func copyOne(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// plant replaces the first old with repl and fails when old is absent: a defect that did not land proves nothing.
func plant(t *testing.T, root, rel, old, repl string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("plant %s: %v", rel, err)
	}
	if !strings.Contains(string(b), old) {
		t.Fatalf("plant %s: text %q not found, the defect did not land", rel, old)
	}
	if err := os.WriteFile(p, []byte(strings.Replace(string(b), old, repl, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mut(rel, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) { plant(t, root, rel, old, repl) }
}

// all applies several mutations in order: a historical defect that needed two edits to bite is planted whole.
func all(ms ...func(*testing.T, string)) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		for _, m := range ms {
			m(t, root)
		}
	}
}

// --- slicing the shipped functions out of the real scripts ---

// slice requires each anchor exactly once: with first-match, a decoy before the anchor would be what gets tested.
func (g *guard) slice(rel, start, end string, inclusive bool) (string, bool) {
	text := g.read(rel)
	s, okS := g.unique(rel, text, start)
	e, okE := g.unique(rel, text, end)
	if !okS || !okE {
		return "", false
	}
	if s[0] >= e[0] {
		g.failf("%s: anchor %q does not precede anchor %q", rel, start, end)
		return "", false
	}
	if inclusive {
		return text[s[0]:e[1]] + "\n", true
	}
	return text[s[0]:e[0]], true
}

// line returns the one line of rel matching pattern.
func (g *guard) line(rel, pattern string) (string, bool) {
	text := g.read(rel)
	m, ok := g.unique(rel, text, pattern)
	if !ok {
		return "", false
	}
	return text[m[0]:m[1]] + "\n", true
}

func writeExec(g *guard, p, content string) {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		g.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
		g.t.Fatal(err)
	}
}

func (g *guard) unique(rel, text, pattern string) ([]int, bool) {
	all := regexp.MustCompile("(?m)"+pattern).FindAllStringIndex(text, -1)
	if len(all) != 1 {
		g.failf("%s: anchor %q occurs %dx, want exactly 1", rel, pattern, len(all))
		return nil, false
	}
	return all[0], true
}

// definedOnce covers every spelling bash accepts: `name() {` with any spacing and `function name {`.
func (g *guard) definedOnce(rel string, names ...string) bool {
	text := g.read(rel)
	ok := true
	for _, name := range names {
		re := regexp.MustCompile(`(?m)^(?:function\s+` + name + `\b|` + name + `\s*\(\)\s*\{)`)
		if n := len(re.FindAllString(text, -1)); n != 1 {
			g.failf("%s is defined %dx in %s (counting both `name() {` and `function name {`); a second definition would shadow the one these tests slice out", name, n, rel)
			ok = false
		}
	}
	return ok
}

// --- running the shell under test ---

type shResult struct {
	stdout, stderr string
	code           int
	signal         syscall.Signal
	elapsed        time.Duration
}

// bash runs body in a new session, so every process it starts is reaped by that session id, never by name.
func (g *guard) bash(launcher []string, body string, timeout time.Duration) (shResult, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	argv := append(append([]string{}, launcher...), "bash", "-c", body)
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = g.tmp
	cmd.Env = os.Environ()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	// Files, not pipes: a stray child holding stdout must not block Wait.
	outF, errF := g.output("stdout"), g.output("stderr")
	defer outF.Close()
	defer errF.Close()
	cmd.Stdout, cmd.Stderr = outF, errF
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	started := time.Now()
	err := cmd.Start()
	if err != nil {
		g.failf("cannot start %v: %v", argv[:len(launcher)+1], err)
		return shResult{}, false
	}
	sid := cmd.Process.Pid
	// Reaped at cleanup, not here: the checks first assert what the teardown under test left alive.
	g.t.Cleanup(func() { reapSession(sid) })
	err = cmd.Wait()
	res := shResult{elapsed: time.Since(started)}
	res.stdout, res.stderr = mustReadFile(g.t, outF.Name()), mustReadFile(g.t, errF.Name())
	if ctx.Err() != nil {
		g.failf("the shell under test hung: killed after %s (stdout %q, stderr %q)", timeout, res.stdout, res.stderr)
		return res, false
	}
	var ee *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &ee):
		ws := ee.Sys().(syscall.WaitStatus)
		if ws.Signaled() {
			res.signal = ws.Signal()
		}
		res.code = ee.ExitCode()
	default:
		g.failf("bash: %v", err)
		return res, false
	}
	return res, true
}

func (g *guard) output(name string) *os.File {
	f, err := os.CreateTemp(g.tmp, name)
	if err != nil {
		g.t.Fatal(err)
	}
	return f
}

func mustReadFile(t testing.TB, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// reapSession: a live /proc/<sid> means a reused pid, which the kernel allows only once the session is empty.
func reapSession(sid int) {
	for range 50 {
		if _, err := os.Stat("/proc/" + strconv.Itoa(sid)); err == nil {
			return
		}
		members := sessionMembers(sid)
		if len(members) == 0 {
			return
		}
		for _, pid := range members {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func sessionMembers(sid int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var out []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		state, fields, ok := procStat(pid)
		// fields after the command name: state ppid pgrp session ...
		if ok && state != "Z" && len(fields) > 3 && fields[3] == strconv.Itoa(sid) {
			out = append(out, pid)
		}
	}
	return out
}

func procStat(pid int) (string, []string, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", nil, false
	}
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 {
		return "", nil, false
	}
	fields := strings.Fields(string(b)[i+1:])
	if len(fields) == 0 {
		return "", nil, false
	}
	return fields[0], fields, true
}

// alive reports whether pid is a live, non-zombie process.
func alive(pid int) bool {
	state, _, ok := procStat(pid)
	return ok && state != "Z"
}

// diedWithin polls until pid is gone or d passes.
func diedWithin(pid int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !alive(pid) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return !alive(pid)
}

func readPid(g *guard, p string) (int, bool) {
	b, err := os.ReadFile(p)
	if err != nil {
		g.failf("the shell under test did not record a pid in %s: %v", filepath.Base(p), err)
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 1 {
		g.failf("bad pid %q in %s", string(b), filepath.Base(p))
		return 0, false
	}
	return pid, true
}
