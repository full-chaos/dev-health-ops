package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The tests here drive the real binary as a real process, because that is the
// only way to test the thing they are about: what a signal does. An in-process
// context cancellation is a different mechanism and is covered separately in
// internal/gqlgenguard.

const fixtureConfig = `
schema: [schema.graphql]
exec: {filename: gen/generated.go, package: gen}
model: {filename: gen/model/models_gen.go, package: model}
resolver: {layout: follow-schema, dir: gen, package: gen}
`

const fixtureSchema = `type Query {
  hello(name: String): String!
  item: Item
}

type Item {
  key: String!
  value: Float
}
`

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("expected a go.mod at %s: %v", root, err)
	}
	return root
}

// buildGuard compiles the binary once for the whole package, into a directory
// TestMain removes when the package's tests finish.
var (
	guardOnce sync.Once
	guardDir  string
	guardPath string
	guardErr  error
)

// TestMain exists only to remove the binary's build directory. The build is
// shared by several tests, so no single test's t.TempDir can own it -- and an
// os.MkdirTemp with no removal left one gqlgen-guard-bin-* directory behind in
// TMPDIR per run (fourteen had piled up on the lane host before it was seen).
func TestMain(m *testing.M) {
	code := m.Run()
	if guardDir != "" {
		if err := os.RemoveAll(guardDir); err != nil {
			fmt.Fprintf(os.Stderr, "remove the guard's build directory %s: %v\n", guardDir, err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}

func buildGuard(t *testing.T) string {
	t.Helper()
	guardOnce.Do(func() {
		dir, err := os.MkdirTemp("", "gqlgen-guard-bin-")
		if err != nil {
			guardErr = err
			return
		}
		guardDir = dir
		guardPath = filepath.Join(dir, "gqlgen-guard")
		cmd := exec.Command("go", "build", "-o", guardPath, "./cmd/gqlgen-guard")
		cmd.Dir = repoRoot(t)
		out, err := cmd.CombinedOutput()
		if err != nil {
			guardErr = fmt.Errorf("build the guard: %v\n%s", err, out)
		}
	})
	if guardErr != nil {
		t.Fatalf("%v", guardErr)
	}
	return guardPath
}

// newGeneratableFixture builds a module the real gqlgen CLI can generate in:
// this repository's go.mod, go.sum and tools.go plus a small schema and config.
func newGeneratableFixture(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	root := repoRoot(t)
	for _, name := range []string{"go.mod", "go.sum", "tools.go"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		write(t, dir, name, string(data))
	}
	write(t, dir, "gqlgen.yml", fixtureConfig)
	write(t, dir, "schema.graphql", fixtureSchema)
	return dir
}

func write(t *testing.T, dir, rel, body string) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("create directory for %s: %v", rel, err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

// treeDigests digests every file under dir, which is how "the tree is
// untouched" is asserted here.
func treeDigests(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			out[filepath.ToSlash(rel)] = "symlink:" + target
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		out[filepath.ToSlash(rel)] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatalf("digest the tree: %v", err)
	}
	return out
}

func assertTreeUnchanged(t *testing.T, before, after map[string]string, context string) {
	t.Helper()
	var problems []string
	for rel, b := range before {
		a, ok := after[rel]
		if !ok {
			problems = append(problems, rel+" was deleted")
			continue
		}
		if a != b {
			problems = append(problems, fmt.Sprintf("%s changed: %s -> %s", rel, b, a))
		}
	}
	for rel := range after {
		if _, ok := before[rel]; !ok {
			problems = append(problems, rel+" was created")
		}
	}
	if len(problems) > 0 {
		t.Fatalf("%s: the tree was modified:\n  %s", context, strings.Join(problems, "\n  "))
	}
}

// TestInterruptingTheBinaryLeavesTheTreeUntouched sends each signal the guard
// catches to a real process running the real generator, at a point where
// generation has provably started, and asserts the working tree is byte for
// byte what it was.
//
// The signal is sent only after the guard's own report shows it reached the
// generator, so a passing row cannot be a signal that arrived before any work
// began -- which would prove nothing.
func TestInterruptingTheBinaryLeavesTheTreeUntouched(t *testing.T) {
	signals := []struct {
		name string
		sig  syscall.Signal
	}{
		{"SIGINT", syscall.SIGINT},
		{"SIGTERM", syscall.SIGTERM},
		{"SIGHUP", syscall.SIGHUP},
		{"SIGPIPE", syscall.SIGPIPE},
	}

	bin := buildGuard(t)

	for _, s := range signals {
		t.Run(s.name, func(t *testing.T) {
			dir := newGeneratableFixture(t)
			before := treeDigests(t, dir)

			// The guard makes its private copy under TMPDIR. Pointing that at
			// a directory this test owns is what lets the assertion below say
			// the copy was removed without racing anything else on the host.
			tmp := t.TempDir()
			cmd := exec.Command(bin, "generate", "-module", dir, "-config", "gqlgen.yml",
				"-record", "contracts/expected-drift.record")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "TMPDIR="+tmp)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatalf("stdout pipe: %v", err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			// The guard must be in its own process group, or a signal sent to
			// it could reach the generator by a route the guard does not
			// control and the test would be measuring the wrong thing.
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

			if err := cmd.Start(); err != nil {
				t.Fatalf("start the guard: %v", err)
			}

			// Read until the guard reports that it is running the generator.
			// A mutex guards the buffer because the reader runs in its own
			// goroutine; without it the race detector, not a reviewer, would
			// be the one to find this.
			reached := make(chan struct{})
			var (
				mu   sync.Mutex
				seen bytes.Buffer
			)
			snapshot := func() string {
				mu.Lock()
				defer mu.Unlock()
				return seen.String()
			}
			go func() {
				buf := make([]byte, 512)
				closed := false
				for {
					n, err := stdout.Read(buf)
					if n > 0 {
						mu.Lock()
						seen.Write(buf[:n])
						hit := strings.Contains(seen.String(), "generator:")
						mu.Unlock()
						if !closed && hit {
							closed = true
							close(reached)
						}
					}
					if err != nil {
						if !closed {
							close(reached)
						}
						return
					}
				}
			}()

			select {
			case <-reached:
			case <-time.After(60 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatalf("the guard never reached the generator; output so far:\n%s\n%s", snapshot(), stderr.String())
			}
			if !strings.Contains(snapshot(), "generator:") {
				_ = cmd.Process.Kill()
				t.Fatalf("the guard exited before generating, so this row proves nothing:\n%s\n%s", snapshot(), stderr.String())
			}
			// Let the generator get properly under way. The assertion on the
			// refusal text below is what actually proves it was running; this
			// only makes that assertion the common case rather than a race.
			time.Sleep(250 * time.Millisecond)

			if err := cmd.Process.Signal(s.sig); err != nil {
				t.Fatalf("signal the guard: %v", err)
			}

			waitErr := cmd.Wait()
			if waitErr == nil {
				t.Fatalf("the guard exited 0 after %s; it should refuse:\n%s", s.name, snapshot())
			}
			if got := exitCodeOf(t, waitErr); got != exitRefused {
				t.Fatalf("exit status %d after %s, want %d\n%s", got, s.name, exitRefused, stderr.String())
			}
			// The refusal must be the one that comes from cancelling a RUNNING
			// generator. "cancelled before generating" is a different, weaker
			// outcome, and a row that produced it would prove nothing about
			// interrupting mid-generation.
			if !strings.Contains(stderr.String(), "the generator failed") ||
				!strings.Contains(stderr.String(), "cancelled") {
				t.Fatalf("%s did not interrupt a RUNNING generator; the guard said:\n%s", s.name, stderr.String())
			}

			assertTreeUnchanged(t, before, treeDigests(t, dir), s.name+" during generation")

			// The private copy is removed on the way out, so an interrupted run
			// leaves nothing behind either.
			leftovers, err := os.ReadDir(tmp)
			if err != nil {
				t.Fatalf("read the guard's temporary directory: %v", err)
			}
			for _, l := range leftovers {
				t.Errorf("an interrupted run left its private copy behind: %s", l.Name())
			}
		})
	}
}

// TestTheBinaryReportsItsRefusalsAndExitCodes pins the command-line surface:
// what each verb and flag does, and which exit status each outcome produces.
func TestTheBinaryReportsItsRefusalsAndExitCodes(t *testing.T) {
	bin := buildGuard(t)
	dir := newGeneratableFixture(t)

	cases := []struct {
		name     string
		args     []string
		wantExit int
		wantText string
	}{
		{name: "no arguments prints usage", args: nil, wantExit: exitUsage, wantText: "gqlgen-guard runs gqlgen"},
		{name: "an unknown verb is refused", args: []string{"regenerate"}, wantExit: exitUsage, wantText: "unknown verb"},
		{name: "an unexpected positional argument is refused",
			args: []string{"check-drift", "extra"}, wantExit: exitUsage, wantText: "unexpected argument"},
		{name: "-update on generate is refused",
			args: []string{"generate", "-update"}, wantExit: exitUsage, wantText: "applies to check-drift"},
		{name: "an unknown flag is refused", args: []string{"check-drift", "-nope"}, wantExit: exitUsage, wantText: "flag provided but not defined"},
		{name: "a missing config is refused",
			args:     []string{"check-drift", "-module", dir, "-config", "absent.yml"},
			wantExit: exitRefused, wantText: "load gqlgen config"},
		{name: "a missing module is refused",
			args:     []string{"check-drift", "-module", filepath.Join(dir, "no-such-directory")},
			wantExit: exitRefused, wantText: "open module root"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(bin, tc.args...)
			cmd.Dir = dir
			var out bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &out
			err := cmd.Run()

			got := exitCodeOf(t, err)
			if got != tc.wantExit {
				t.Fatalf("exit status %d, want %d\noutput:\n%s", got, tc.wantExit, out.String())
			}
			if !strings.Contains(out.String(), tc.wantText) {
				t.Fatalf("output does not contain %q:\n%s", tc.wantText, out.String())
			}
		})
	}
}

func exitCodeOf(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	t.Fatalf("not an exit error: %v", err)
	return -1
}
