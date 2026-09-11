package gqlgenguard

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// No test in this package calls t.Parallel.
//
// EnumerateOutputs moves the process working directory, because that is how
// gqlgen resolves a config's relative paths and re-deriving that resolution is
// the mistake this package exists to remove. A parallel test that read the
// working directory, or a second enumeration, would see the moved value.
// Serial execution is the price of using gqlgen's own resolution rather than a
// copy of it.

// repoRoot returns the module root this test binary was built from.
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

// fixture is a throwaway module a test generates into.
type fixture struct {
	dir string
	t   *testing.T
}

// newFixture writes files into a fresh temporary directory.
//
// Every path is relative and every parent directory is created. The returned
// directory is the module root as far as the guard is concerned.
func newFixture(t *testing.T, files map[string]string) *fixture {
	t.Helper()
	dir := t.TempDir()
	// t.TempDir can hand back a path through a symbolic link (/tmp -> /private/tmp
	// on some systems, and the harness's own TMPDIR here). Resolving it once
	// means a test's expected paths and the guard's resolved paths agree.
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	f := &fixture{dir: resolved, t: t}
	for rel, body := range files {
		f.write(rel, body)
	}
	return f
}

// withModuleFiles copies this repository's go.mod, go.sum and tools.go into the
// fixture, which is the minimum that makes `go run github.com/99designs/gqlgen`
// resolvable. tools.go is what puts the generator's own CLI dependencies in
// go.sum; without it the command cannot start.
func (f *fixture) withModuleFiles() *fixture {
	f.t.Helper()
	root := repoRoot(f.t)
	for _, name := range []string{"go.mod", "go.sum", "tools.go"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			f.t.Fatalf("read %s from the repository root: %v", name, err)
		}
		f.write(name, string(data))
	}
	return f
}

func (f *fixture) write(rel, body string) {
	f.t.Helper()
	full := filepath.Join(f.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		f.t.Fatalf("create directory for %s: %v", rel, err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		f.t.Fatalf("write %s: %v", rel, err)
	}
}

func (f *fixture) read(rel string) string {
	f.t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, filepath.FromSlash(rel)))
	if err != nil {
		f.t.Fatalf("read %s: %v", rel, err)
	}
	return string(data)
}

func (f *fixture) exists(rel string) bool {
	_, err := os.Lstat(filepath.Join(f.dir, filepath.FromSlash(rel)))
	return err == nil
}

// digests returns a content digest for every file in the fixture, which is how
// every "the tree was not touched" assertion in this package is made.
func (f *fixture) digests() map[string]string {
	f.t.Helper()
	root, err := os.OpenRoot(f.dir)
	if err != nil {
		f.t.Fatalf("open fixture root: %v", err)
	}
	defer root.Close()
	snap, err := TakeSnapshot(root, skipVCS)
	if err != nil {
		f.t.Fatalf("snapshot fixture: %v", err)
	}
	out := make(map[string]string, len(snap))
	for rel, e := range snap {
		out[rel] = e.Digest
	}
	return out
}

// assertUnchanged fails with the specific paths that moved, so a failure names
// the file rather than only reporting that something changed.
func assertUnchanged(t *testing.T, before, after map[string]string, context string) {
	t.Helper()
	var problems []string
	for rel, b := range before {
		a, ok := after[rel]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s was deleted", rel))
			continue
		}
		if a != b {
			problems = append(problems, fmt.Sprintf("%s changed: %s -> %s", rel, b, a))
		}
	}
	for rel := range after {
		if _, ok := before[rel]; !ok {
			problems = append(problems, fmt.Sprintf("%s was created", rel))
		}
	}
	if len(problems) > 0 {
		t.Fatalf("%s: the tree was modified:\n  %s", context, strings.Join(problems, "\n  "))
	}
}

// fakeGenerator drives the guard directly instead of through a real generator,
// so a test can produce exactly the situation it is about: a failure, an extra
// file, a deleted output, a module tidy. Every fake records that it ran, so a
// test can tell "the generator did nothing" from "the generator was never
// called" -- a distinction a pass/fail result alone cannot make.
type fakeGenerator struct {
	name         string
	ran          bool
	ignoreCancel bool
	// env is the environment the guard handed the generator.
	env []string
	fn  func(workDir string) error
}

func (g *fakeGenerator) Generate(ctx context.Context, workDir, configFile string, env []string) error {
	g.ran = true
	g.env = env
	if err := ctx.Err(); err != nil && !g.ignoreCancel {
		return err
	}
	if g.fn == nil {
		return nil
	}
	return g.fn(workDir)
}

// ignoresCancellation makes the fake behave like a generator that does not
// watch its context: it runs to completion and reports success even after the
// context is cancelled. Without it, every cancellation test exercises only the
// "the generator itself failed" path, and the guard's own post-generation
// refusal is never reached.
func (g *fakeGenerator) ignoresCancellation() *fakeGenerator {
	g.ignoreCancel = true
	return g
}

func (g *fakeGenerator) Describe() string {
	if g.name == "" {
		return "fake generator"
	}
	return g.name
}

// writeIn writes a file inside the private copy. Every fixture in this package
// puts its gqlgen config at the module root, so the generator's working
// directory IS the copy root and a relative path here is a module-relative one.
func writeIn(workDir, rel, body string) error {
	full := filepath.Join(workDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	return os.WriteFile(full, []byte(body), 0o644)
}

// goAvailable reports whether the go command can be executed, so a test that
// needs the real generator says why it cannot run rather than failing opaquely.
func goAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Fatalf("the go command is required to run the real generator: %v", err)
	}
}

const fixtureSchema = `type Query {
  hello(name: String): String!
  item: Item
}

type Item {
  key: String!
  value: Float
}
`

const fixtureSchemaExtra = `extend type Query {
  farewell: String
}
`

// cellID names one input-domain cell so that it is its own -run selector:
// `go test -run 'TestX/G2-07' ./internal/gqlgenguard/` executes that cell alone.
func cellID(table string, i int) string { return fmt.Sprintf("%s-%02d", table, i+1) }

// logCell records what the guard actually DID for a cell -- the refusal text,
// or what it accepted -- as one CELL-OUTPUT line, so the body's domain tables
// quote the executed output rather than only a pass/fail colour.
func logCell(t *testing.T, err error, accepted string) {
	t.Helper()
	if err != nil {
		t.Logf("CELL-OUTPUT: refused: %s", strings.ReplaceAll(err.Error(), "\n", " / "))
		return
	}
	t.Logf("CELL-OUTPUT: accepted: %s", accepted)
}

// vettedGoTool is the resolved go command for a test that needs one directly.
// Production resolves it once per run; a test that calls goEnv or builds a
// generator needs the same value.
func vettedGoTool(t *testing.T) GoTool {
	t.Helper()
	tool, err := ResolveGoTool("")
	if err != nil {
		t.Fatal(err)
	}
	return tool
}

// writeOptions is guardOptions for a cell that drives the WRITING verb and is
// not about the expected-drift record.
//
// `generate` fails closed on the record STATE: with no record it cannot say
// which of the files it is about to overwrite hold deliberate hand-edits, so it
// refuses. These fixtures carry no record because the cell is about the link
// policy, copy-back, a config key or the file mode -- so they say what the
// operator would say, and pass the documented override. The record's own cells
// never use this: weakening the refusal to fit a fixture is how the hole stayed
// open, since five unrelated cells silently depended on generate succeeding
// without a record.
func writeOptions(f *fixture, gen Generator) Options {
	o := guardOptions(f, gen)
	o.RevertRecorded = true
	return o
}
