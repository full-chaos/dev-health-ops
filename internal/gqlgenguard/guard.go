package gqlgenguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// DefaultConfigPath is the gqlgen config this repository guards.
const DefaultConfigPath = "cmd/query-api/gqlgen.yml"

// DefaultDriftPath is the committed record of the differences between the
// checked-in generated files and a fresh generation.
const DefaultDriftPath = "contracts/gqlgen/v1/expected-drift.record"

// moduleFileNames are the files the generator may rewrite in its own copy
// without that being a finding. gqlgen runs `go mod tidy` unless the config
// disables it, and a tidy inside a throwaway copy is harmless -- it is
// reported, never copied back, and therefore cannot reach the real module.
var moduleFileNames = map[string]bool{"go.mod": true, "go.sum": true, "go.work": true, "go.work.sum": true}

// Options configures one guard invocation.
type Options struct {
	// ModuleDir is the module root -- the working tree that must not be
	// written to except by an explicit copy-back.
	ModuleDir string
	// ConfigPath is the module-relative gqlgen config. Empty means
	// DefaultConfigPath.
	ConfigPath string
	// DriftPath is the module-relative expected-drift record. Empty means
	// DefaultDriftPath.
	DriftPath string
	// Generator runs the code generator. Empty means the real gqlgen CLI.
	Generator Generator
	// TempParent is the directory the private copy is made under. Empty means
	// the system temporary directory.
	TempParent string
	// Report receives the guard's own progress and digest output.
	Report io.Writer
}

func (o *Options) configPath() string {
	if o.ConfigPath != "" {
		return o.ConfigPath
	}
	return DefaultConfigPath
}

func (o *Options) driftPath() string {
	if o.DriftPath != "" {
		return o.DriftPath
	}
	return DefaultDriftPath
}

func (o *Options) report() io.Writer {
	if o.Report != nil {
		return o.Report
	}
	return io.Discard
}

// Change records one expected output's digest on both sides.
type Change struct {
	Path string
	// TreeDigest is the hex sha256 of the checked-in file, or "" when the
	// working tree has no such file.
	TreeDigest string
	// GeneratedDigest is the hex sha256 of what the generator produced, or ""
	// when this run did not write the file at all.
	GeneratedDigest string
}

// Drifted reports whether the checked-in file and the generated one differ.
func (c Change) Drifted() bool { return c.TreeDigest != c.GeneratedDigest }

// Result is what one generation inside the private copy established.
type Result struct {
	Plan *Plan
	// Changes covers every planned output, in path order, whether or not it
	// drifted.
	Changes []Change
	// ModuleFileRewrites names the module files the generator rewrote inside
	// the copy. They are never copied back.
	ModuleFileRewrites []string
	// Record is the rendered expected-drift record for this generation.
	Record string
	// Applied names the outputs copied back into the working tree. It is
	// always empty for CheckDrift.
	Applied []string
	// DroppedLinks names every symbolic link in the working tree the private
	// copy does not carry, because it is absolute or does not resolve inside
	// the module. The generator never sees them.
	DroppedLinks []DroppedLink

	// copyDir is the private module copy this result was produced in. It is
	// unexported because it is valid only until the caller's cleanup runs.
	copyDir string
}

// Drifted reports whether any planned output differs from the tree.
func (r *Result) Drifted() bool {
	for _, c := range r.Changes {
		if c.Drifted() {
			return true
		}
	}
	return false
}

// CheckDrift generates into a private copy and compares the result with the
// working tree. It writes nothing to the working tree.
//
// A non-nil error is a refusal: either the generation could not be trusted
// (the generator failed, wrote outside its declared surface, or deleted a
// checked-in output) or the drift does not match the committed record.
func CheckDrift(ctx context.Context, opts Options) (*Result, error) {
	res, cleanup, err := generateIntoCopy(ctx, opts)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return nil, err
	}

	moduleRoot, err := os.OpenRoot(opts.ModuleDir)
	if err != nil {
		return nil, fmt.Errorf("open module root %q: %w", opts.ModuleDir, err)
	}
	defer moduleRoot.Close()

	committed, err := ReadThroughRoot(moduleRoot, opts.driftPath())
	if err != nil {
		if errNotExist(err) {
			return res, fmt.Errorf(
				"refusing: no expected-drift record at %q. Regenerate it with `gqlgen-guard check-drift -update` and review every line before committing it",
				opts.driftPath())
		}
		return nil, fmt.Errorf("read expected-drift record %q: %w", opts.driftPath(), err)
	}

	if bytes.Equal(committed, []byte(res.Record)) {
		fmt.Fprintf(opts.report(), "gqlgen drift matches %s exactly.\n", opts.driftPath())
		return res, nil
	}

	return res, fmt.Errorf("refusing: gqlgen drift does not match %s.\n%s",
		opts.driftPath(), describeRecordMismatch(string(committed), res.Record))
}

// UpdateDriftRecord rewrites the committed expected-drift record from a fresh
// generation. This is the ONE code path in this package that writes to the
// working tree outside `generate`, it is never used by CI, and a test asserts
// the CI step does not pass its flag.
func UpdateDriftRecord(ctx context.Context, opts Options) (*Result, error) {
	res, cleanup, err := generateIntoCopy(ctx, opts)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return nil, err
	}
	moduleRoot, err := os.OpenRoot(opts.ModuleDir)
	if err != nil {
		return nil, fmt.Errorf("open module root %q: %w", opts.ModuleDir, err)
	}
	defer moduleRoot.Close()

	// The record describes the tree the generation was checked against; if a
	// declared output moved meanwhile, the record would describe a tree that
	// no longer exists.
	if err := verifyTreeUnchanged(moduleRoot, res); err != nil {
		return nil, err
	}
	if err := moduleRoot.MkdirAll(path.Dir(opts.driftPath()), 0o755); err != nil {
		return nil, fmt.Errorf("create directory for %q: %w", opts.driftPath(), err)
	}
	if err := AtomicWrite(moduleRoot, opts.driftPath(), []byte(res.Record), 0o644); err != nil {
		return nil, err
	}
	res.Applied = []string{opts.driftPath()}
	fmt.Fprintf(opts.report(), "wrote %s (%d bytes)\n", opts.driftPath(), len(res.Record))
	return res, nil
}

// Generate regenerates the GraphQL layer and copies the outputs back into the
// working tree.
//
// Nothing reaches the tree unless the generator succeeded, every output it
// produced was one the configuration declared, and no checked-in output
// vanished. Each file is replaced atomically, through the module's own root
// handle, so a path that resolves outside the module cannot be written even if
// the configuration asks for one.
func Generate(ctx context.Context, opts Options) (*Result, error) {
	res, cleanup, err := generateIntoCopy(ctx, opts)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return nil, err
	}
	// There is deliberately no second cancellation check here. generateIntoCopy
	// already refuses on a cancelled context after the generator returns, so a
	// check at this point is unreachable -- proved by a mutant that removed it
	// and survived every test. A guard that cannot fail is not a guard.

	moduleRoot, err := os.OpenRoot(opts.ModuleDir)
	if err != nil {
		return nil, fmt.Errorf("open module root %q: %w", opts.ModuleDir, err)
	}
	defer moduleRoot.Close()

	copyRoot, err := os.OpenRoot(res.copyDir)
	if err != nil {
		return nil, fmt.Errorf("open module copy %q: %w", res.copyDir, err)
	}
	defer copyRoot.Close()

	// Every destination must still be exactly what the generation was checked
	// against -- the state the private copy was taken from. A file edited,
	// created, removed or swapped for a link while the generator ran is a
	// refusal of the WHOLE apply, before anything is written: overwriting it
	// would destroy work nobody checked.
	if err := verifyTreeUnchanged(moduleRoot, res); err != nil {
		return nil, err
	}

	// Past this line the tree is being changed. Signals are deliberately not
	// consulted again: the phase is a handful of renames of files already held
	// in memory, and stopping halfway would leave exactly the partial state
	// the copy-first design exists to prevent.
	for _, c := range res.Changes {
		if c.GeneratedDigest == "" || !c.Drifted() {
			continue
		}
		data, err := ReadThroughRoot(copyRoot, c.Path)
		if err != nil {
			return nil, fmt.Errorf("read generated %q: %w", c.Path, err)
		}
		if err := moduleRoot.MkdirAll(path.Dir(c.Path), 0o755); err != nil {
			return nil, fmt.Errorf("create directory for %q: %w", c.Path, err)
		}
		if err := AtomicWrite(moduleRoot, c.Path, data, 0o644); err != nil {
			return nil, err
		}
		res.Applied = append(res.Applied, c.Path)
	}

	w := opts.report()
	if len(res.Applied) == 0 {
		fmt.Fprintln(w, "no output changed; the working tree already matches a fresh generation.")
	}
	for _, p := range res.Applied {
		fmt.Fprintf(w, "updated %s\n", p)
	}
	return res, nil
}

// generateIntoCopy performs every step both verbs share: copy the module,
// enumerate the generator's declared output surface from its own config, run
// the generator inside the copy, and classify everything it touched.
//
// It returns a cleanup function that removes the private copy. The caller must
// call it; the copy is the only thing this function creates outside memory.
func generateIntoCopy(ctx context.Context, opts Options) (*Result, func(), error) {
	if opts.ModuleDir == "" {
		return nil, nil, errors.New("ModuleDir is required")
	}
	moduleAbs, err := filepath.Abs(opts.ModuleDir)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve module dir %q: %w", opts.ModuleDir, err)
	}

	moduleRoot, err := os.OpenRoot(moduleAbs)
	if err != nil {
		return nil, nil, fmt.Errorf("open module root %q: %w", moduleAbs, err)
	}
	defer moduleRoot.Close()

	// The private copy must be made OUTSIDE the module. The directory it is made
	// under (Options.TempParent, or TMPDIR through os.TempDir when that is
	// empty) and the module root are two inputs naming a location; when the
	// first is inside the second, copying the module copies the copy into
	// itself without end -- measured on the real binary before this check: 91
	// nested levels and 11 GB in nine minutes.
	if err := refuseCopyInsideModule(moduleAbs, opts.TempParent); err != nil {
		return nil, nil, err
	}

	copyDir, err := os.MkdirTemp(opts.TempParent, "gqlgen-guard-")
	if err != nil {
		return nil, nil, fmt.Errorf("create private module copy: %w", err)
	}
	// The generator's HOME and TMPDIR: beside the copy, never inside it (a
	// file there would be an undeclared output) and never inherited.
	scratchDir, err := os.MkdirTemp(opts.TempParent, "gqlgen-guard-env-")
	if err != nil {
		_ = os.RemoveAll(copyDir)
		return nil, nil, fmt.Errorf("create the generator's scratch directory: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(copyDir)
		_ = os.RemoveAll(scratchDir)
	}

	copyRoot, err := os.OpenRoot(copyDir)
	if err != nil {
		return nil, cleanup, fmt.Errorf("open private module copy %q: %w", copyDir, err)
	}
	defer copyRoot.Close()

	w := opts.report()
	fmt.Fprintf(w, "module: %s\n", moduleAbs)
	fmt.Fprintf(w, "private copy: %s\n", copyDir)

	dropped, err := CopyTree(ctx, moduleRoot, copyRoot, skipVCS)
	if err != nil {
		return nil, cleanup, fmt.Errorf("copy module into %q: %w", copyDir, err)
	}
	// Every link the copy does NOT carry is named, so the generator's view of
	// the module is observable: a schema or output that fails below because a
	// link was dropped shows its cause on the line above the failure.
	for _, d := range dropped {
		if d.Detail != "" {
			fmt.Fprintf(w, "dropped symbolic link %s (%s); the copy holds an inert self-loop in its place [%s]\n", d.Path, d.Reason, d.Detail)
			continue
		}
		fmt.Fprintf(w, "dropped symbolic link %s (%s); the copy holds an inert self-loop in its place\n", d.Path, d.Reason)
	}

	// The config is read from the COPY, which is byte-identical to the tree,
	// so the enumeration describes the file the generator is about to read.
	plan, err := EnumerateOutputs(copyDir, opts.configPath())
	if err != nil {
		return nil, cleanup, err
	}
	fmt.Fprintf(w, "config: %s (%d schema file(s), %d declared output path(s))\n",
		plan.ConfigPath, len(plan.Schemas), len(plan.Outputs))

	if err := refuseDiscoverableConfigs(copyRoot, plan); err != nil {
		return nil, cleanup, err
	}

	workDir := filepath.Join(copyDir, filepath.FromSlash(plan.ConfigDir))
	childEnv, err := childEnvironment(ctx, scratchDir, workDir, copyDir, moduleAbs, w)
	if err != nil {
		return nil, cleanup, err
	}
	if err := refuseReplacesOutsideTheCopy(ctx, copyRoot, copyDir, childEnv); err != nil {
		return nil, cleanup, err
	}

	if err := refuseHandWrittenCollisions(moduleRoot, plan, w); err != nil {
		return nil, cleanup, err
	}

	before, err := TakeSnapshotContext(ctx, copyRoot, skipVCS)
	if err != nil {
		return nil, cleanup, fmt.Errorf("snapshot private copy: %w", err)
	}

	gen := opts.Generator
	if gen == nil {
		gen = NewGoRunGenerator(w, w)
	}
	fmt.Fprintf(w, "generator: %s (cwd %s)\n", gen.Describe(), plan.ConfigDir)

	if err := gen.Generate(ctx, workDir, path.Base(plan.ConfigPath), childEnv); err != nil {
		return nil, cleanup, fmt.Errorf("refusing: the generator failed: %w (nothing was written to %s)", err, moduleAbs)
	}
	// No separate "cancelled after generating" check, and none after the copy
	// either: every walk here (the copy and both snapshots) checks ctx before
	// its first entry, so each explicit check that used to sit between them
	// was subsumed by the next walk's own first check. Mutant M17 removed one
	// and survived every test, which is how that was found; a guard that
	// cannot fail is not a guard.
	after, err := TakeSnapshotContext(ctx, copyRoot, skipVCS)
	if err != nil {
		return nil, cleanup, fmt.Errorf("snapshot private copy after generating: %w", err)
	}

	res := &Result{Plan: plan, DroppedLinks: dropped, copyDir: copyDir}

	planned := plan.PathSet()
	var unexpected []string
	for _, rel := range changedPaths(before, after) {
		if _, ok := planned[rel]; ok {
			continue
		}
		if moduleFileNames[path.Base(rel)] {
			res.ModuleFileRewrites = append(res.ModuleFileRewrites, rel)
			continue
		}
		unexpected = append(unexpected, rel)
	}
	if len(unexpected) > 0 {
		return nil, cleanup, fmt.Errorf(
			"refusing: the generator wrote %d path(s) its own configuration does not declare, so nothing can be copied back safely:\n  %s",
			len(unexpected), strings.Join(unexpected, "\n  "))
	}

	for _, o := range plan.Outputs {
		c := Change{Path: o.Path}
		if e, ok := before[o.Path]; ok {
			c.TreeDigest = e.Digest
		}
		if e, ok := after[o.Path]; ok {
			c.GeneratedDigest = e.Digest
		}
		if c.TreeDigest != "" && c.GeneratedDigest == "" {
			return nil, cleanup, fmt.Errorf(
				"refusing: the generator reported success but removed %q, which is checked in; copying that back would delete a committed file",
				o.Path)
		}
		res.Changes = append(res.Changes, c)
	}

	for _, rel := range res.ModuleFileRewrites {
		fmt.Fprintf(w, "note: the generator rewrote %s inside the private copy; the real module is untouched.\n", rel)
	}

	res.Record = renderRecord(res, plan, gen, moduleRoot, copyRoot)
	return res, cleanup, nil
}

// childEnvKeys is the WHOLE environment the generator child gets: an
// allowlist, built from nothing, never a filtered copy of the guard's own. A
// setting a future Go release adds cannot reach the child, because nothing is
// inherited that is not named here. Round 4 of review executed an inherited
// `GOFLAGS=-mod=mod -modfile=<tree>/x.mod` (and the same through a GOENV file)
// making the child write into the working tree while check-drift passed.
//
//	PATH        the directory of the go binary the guard resolved, only
//	HOME        a scratch directory beside the private copy
//	TMPDIR      a scratch directory beside the private copy
//	GOCACHE     the guard's effective build cache      } shared with the parent for
//	GOMODCACHE  the guard's effective module cache     } speed and offline use, each
//	GOPATH      the guard's effective GOPATH           } refused if inside the module
//	GOFLAGS     -mod=readonly
//	GOENV       off   (no go env file is read)
//	GOWORK      off   (no workspace redirects package loading)
//	GOTOOLCHAIN local (no other toolchain is fetched or run)
var childEnvKeys = []string{"PATH", "HOME", "TMPDIR", "GOCACHE", "GOMODCACHE", "GOPATH", "GOFLAGS", "GOENV", "GOWORK", "GOTOOLCHAIN"}

// sharedLocations are the three values the child takes from the guard's own
// go environment: where the build and module caches live.
var sharedLocations = []string{"GOPATH", "GOMODCACHE", "GOCACHE"}

// goBinary resolves the go command once, from the guard's PATH.
func goBinary() (string, error) {
	p, err := exec.LookPath("go")
	if err != nil {
		return "", fmt.Errorf("refusing: the go command is not on PATH: %w", err)
	}
	return resolvedPath(p), nil
}

// childEnvironment builds the generator's environment from the allowlist, and
// then ASSERTS it by asking the go command, in the directory the generator
// will run in and with exactly that environment, for its effective settings.
func childEnvironment(ctx context.Context, scratch, workDir, copyDir, moduleAbs string, w io.Writer) ([]string, error) {
	goBin, err := goBinary()
	if err != nil {
		return nil, err
	}
	copyReal := resolvedPath(copyDir)
	moduleReal := resolvedPath(moduleAbs)
	inside := func(p string) bool {
		r := resolvedPath(p)
		return within(moduleReal, r) || within(copyReal, r)
	}
	if inside(goBin) {
		return nil, fmt.Errorf("refusing: the go command resolves to %q, inside the module", goBin)
	}

	// The caches: the guard's EFFECTIVE values (environment and go env file
	// alike), read in the scratch directory so no module is in play.
	// GOTOOLCHAIN/GOWORK/GOFLAGS are pinned for this one query too, so the
	// guard's own lookup cannot switch toolchains or be redirected; the caches
	// it reports come from the environment and the go env file as the user set
	// them. (exec keeps the LAST value of a duplicated key.)
	parent := append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOFLAGS=")
	shared, err := goEnv(ctx, goBin, scratch, parent, sharedLocations...)
	if err != nil {
		return nil, err
	}
	for _, key := range sharedLocations {
		for _, v := range filepath.SplitList(shared[key]) {
			if v != "" && inside(v) {
				return nil, fmt.Errorf("refusing: %s=%q is inside the module, so the go command would write there while the generator runs", key, v)
			}
		}
	}

	home := filepath.Join(scratch, "home")
	tmp := filepath.Join(scratch, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("create the generator's scratch %q: %w", d, err)
		}
	}
	// Go telemetry is OFF in the scratch HOME. Its default ("local") has the
	// go command write counters under $HOME/.config/go/telemetry from a
	// process that can outlive the command -- measured: files appearing in the
	// scratch directory after the guard had returned and removed it. "off" is
	// the mode file's documented value (x/telemetry internal/telemetry dir.go).
	modeFile := filepath.Join(home, ".config", "go", "telemetry", "mode")
	if err := os.MkdirAll(filepath.Dir(modeFile), 0o700); err != nil {
		return nil, fmt.Errorf("create the generator's telemetry config: %w", err)
	}
	if err := os.WriteFile(modeFile, []byte("off\n"), 0o600); err != nil {
		return nil, fmt.Errorf("write the generator's telemetry mode: %w", err)
	}
	env := []string{
		"PATH=" + filepath.Dir(goBin),
		"HOME=" + home,
		"TMPDIR=" + tmp,
		"GOCACHE=" + shared["GOCACHE"],
		"GOMODCACHE=" + shared["GOMODCACHE"],
		"GOPATH=" + shared["GOPATH"],
		"GOFLAGS=-mod=readonly",
		"GOENV=off",
		"GOWORK=off",
		"GOTOOLCHAIN=local",
	}

	// The executed assertion: what the go command itself will do with it.
	eff, err := goEnv(ctx, goBin, workDir, env, "GOMOD", "GOFLAGS", "GOENV", "GOWORK", "GOTOOLCHAIN", "GOMODCACHE", "GOCACHE", "GOTMPDIR")
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(w, "generator env (allowlist %s; nothing inherited): GOMOD=%s GOFLAGS=%s GOENV=%q GOWORK=%s GOTOOLCHAIN=%s\n",
		strings.Join(childEnvKeys, ","), eff["GOMOD"], eff["GOFLAGS"], eff["GOENV"], eff["GOWORK"], eff["GOTOOLCHAIN"])
	if err := checkChildGoEnv(eff, copyReal, inside); err != nil {
		return nil, err
	}
	return env, nil
}

// checkChildGoEnv is the assertion over the go command's effective settings
// under the child's environment: the main module is the private copy's (or
// none), the fixed values took effect, and nothing it writes lands inside the
// module. It is a separate function so its own input domain can be executed
// (G15): under the allowlist no input of the guard can make these values
// wrong, so without that table the assertion could not be shown to fail.
func checkChildGoEnv(eff map[string]string, copyReal string, inside func(string) bool) error {
	if m := eff["GOMOD"]; m != "" && m != os.DevNull && !within(copyReal, resolvedPath(m)) {
		return fmt.Errorf("refusing: the go command resolves the main module to %q, outside the private copy", m)
	}
	// `go env GOENV` reports "" when GOENV=off (measured, go1.27), a path otherwise.
	if eff["GOFLAGS"] != "-mod=readonly" || (eff["GOENV"] != "" && eff["GOENV"] != "off") || eff["GOWORK"] != "off" || !strings.HasPrefix(eff["GOTOOLCHAIN"], "local") {
		return fmt.Errorf("refusing: the go command does not honour the generator's environment (GOFLAGS=%q GOENV=%q GOWORK=%q GOTOOLCHAIN=%q)", eff["GOFLAGS"], eff["GOENV"], eff["GOWORK"], eff["GOTOOLCHAIN"])
	}
	for _, key := range []string{"GOMODCACHE", "GOCACHE", "GOTMPDIR"} {
		if v := eff[key]; v != "" && inside(v) {
			return fmt.Errorf("refusing: the generator's effective %s=%q is inside the module", key, v)
		}
	}
	return nil
}

func goEnv(ctx context.Context, goBin, dir string, env []string, keys ...string) (map[string]string, error) {
	cmd := exec.CommandContext(ctx, goBin, append([]string{"env", "-json"}, keys...)...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return nil, fmt.Errorf("refusing: could not read the go command's effective environment: %w: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, fmt.Errorf("refusing: could not read the go command's effective environment: %w", err)
	}
	var m map[string]string
	if err := json.Unmarshal(out, &m); err != nil {
		return nil, fmt.Errorf("refusing: could not parse `go env -json`: %w", err)
	}
	return m, nil
}

// goModEdit is the part of `go mod edit -json` this package reads.
type goModEdit struct {
	Replace []struct {
		Old struct{ Path, Version string }
		New struct{ Path, Version string }
	}
}

// refuseReplacesOutsideTheCopy refuses a filesystem `replace` in the copy's
// go.mod that leaves the copy or does not resolve: the generator loads that
// directory's packages to bind models, so it is an input like the schema.
// Version replaces resolve through the module cache and go.sum, like any
// dependency. The go command parses go.mod (`go mod edit -json`), not a regexp.
func refuseReplacesOutsideTheCopy(ctx context.Context, copyRoot *os.Root, copyDir string, env []string) error {
	if _, err := copyRoot.Lstat("go.mod"); err != nil {
		if errNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat go.mod: %w", err)
	}
	goBin, err := goBinary()
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, goBin, "mod", "edit", "-json")
	cmd.Dir = copyDir
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("refusing: could not read go.mod with `go mod edit -json`: %w", err)
	}
	var mod goModEdit
	if err := json.Unmarshal(out, &mod); err != nil {
		return fmt.Errorf("refusing: could not parse `go mod edit -json`: %w", err)
	}
	copyReal := resolvedPath(copyDir)
	for _, r := range mod.Replace {
		if r.New.Version != "" {
			continue
		}
		p := r.New.Path
		if !filepath.IsAbs(p) {
			p = filepath.Join(copyDir, p)
		}
		p = filepath.Clean(p)
		if !within(copyReal, p) && !within(copyDir, p) {
			return fmt.Errorf("refusing: go.mod replaces %s with %q, a directory outside the module; the generator would load packages from it", r.Old.Path, r.New.Path)
		}
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("refusing: go.mod replaces %s with %q, which cannot be resolved inside the module (%v)", r.Old.Path, r.New.Path, err)
		}
	}
	return nil
}

func resolvedPath(p string) string {
	a, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	if r, err := filepath.EvalSymlinks(a); err == nil {
		return r
	}
	return a
}

// within reports whether p is root or beneath it (lexically, both absolute).
func within(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// discoveryConfigNames are the names gqlgen's own config discovery tries, in
// the working directory and then every parent (gqlgen v0.17.66
// codegen/config/config.go: cfgFilenames, findCfg). A test reads them back out
// of the gqlgen source this module requires.
var discoveryConfigNames = []string{".gqlgen.yml", "gqlgen.yml", "gqlgen.yaml"}

// refuseDiscoverableConfigs refuses a private copy holding any file gqlgen's
// discovery would find -- beside the validated config or in any directory
// above it up to the module root -- other than the validated config itself.
// The generator is handed the validated file with --config, so discovery does
// not run; this is the second wall, so that a config other than the one the
// guard checked cannot be selected by any invocation at all.
func refuseDiscoverableConfigs(copyRoot *os.Root, plan *Plan) error {
	dir := path.Clean(plan.ConfigDir)
	for {
		for _, name := range discoveryConfigNames {
			rel := path.Join(dir, name)
			if rel == plan.ConfigPath {
				continue
			}
			if _, err := copyRoot.Lstat(rel); err == nil {
				return fmt.Errorf(
					"refusing: %q is a config gqlgen's own discovery would find (it tries %s in %q and every parent); only the validated %q may be present on that path",
					name, strings.Join(discoveryConfigNames, ", "), dir, plan.ConfigPath)
			} else if !errNotExist(err) {
				return fmt.Errorf("stat %q: %w", rel, err)
			}
		}
		if dir == "." {
			return nil
		}
		dir = path.Dir(dir)
	}
}

// verifyTreeUnchanged re-reads every declared output in the working tree and
// refuses if any is no longer in the state the private copy was taken from:
// the same bytes, or still absent.
func verifyTreeUnchanged(moduleRoot *os.Root, res *Result) error {
	for _, c := range res.Changes {
		now, err := treeState(moduleRoot, c.Path)
		if err != nil {
			return err
		}
		if now != c.TreeDigest {
			return fmt.Errorf(
				"refusing: %q changed in the working tree while the generator ran (checked %s, now %s); nothing was written",
				c.Path, stateLabel(c.TreeDigest), stateLabel(now))
		}
	}
	return nil
}

func treeState(root *os.Root, rel string) (string, error) {
	info, err := root.Lstat(rel)
	if err != nil {
		if errNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("stat %q: %w", rel, err)
	}
	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := root.Readlink(rel)
		if err != nil {
			return "", fmt.Errorf("read symlink %q: %w", rel, err)
		}
		return "symlink:" + target, nil
	case info.Mode().IsRegular():
		d, _, err := digestFile(root, rel)
		return d, err
	default:
		return "not-a-file:" + info.Mode().String(), nil
	}
}

func stateLabel(s string) string {
	switch {
	case s == "":
		return "absent"
	case strings.HasPrefix(s, "symlink:"), strings.HasPrefix(s, "not-a-file:"):
		return s
	default:
		return "sha256 " + s[:12]
	}
}

// refuseCopyInsideModule refuses a temporary parent that resolves to the module
// root or anywhere beneath it. Both sides are resolved through symbolic links,
// so an alias of an in-module directory is caught too.
func refuseCopyInsideModule(moduleAbs, tempParent string) error {
	parent := tempParent
	if parent == "" {
		parent = os.TempDir()
	}
	parentAbs, err := filepath.Abs(parent)
	if err != nil {
		return fmt.Errorf("resolve the private copy's parent %q: %w", parent, err)
	}
	if real, err := filepath.EvalSymlinks(parentAbs); err == nil {
		parentAbs = real
	}
	moduleReal := moduleAbs
	if real, err := filepath.EvalSymlinks(moduleAbs); err == nil {
		moduleReal = real
	}
	rel, err := filepath.Rel(moduleReal, parentAbs)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf(
			"refusing: the private copy would be made inside the module (%s is under %s), and copying the module would copy the copy into itself. Point TMPDIR outside the module",
			parentAbs, moduleReal)
	}
	return nil
}

// refuseHandWrittenCollisions refuses a configuration whose declared outputs
// would overwrite a file in the tree that gqlgen did not write.
//
// This is the class where a single edit to `exec.filename` silently makes a
// hand-written resolver a generator target. The evidence is the provenance
// notice gqlgen itself writes -- checked at the position and in the shape
// gqlgen writes it for the section that declares the output (see
// provenance.go), never as words appearing somewhere in the file. Every
// decision, accepted or refused, is written to w.
func refuseHandWrittenCollisions(moduleRoot *os.Root, plan *Plan, w io.Writer) error {
	for _, o := range plan.Outputs {
		info, err := moduleRoot.Lstat(o.Path)
		if err != nil {
			if errNotExist(err) {
				fmt.Fprintf(w, "provenance: %s: absent; the generator will create it\n", o.Path)
				continue
			}
			return fmt.Errorf("stat declared output %q: %w", o.Path, err)
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf(
				"refusing: %s output %q is a symbolic link in the working tree; the generator would follow it and write through the link",
				o.Declaree, o.Path)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf(
				"refusing: %s output %q exists in the working tree and is not a regular file (%s)",
				o.Declaree, o.Path, info.Mode())
		}
		shape, ok, reason, err := provenance(moduleRoot, o.Path, o.Declaree)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf(
				"refusing: the %s section declares %q as an output, but nothing proves gqlgen wrote that file, so it is treated as hand-written -- %s. "+
					"Regenerating would overwrite it. Fix the config, or if the file really is generated with omit_gqlgen_file_notice set, remove that setting so provenance is visible",
				o.Declaree, o.Path, reason)
		}
		fmt.Fprintf(w, "provenance: %s: %s\n", o.Path, shape)
	}
	return nil
}

// changedPaths returns every path whose presence or content differs between
// two snapshots, in sorted order.
func changedPaths(before, after Snapshot) []string {
	seen := map[string]bool{}
	var out []string
	for rel, b := range before {
		a, ok := after[rel]
		if !ok || a.Digest != b.Digest {
			if !seen[rel] {
				seen[rel] = true
				out = append(out, rel)
			}
		}
	}
	for rel, a := range after {
		b, ok := before[rel]
		if !ok || b.Digest != a.Digest {
			if !seen[rel] {
				seen[rel] = true
				out = append(out, rel)
			}
		}
	}
	sort.Strings(out)
	return out
}
