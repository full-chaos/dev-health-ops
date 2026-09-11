package gqlgenguard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
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
	cleanup := func() { _ = os.RemoveAll(copyDir) }

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

	if err := gen.Generate(ctx, filepath.Join(copyDir, filepath.FromSlash(plan.ConfigDir))); err != nil {
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
