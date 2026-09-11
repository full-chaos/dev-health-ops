package gqlgenguard

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
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
	// GoTool is the go command this run uses, resolved once by the caller
	// before anything ran. The zero value makes the guard resolve and confine
	// it itself.
	GoTool GoTool
	// RevertRecorded allows Generate to overwrite outputs the expected-drift
	// record describes as deliberate hand-edits. Without it Generate refuses
	// rather than destroying recorded state.
	RevertRecorded bool
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

	committed, err := readTheRecord(moduleRoot, opts.driftPath())
	if err != nil {
		return res, err
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
	// The other half of the same posture: a destination that already holds
	// something this tool did not write is never replaced. -update refreshes a
	// RECORD; pointed at any other tracked file it used to overwrite it and say
	// `wrote <path>`.
	if err := refuseRecordDestinationThatIsNotARecord(moduleRoot, opts.driftPath()); err != nil {
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
	// check at this point is unreachable, and a guard that cannot fail is not a
	// guard.

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

	// A write verb does not silently destroy recorded state, and being unable
	// to READ the record is not permission to overwrite -- it is the state in
	// which the guard knows least. The posture belongs to the record STATE, not
	// to the verb: before anything is written, the record must be there, parse,
	// and carry a digest line for every file about to be overwritten.
	// -revert-recorded is the one override, and it is the operator saying so.
	reverts, err := recordedRevertsOf(moduleRoot, opts.driftPath(), res)
	if err != nil {
		return nil, err
	}
	w := opts.report()
	if !opts.RevertRecorded {
		// The most specific refusal first: the record NAMES these as deliberate
		// and shows all three digests. Only when it has nothing to say about
		// what is being overwritten does the state check speak.
		if len(reverts) > 0 {
			return nil, fmt.Errorf(
				"refusing: this would revert %d hand-edit(s) that %s records as deliberate; nothing was written:\n%s\nRe-run with -revert-recorded to replace them, then refresh the record with `gqlgen-guard check-drift -update`",
				len(reverts), opts.driftPath(), describeReverts(reverts))
		}
		if err := refuseUnlessTheRecordCoversTheWrites(moduleRoot, opts.driftPath(), res); err != nil {
			return nil, err
		}
	} else if len(reverts) > 0 {
		fmt.Fprintf(w, "reverting %d recorded hand-edit(s) (-revert-recorded); %s describes them as deliberate and will be stale afterwards:\n%s",
			len(reverts), opts.driftPath(), describeReverts(reverts))
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
		// The mode comes from the file being copied back, not from a constant:
		// the copy carries the tree's own mode (CopyTree) and the generator
		// rewrites in place, so an executable or group-writable output keeps
		// what it had instead of being normalised to 0644 by regenerating.
		info, err := copyRoot.Lstat(c.Path)
		if err != nil {
			return nil, fmt.Errorf("stat generated %q: %w", c.Path, err)
		}
		if err := moduleRoot.MkdirAll(path.Dir(c.Path), 0o755); err != nil {
			return nil, fmt.Errorf("create directory for %q: %w", c.Path, err)
		}
		if err := AtomicWrite(moduleRoot, c.Path, data, info.Mode().Perm()); err != nil {
			return nil, err
		}
		res.Applied = append(res.Applied, c.Path)
	}

	if len(res.Applied) == 0 {
		fmt.Fprintln(w, "no output changed; the working tree already matches a fresh generation.")
	}
	for _, p := range res.Applied {
		fmt.Fprintf(w, "updated %s\n", p)
	}
	return res, nil
}

// readTheRecord reads the committed expected-drift record. Both verbs go
// through it, so "there is no record" is ONE refusal with one wording: the
// reading verb refused it from the start, and the writing verb treating the
// same state as permission to overwrite was the hole.
func readTheRecord(moduleRoot *os.Root, driftPath string) ([]byte, error) {
	committed, err := ReadThroughRoot(moduleRoot, driftPath)
	if err != nil {
		if errNotExist(err) {
			return nil, fmt.Errorf(
				"refusing: no expected-drift record at %q. Regenerate it with `gqlgen-guard check-drift -update` and review every line before committing it",
				driftPath)
		}
		return nil, fmt.Errorf("read expected-drift record %q: %w", driftPath, err)
	}
	return committed, nil
}

// overwrittenByThisRun lists the declared outputs this result would write over
// -- present in the tree, drifted, and produced by this generation. A file the
// generator creates is not one of them: there is nothing there to destroy.
func overwrittenByThisRun(res *Result) []string {
	var out []string
	for _, c := range res.Changes {
		if c.GeneratedDigest != "" && c.Drifted() && c.TreeDigest != "" {
			out = append(out, c.Path)
		}
	}
	return out
}

// refuseUnlessTheRecordCoversTheWrites is the fail-closed check a writing run
// makes about the record STATE: present, parsing, and carrying a digest line
// for every file it is about to overwrite. A record that is absent, empty,
// mistyped or stripped of its digest table cannot say which of those files hold
// deliberate hand-edits, and a run that cannot tell must not write.
func refuseUnlessTheRecordCoversTheWrites(moduleRoot *os.Root, driftPath string, res *Result) error {
	overwritten := overwrittenByThisRun(res)
	if len(overwritten) == 0 {
		return nil
	}
	committed, err := readTheRecord(moduleRoot, driftPath)
	if err != nil {
		return err
	}
	recorded := digestLines(string(committed))
	if len(recorded) == 0 {
		return fmt.Errorf(
			"refusing: the expected-drift record %q carries no digest line, so it cannot say whether these %d file(s) hold deliberate hand-edits; nothing was written:\n%s%s",
			driftPath, len(overwritten), indentPaths(overwritten), recordAdvice)
	}
	var uncovered, stale []string
	tree := map[string]string{}
	for _, c := range res.Changes {
		tree[c.Path] = c.TreeDigest
	}
	for _, p := range overwritten {
		line, ok := recorded[p]
		if !ok {
			uncovered = append(uncovered, p)
			continue
		}
		// A digest line that describes OTHER bytes than the file now holds
		// does not describe this file: an edit made since the last -update is
		// exactly the hand-edit the record exists to protect, and it is the
		// one the record cannot see.
		if recordedTreeDigest(line) != tree[p] {
			stale = append(stale, fmt.Sprintf("%s\n    recorded  %s\n    in the tree %s", p, recordedTreeDigest(line), tree[p]))
		}
	}
	if len(uncovered) > 0 {
		return fmt.Errorf(
			"refusing: the expected-drift record %q has no digest line for %d file(s) this run would overwrite, so it cannot say whether they hold deliberate hand-edits; nothing was written:\n%s%s",
			driftPath, len(uncovered), indentPaths(uncovered), recordAdvice)
	}
	if len(stale) > 0 {
		return fmt.Errorf(
			"refusing: the expected-drift record %q describes other bytes than %d file(s) this run would overwrite now hold, so an edit made since the record was written would be reverted unseen; nothing was written:\n%s%s",
			driftPath, len(stale), indentPaths(stale), recordAdvice)
	}
	return nil
}

const recordAdvice = "Refresh it with `gqlgen-guard check-drift -update`, or pass -revert-recorded to overwrite them anyway."

func indentPaths(paths []string) string {
	var b strings.Builder
	for _, p := range paths {
		fmt.Fprintf(&b, "  %s\n", p)
	}
	return b.String()
}

// recordMarker is the first line of recordHeader: the shortest thing that says
// a file is this tool's record. The REST of the header is part of the bytes
// check-drift compares and may change between versions of this tool; the marker
// must not, or -update could no longer refresh a record an older build wrote.
var recordMarker = []byte(recordHeader[:strings.IndexByte(recordHeader, '\n')+1])

// refuseRecordDestinationThatIsNotARecord refuses to replace a file that is not
// an expected-drift record. -update is the one path in this package that writes
// to the tree outside `generate`, and its destination is operator-supplied: any
// tracked file could be named, and every one of them was overwritten.
func refuseRecordDestinationThatIsNotARecord(moduleRoot *os.Root, driftPath string) error {
	existing, err := ReadThroughRoot(moduleRoot, driftPath)
	if err != nil {
		if errNotExist(err) {
			return nil // nothing is there to destroy
		}
		return fmt.Errorf("read the destination of the expected-drift record %q: %w", driftPath, err)
	}
	if bytes.HasPrefix(existing, recordMarker) {
		return nil
	}
	return fmt.Errorf(
		"refusing: %q already holds a file that is not an expected-drift record (it does not begin with %q), and -update replaces a record rather than overwriting a file; nothing was written",
		driftPath, strings.TrimRight(string(recordMarker), "\n"))
}

// recordedRevert is one output Generate would overwrite that the committed
// expected-drift record describes as a deliberate hand-edit.
type recordedRevert struct {
	Path string
	// Recorded is the tree digest the record carries for Path.
	Recorded string
	// Tree is the digest the working tree has now. It differs from Recorded
	// only when the record is stale -- which makes the hand-edits MORE at
	// risk, not less, so it never softens the refusal.
	Tree string
	// Generated is the digest of what this run produced and would write.
	Generated string
}

// recordedRevertsOf lists the outputs this result would write over that the
// committed record marks as drift. A missing record means nothing is recorded
// and nothing can be destroyed; an unreadable one is an error, not a silent
// permission to overwrite.
func recordedRevertsOf(moduleRoot *os.Root, driftPath string, res *Result) ([]recordedRevert, error) {
	committed, err := ReadThroughRoot(moduleRoot, driftPath)
	if err != nil {
		if errNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read expected-drift record %q: %w", driftPath, err)
	}
	recorded := digestLines(string(committed))
	var out []recordedRevert
	for _, c := range res.Changes {
		if c.GeneratedDigest == "" || !c.Drifted() {
			continue
		}
		line, ok := recorded[c.Path]
		// digestLines yields "<status> <path> tree=<d> generated=<d>". A
		// recorded DRIFT is a hand-edit; whether the tree still holds exactly
		// the recorded bytes decides nothing, because regenerating reverts
		// whatever is there either way.
		if !ok || !strings.HasPrefix(line, "drift ") {
			continue
		}
		out = append(out, recordedRevert{
			Path:      c.Path,
			Recorded:  recordedTreeDigest(line),
			Tree:      c.TreeDigest,
			Generated: c.GeneratedDigest,
		})
	}
	return out, nil
}

// recordedTreeDigest reads the tree= field out of a record's digest line.
func recordedTreeDigest(line string) string {
	for _, f := range strings.Fields(line) {
		if d, ok := strings.CutPrefix(f, "tree="); ok {
			return d
		}
	}
	return ""
}

func describeReverts(rs []recordedRevert) string {
	var b strings.Builder
	for _, r := range rs {
		fmt.Fprintf(&b, "  %s\n    recorded  %s\n    in the tree %s\n    generated %s\n", r.Path, r.Recorded, r.Tree, r.Generated)
	}
	return b.String()
}

// testStageHook, when set (tests only), runs at named points between a check
// and the writes it guards, so a test can change the filesystem exactly there:
// "copy-parent-checked" (the copy's parent passed its check, nothing created),
// "copy-parent-opened" (the parent is open, its location not yet read back),
// "scratch-created" (the generator's scratch directory exists, nothing written
// in it), "copy-created" (the private copy exists, nothing copied into it),
// "before-generator" (every check has passed; dir is "<copy>\x00<scratch>").
var testStageHook func(stage, dir string)

func stageHook(stage, dir string) {
	if testStageHook != nil {
		testStageHook(stage, dir)
	}
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

	// The go command for this whole run, resolved and confined BEFORE anything
	// is executed. A caller that already resolved it (the command, which had to
	// run `go list -m` to find this module) passes it in; it is confined again
	// here against the root actually in use, which -module may have moved.
	tool := opts.GoTool
	if tool.Path() == "" {
		if tool, err = ResolveGoTool(moduleAbs); err != nil {
			return nil, nil, err
		}
	} else if tool, err = tool.Confine(moduleAbs); err != nil {
		return nil, nil, err
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
	parentPhys, err := refuseCopyInsideModule(moduleAbs, opts.TempParent)
	if err != nil {
		return nil, nil, err
	}
	stageHook("copy-parent-checked", parentPhys)

	// Every write the guard makes outside the tree goes through an os.Root
	// opened on the copy's parent: the check above names a PATH, and a link
	// swapped into that path afterwards would move every later write with it.
	// The root is a handle on the directory itself -- nothing done to a path
	// afterwards moves it -- and where the kernel says that directory is, is
	// checked once more; the private copy and the generator's scratch are
	// created and written only through it (and roots opened beneath it, which
	// refuse any link that leads out of them).
	parentRoot, err := os.OpenRoot(parentPhys)
	if err != nil {
		return nil, nil, fmt.Errorf("open the private copy's parent %q: %w", parentPhys, err)
	}
	// The window rootLocation exists to close: the directory is OPEN, and its
	// path has not been read back yet. A test plants an escape here, where
	// only an answer read from the open handle itself can still be right.
	stageHook("copy-parent-opened", parentPhys)
	parentLoc, err := rootLocation(parentRoot)
	if err != nil {
		parentRoot.Close()
		return nil, nil, err
	}
	if within(resolvedPath(moduleAbs), parentLoc) {
		parentRoot.Close()
		return nil, nil, fmt.Errorf("refusing: the private copy's parent %s is physically inside the module (the kernel resolves the opened directory to %s). Point TMPDIR outside the module", parentPhys, parentLoc)
	}
	sweepStaleCopies(parentRoot, parentLoc, opts.report())

	copyName, removeCopy, err := tempDirIn(parentRoot, "gqlgen-guard-")
	if err != nil {
		parentRoot.Close()
		return nil, nil, fmt.Errorf("create private module copy: %w", err)
	}
	// The generator's HOME and TMPDIR: beside the copy, never inside it (a
	// file there would be an undeclared output) and never inherited.
	scratchName, removeScratch, err := tempDirIn(parentRoot, "gqlgen-guard-env-")
	if err != nil {
		_ = removeCopy()
		parentRoot.Close()
		return nil, nil, fmt.Errorf("create the generator's scratch directory: %w", err)
	}
	// A removal that fails is REPORTED. Discarded, it left a whole copy of the
	// working tree in the temporary directory with nothing in the output
	// saying so.
	cleanup := func() {
		rw := opts.report()
		for _, d := range []struct {
			name, what string
			remove     func() error
		}{
			{copyName, "private copy", removeCopy},
			{scratchName, "generator's scratch directory", removeScratch},
		} {
			if err := d.remove(); err != nil {
				fmt.Fprintf(rw, "note: the %s %s could not be removed: %v\n", d.what, filepath.Join(parentLoc, d.name), err)
			}
		}
		parentRoot.Close()
	}
	copyDir := filepath.Join(parentLoc, copyName)
	scratchDir := filepath.Join(parentLoc, scratchName)
	scratchRoot, err := parentRoot.OpenRoot(scratchName)
	if err != nil {
		return nil, cleanup, fmt.Errorf("open the generator's scratch directory: %w", err)
	}
	defer scratchRoot.Close()
	stageHook("scratch-created", scratchDir)

	copyRoot, err := parentRoot.OpenRoot(copyName)
	if err != nil {
		return nil, cleanup, fmt.Errorf("open private module copy %q: %w", copyDir, err)
	}
	defer copyRoot.Close()
	stageHook("copy-created", copyDir)

	w := opts.report()
	fmt.Fprintf(w, "module: %s\n", moduleAbs)
	fmt.Fprintf(w, "private copy: %s (created through the parent's root; the kernel resolves the parent to %s)\n", copyDir, parentLoc)

	dropped, skipped, err := CopyTree(ctx, moduleRoot, copyRoot, skipVCS)
	if err != nil {
		return nil, cleanup, fmt.Errorf("copy module into %q: %w", copyDir, err)
	}
	// From here on the copy is also used by PATH -- gqlgen's loader and the
	// go command read it, the generator runs in it -- and a path can be
	// swapped for a link after the directory was created through the root.
	// The reads in between write nothing; before the first step that writes
	// by path, each path is checked to still name the directory its root
	// holds (stillNamed): the scratch directory before the go command runs
	// with HOME in it, both directories before the generator.
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

	for _, sk := range skipped {
		fmt.Fprintf(w, "skipped %s (%s: not a regular file, directory or symlink); the copy holds an inert self-loop in its place\n", sk.Path, sk.Mode)
	}

	// The config is read from the COPY, which is byte-identical to the tree,
	// so the enumeration describes the file the generator is about to read.
	plan, err := EnumerateOutputs(copyDir, opts.configPath())
	if err != nil {
		return nil, cleanup, explainSkipped(err, skipped)
	}
	fmt.Fprintf(w, "config: %s (%d schema file(s), %d declared output path(s))\n",
		plan.ConfigPath, len(plan.Schemas), len(plan.Outputs))
	// Every file the generator will read from the config, as the config names
	// it and where it physically resolves -- so a change that widens the
	// generator's inputs shows on the line that names the input.
	for _, in := range append(append([]Input(nil), plan.SchemaInputs...), plan.Templates...) {
		fmt.Fprintf(w, "generator input: %s %s -> %s\n", in.Knob, in.Path, in.Physical)
	}

	if err := refuseDiscoverableConfigs(copyRoot, plan); err != nil {
		return nil, cleanup, err
	}
	// The record and the configuration are two inputs that name paths in the
	// same tree. Where they name the SAME path, the record write is a write
	// over a generator input or output: `check-drift -update -record
	// <a declared output>` replaced a 3 MB generated file with the 15 KB
	// record, and `check-drift` alone read that file as a record and reported
	// a mismatch about it.
	if err := refuseRecordOverTheGeneratorsOwnPaths(moduleAbs, opts.driftPath(), plan); err != nil {
		return nil, cleanup, err
	}

	workDir := filepath.Join(copyDir, filepath.FromSlash(plan.ConfigDir))
	// The go command is about to run with HOME and TMPDIR in the scratch
	// directory, by path.
	if err := stillNamed(scratchRoot, scratchDir, "generator's scratch directory"); err != nil {
		return nil, cleanup, err
	}
	childEnv, err := childEnvironment(ctx, tool, scratchRoot, scratchDir, workDir, copyDir, moduleAbs, w)
	if err != nil {
		return nil, cleanup, err
	}
	if err := refuseReplacesOutsideTheCopy(ctx, tool, copyRoot, copyDir, childEnv); err != nil {
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
		gen = NewGoRunGenerator(tool, w, w)
	}
	fmt.Fprintf(w, "generator: %s (cwd %s)\n", gen.Describe(), plan.ConfigDir)

	// The last path-based check before the generator -- a separate process
	// that can only be handed paths -- starts in the copy with its HOME and
	// TMPDIR in the scratch directory.
	stageHook("before-generator", copyDir+"\x00"+scratchDir)
	if err := stillNamed(copyRoot, copyDir, "private copy"); err != nil {
		return nil, cleanup, err
	}
	if err := stillNamed(scratchRoot, scratchDir, "generator's scratch directory"); err != nil {
		return nil, cleanup, err
	}

	if err := gen.Generate(ctx, workDir, path.Base(plan.ConfigPath), childEnv); err != nil {
		return nil, cleanup, fmt.Errorf("refusing: the generator failed: %w (nothing was written to %s)", err, moduleAbs)
	}
	// No separate "cancelled after generating" check, and none after the copy
	// either: every walk here (the copy and both snapshots) checks ctx before
	// its first entry, so each explicit check that used to sit between them
	// was subsumed by the next walk's own first check; a guard that cannot
	// fail is not a guard.
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
// inherited that is not named here. An inherited
// `GOFLAGS=-mod=mod -modfile=<tree>/x.mod` (or the same through a GOENV file)
// would otherwise make the child write into the working tree while check-drift
// passes.
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
// GuardQueryEnv is the environment of the go commands the GUARD itself runs to
// read the user's setup -- `go list -m` for the module root, `go env` for the
// cache locations. Those queries exist to read the user's environment, so it is
// passed, with three changes that stop them acting on anything:
//
//   - GOENV names the user's go env file explicitly (the same path the go
//     command derives: $GOENV, else os.UserConfigDir()/go/env), so the file is
//     still read after the next change;
//   - XDG_CONFIG_HOME is os.DevNull: the go command's telemetry writes counters
//     under the config directory -- measured: with HOME inside the module, 3
//     files under <module>/.home/.config/go/telemetry/local from one `go env`
//     -- and under /dev/null there is nowhere to create them (measured: exit 0,
//     0 entries). os.UserConfigDir honours XDG_CONFIG_HOME on Linux, not on
//     macOS (RISK-NOTES);
//   - GOTOOLCHAIN=local: a query never downloads or switches toolchains.
func GuardQueryEnv() []string {
	goenv := os.Getenv("GOENV")
	if goenv == "" {
		goenv = "off" // what the go command does when there is no config dir
		if dir, err := os.UserConfigDir(); err == nil {
			goenv = filepath.Join(dir, "go", "env")
		}
	}
	return append(os.Environ(), "GOENV="+goenv, "XDG_CONFIG_HOME="+os.DevNull, "GOTOOLCHAIN=local")
}

// GoTool is the go command ONE run uses. It is resolved once, before anything
// is executed, and every later command is checked against that one resolution
// rather than against a fresh lookup -- a fresh lookup walks the same PATH in
// the same process and compares a value with itself, so it can only catch a
// PATH that changes mid-run.
//
// The zero value runs nothing: Command refuses until ResolveGoTool has
// produced a path.
type GoTool struct {
	// path is the vetted go binary, resolved physically.
	path string
}

// ResolveGoTool resolves the go command from PATH once and refuses one that
// lives inside moduleDir, BEFORE any go command runs -- so a `go` shim
// committed in the working tree is never executed, not even by the query that
// locates the module. An empty moduleDir skips the confinement check; the
// caller that does not know the module root yet must call Confine once it
// does.
func ResolveGoTool(moduleDir string) (GoTool, error) {
	p, err := exec.LookPath("go")
	if err != nil {
		return GoTool{}, fmt.Errorf("refusing: the go command is not on PATH: %w", err)
	}
	return GoTool{path: resolvedPath(p)}.Confine(moduleDir)
}

// Confine refuses the tool when it resolves inside moduleDir. It is separate
// from ResolveGoTool because the module root is itself discovered by running
// the go command: the caller confines against the module that lexically
// encloses the working directory first, then against the root it settled on.
func (g GoTool) Confine(moduleDir string) (GoTool, error) {
	if g.path == "" {
		return GoTool{}, errors.New("refusing: the go command has not been resolved")
	}
	if moduleDir == "" {
		return g, nil
	}
	if within(resolvedPath(moduleDir), g.path) {
		return GoTool{}, fmt.Errorf("refusing: the go command resolves to %q, inside the module", g.path)
	}
	return g, nil
}

// Path is the vetted go binary.
func (g GoTool) Path() string { return g.path }

// Command builds one go command. The first argument is the literal name "go",
// which exec resolves through PATH into cmd.Path; that resolution is then
// compared with the path THIS run vetted, and a mismatch refuses. The command
// is never redirected to a path -- overwriting cmd.Path would silence the
// check instead of making it -- so a PATH that changes after the run began
// stops the run before the command executes.
func (g GoTool) Command(ctx context.Context, args ...string) (*exec.Cmd, error) {
	if g.path == "" {
		return nil, errors.New("refusing: the go command has not been resolved for this run")
	}
	stageHook("go-command", "")
	cmd := exec.CommandContext(ctx, "go", args...)
	if cmd.Err != nil {
		return nil, fmt.Errorf("refusing: the go command cannot be located: %w", cmd.Err)
	}
	if got := resolvedPath(cmd.Path); got != g.path {
		return nil, fmt.Errorf("refusing: `go` now resolves to %q, not the go binary this run vetted (%q)", got, g.path)
	}
	return cmd, nil
}

// childEnvironment builds the generator's environment from the allowlist, and
// then ASSERTS it by asking the go command, in the directory the generator
// will run in and with exactly that environment, for its effective settings.
func childEnvironment(ctx context.Context, tool GoTool, scratchRoot *os.Root, scratch, workDir, copyDir, moduleAbs string, w io.Writer) ([]string, error) {
	goBin := tool.Path()
	copyReal := resolvedPath(copyDir)
	moduleReal := resolvedPath(moduleAbs)
	inside := func(p string) bool {
		r := resolvedPath(p)
		return within(moduleReal, r) || within(copyReal, r)
	}

	// The caches: the guard's EFFECTIVE values (environment and go env file
	// alike), read in the scratch directory so no module is in play.
	// GOTOOLCHAIN/GOWORK/GOFLAGS are pinned for this one query too, so the
	// guard's own lookup cannot switch toolchains or be redirected; the caches
	// it reports come from the environment and the go env file as the user set
	// them. (exec keeps the LAST value of a duplicated key.)
	parent := append(GuardQueryEnv(), "GOWORK=off", "GOFLAGS=")
	shared, err := goEnv(ctx, tool, scratch, parent, sharedLocations...)
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
	// Written through the scratch directory's root: a link planted in it --
	// `home` pointing into the module -- is refused, never followed.
	for _, d := range []string{"home", "tmp"} {
		if err := scratchRoot.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("create the generator's scratch %q: %w", d, err)
		}
	}
	// Go telemetry is OFF in the scratch HOME. Its default ("local") has the
	// go command write counters under $HOME/.config/go/telemetry from a
	// process that can outlive the command -- measured: files appearing in the
	// scratch directory after the guard had returned and removed it. "off" is
	// the mode file's documented value (x/telemetry internal/telemetry dir.go).
	const modeFile = "home/.config/go/telemetry/mode"
	if err := scratchRoot.MkdirAll(path.Dir(modeFile), 0o700); err != nil {
		return nil, fmt.Errorf("create the generator's telemetry config: %w", err)
	}
	if err := scratchRoot.WriteFile(modeFile, []byte("off\n"), 0o600); err != nil {
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
	eff, err := goEnv(ctx, tool, workDir, env, "GOMOD", "GOFLAGS", "GOENV", "GOWORK", "GOTOOLCHAIN", "GOMODCACHE", "GOCACHE", "GOTMPDIR")
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(w, "generator env (allowlist %s; nothing inherited): GOMOD=%s GOFLAGS=%s GOENV=%q GOWORK=%s GOTOOLCHAIN=%s\n",
		strings.Join(childEnvKeys, ","), eff["GOMOD"], eff["GOFLAGS"], eff["GOENV"], eff["GOWORK"], eff["GOTOOLCHAIN"])
	fmt.Fprintf(w, "generator locations (physical): GOCACHE=%s GOMODCACHE=%s GOPATH=%s\n",
		physicalList(shared["GOCACHE"]), physicalList(shared["GOMODCACHE"]), physicalList(shared["GOPATH"]))
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

func goEnv(ctx context.Context, tool GoTool, dir string, env []string, keys ...string) (map[string]string, error) {
	cmd, err := tool.Command(ctx, append([]string{"env", "-json"}, keys...)...)
	if err != nil {
		return nil, err
	}
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
func refuseReplacesOutsideTheCopy(ctx context.Context, tool GoTool, copyRoot *os.Root, copyDir string, env []string) error {
	if _, err := copyRoot.Lstat("go.mod"); err != nil {
		if errNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat go.mod: %w", err)
	}
	cmd, err := tool.Command(ctx, "mod", "edit", "-json")
	if err != nil {
		return err
	}
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

// resolvedPath is p where the operating system would find it (physicalPath):
// never cleaned before its links are followed, so `<link>/..` is judged where
// it lands. An unresolvable p (a loop) is returned cleaned, which no caller
// treats as confinement: every caller refuses on a location INSIDE the module.
func resolvedPath(p string) string {
	if r, err := physicalPath(p); err == nil {
		return r
	}
	if a, err := filepath.Abs(p); err == nil {
		return a
	}
	return p
}

// physicalList is resolvedPath over each entry of a path list (GOPATH).
func physicalList(v string) string {
	var out []string
	for _, e := range filepath.SplitList(v) {
		out = append(out, resolvedPath(e))
	}
	return strings.Join(out, string(filepath.ListSeparator))
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
		return digestFile(root, rel)
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
func refuseCopyInsideModule(moduleAbs, tempParent string) (string, error) {
	parent := tempParent
	if parent == "" {
		parent = os.TempDir()
	}
	// Resolved PHYSICALLY, never cleaned first: `<a link to an in-module
	// dir>/../<dir>` is inside the module to the operating system that creates
	// the copy and outside it to filepath.Abs, and such a TMPDIR would restart
	// the self-copy in the tree.
	parentAbs, err := physicalPath(parent)
	if err != nil {
		return "", fmt.Errorf("refusing: the private copy's parent %q cannot be resolved: %w", parent, err)
	}
	moduleReal := resolvedPath(moduleAbs)
	rel, err := filepath.Rel(moduleReal, parentAbs)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf(
			"refusing: the private copy would be made inside the module (%s is under %s), and copying the module would copy the copy into itself. Point TMPDIR outside the module",
			parentAbs, moduleReal)
	}
	return parentAbs, nil
}

// rootLocation is where the kernel says the directory a root is open on is,
// read from the open handle itself (/proc/self/fd on Linux), so a link swapped
// into the path it was opened by cannot change the answer. Elsewhere the name
// is resolved physically (no /proc: the window between open and check is
// then not closed).
func rootLocation(r *os.Root) (string, error) {
	f, err := r.Open(".")
	if err != nil {
		return "", fmt.Errorf("open %q through its root: %w", r.Name(), err)
	}
	defer f.Close()
	if runtime.GOOS == "linux" {
		loc, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", f.Fd()))
		if err != nil {
			return "", fmt.Errorf("read where %q is open: %w", r.Name(), err)
		}
		return loc, nil
	}
	return physicalPath(r.Name())
}

// stillNamed refuses when path p no longer names the directory root is open
// on -- a link, or another directory, swapped in at p after the directory was
// created through its parent's root. The comparison is by file identity
// (device and inode), not by name.
func stillNamed(root *os.Root, p, what string) error {
	held, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("refusing: stat the %s through its root: %w", what, err)
	}
	named, err := os.Stat(p)
	if err != nil || !os.SameFile(held, named) {
		return fmt.Errorf("refusing: the %s's path %s no longer names the directory the guard created (%v); something replaced it while the guard ran", what, p, err)
	}
	return nil
}

// ownerMarkerOf is the name of the file that records which process created a
// directory the guard made outside the tree. It sits BESIDE that directory,
// never inside it, so the private copy stays byte-identical to the module.
//
// It is what makes the stale sweep safe: a directory with no marker was not
// made by this guard (or was made in the instant before the marker landed) and
// is never removed, and one whose process is still running belongs to a
// concurrent run.
func ownerMarkerOf(name string) string { return "." + name + ".owner" }

// markedName is ownerMarkerOf in reverse: the directory a marker belongs to.
func markedName(marker string) (string, bool) {
	name, ok := strings.CutPrefix(marker, ".")
	if !ok {
		return "", false
	}
	name, ok = strings.CutSuffix(name, ".owner")
	if !ok || !strings.HasPrefix(name, "gqlgen-guard-") {
		return "", false
	}
	return name, true
}

// tempDirIn creates a marked temporary directory inside root and returns its
// name together with the removal that takes the MARKER with it. The two are
// returned as one thing so no error path can remove the directory and leave
// its marker behind: an orphan marker is a file nothing ever cleans up.
func tempDirIn(root *os.Root, prefix string) (string, func() error, error) {
	name, err := mkdirTempIn(root, prefix)
	if err != nil {
		return "", nil, err
	}
	return name, func() error {
		if err := root.RemoveAll(name); err != nil {
			return err
		}
		if err := root.Remove(ownerMarkerOf(name)); err != nil && !errNotExist(err) {
			return err
		}
		return nil
	}, nil
}

// mkdirTempIn creates a new directory named prefix+random inside root and
// returns its name, the way os.MkdirTemp does for a path, leaving its owner
// marker beside it.
func mkdirTempIn(root *os.Root, prefix string) (string, error) {
	for try := 0; try < 10000; try++ {
		var b [6]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		name := prefix + hex.EncodeToString(b[:])
		err := root.Mkdir(name, 0o700)
		if err == nil {
			if err := root.WriteFile(ownerMarkerOf(name), fmt.Appendf(nil, "%d\n", os.Getpid()), 0o600); err != nil {
				_ = root.RemoveAll(name)
				return "", fmt.Errorf("mark %q as this run's: %w", name, err)
			}
			return name, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", err
		}
	}
	return "", fmt.Errorf("could not create a unique %s* directory in %q", prefix, root.Name())
}

// sweepStaleCopies reports and removes the directories earlier runs of THIS
// guard left in parent and could not clean up -- an uncatchable signal
// (SIGKILL) is the only way they survive, and each is a whole copy of the
// working tree, including anything untracked in it. Nothing without the
// guard's own marker is touched, and nothing whose marking process is still
// running: a concurrent run's copy is left alone.
func sweepStaleCopies(parentRoot *os.Root, parentLoc string, w io.Writer) {
	entries, err := fs.ReadDir(parentRoot.FS(), ".")
	if err != nil {
		fmt.Fprintf(w, "note: could not list %s for stale private copies: %v\n", parentLoc, err)
		return
	}
	present := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() {
			present[e.Name()] = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		// A marker whose directory is already gone is an orphan: nothing else
		// would ever look at it again.
		if orphan, ok := markedName(e.Name()); ok && !present[orphan] {
			if err := parentRoot.Remove(e.Name()); err == nil {
				fmt.Fprintf(w, "note: removed the owner marker of a private copy that is already gone (%s)\n", filepath.Join(parentLoc, e.Name()))
			}
			continue
		}
		if !e.IsDir() || !strings.HasPrefix(name, "gqlgen-guard-") {
			continue
		}
		marker, err := ReadThroughRoot(parentRoot, ownerMarkerOf(name))
		if err != nil {
			continue // not ours (or unreadable): never removed
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(marker)))
		if err != nil || processAlive(pid) {
			continue
		}
		age := "unknown age"
		if info, serr := e.Info(); serr == nil {
			age = time.Since(info.ModTime()).Round(time.Second).String() + " old"
		}
		full := filepath.Join(parentLoc, name)
		if rerr := parentRoot.RemoveAll(name); rerr != nil {
			fmt.Fprintf(w, "note: a private copy left by pid %d (%s, %s) could not be removed: %v\n", pid, full, age, rerr)
			continue
		}
		_ = parentRoot.Remove(ownerMarkerOf(name))
		fmt.Fprintf(w, "note: removed the private copy pid %d left behind (%s, %s); it was killed before it could clean up\n", pid, full, age)
	}
}

// processAlive reports whether pid still names a running process. A signal 0
// that is refused (EPERM) means the process exists but is not ours, which is
// still alive -- and still a reason not to remove its directory.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = proc.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}

// refuseRecordOverTheGeneratorsOwnPaths refuses a record path that is also a
// path the gqlgen configuration names: a declared output, a schema, a template,
// or the config file itself. Both verbs refuse, because reading one of those as
// a record is as wrong as writing over it.
func refuseRecordOverTheGeneratorsOwnPaths(moduleAbs, driftPath string, plan *Plan) error {
	// Both sides are compared where the operating system puts them, not only
	// as written: `-record <a link to the output directory>/generated.go` is
	// lexically a different path and physically the same file.
	rel := func(p string) string {
		phys := resolvedPath(moduleAbs + string(filepath.Separator) + filepath.FromSlash(p))
		r, err := filepath.Rel(resolvedPath(moduleAbs), phys)
		if err != nil {
			return path.Clean(p)
		}
		return filepath.ToSlash(r)
	}
	// The record is an operator-named path like the config, so it gets the
	// same rule: a `..` after a directory name climbs from wherever that name
	// resolves, which through a link is not where the path reads.
	if err := refuseDotDotAfterAName("expected-drift record", driftPath); err != nil {
		return err
	}
	named := map[string]string{}
	add := func(p, what string) {
		named[path.Clean(p)] = what
		named[rel(p)] = what
	}
	add(plan.ConfigPath, "the gqlgen config")
	for _, in := range append(append([]Input(nil), plan.SchemaInputs...), plan.Templates...) {
		add(in.Path, in.Knob)
		add(in.Physical, in.Knob)
	}
	for _, sch := range plan.Schemas {
		add(sch, "a schema")
	}
	for _, o := range plan.Outputs {
		add(o.Path, "the declared output of "+string(o.Declaree))
	}
	for _, form := range []string{path.Clean(driftPath), rel(driftPath)} {
		if knob, ok := named[form]; ok {
			return fmt.Errorf(
				"refusing: the expected-drift record %q is also %s (both resolve to %q); the record would be read from, and with -update written over, a file the generator owns",
				driftPath, knob, form)
		}
	}
	return nil
}

// explainSkipped names the cause when a failure is about a path CopyTree could
// not reproduce. The copy holds an inert self-loop there, so the failure that
// reaches the operator is "too many levels of symbolic links" -- true, and
// useless on its own. Everything the guard skipped is already on the report;
// this puts the reason on the refusal itself.
//
// There is deliberately no SEPARATE refusal for "a skipped entry the config
// declares". Every such path is already refused by the check that reads it --
// the config's own resolution, the schema's physical resolution, the
// template's regular-file check, the declared output's -- and a guard that
// cannot fire is not a guard. What was missing was never the refusal; it was
// the reason, which is what this adds.
func explainSkipped(err error, skipped []SkippedEntry) error {
	if err == nil {
		return nil
	}
	for _, sk := range skipped {
		if strings.Contains(err.Error(), sk.Path) {
			return fmt.Errorf("%w (%q in the working tree is not a regular file, directory or symlink (%s), so the copy holds an inert self-loop where it was)", err, sk.Path, sk.Mode)
		}
	}
	return err
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
