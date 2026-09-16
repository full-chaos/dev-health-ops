package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestASkippedNonRegularEntryLeavesAnInertStandIn: a FIFO or socket the
// configuration does not name is skipped rather than refused, but the copy
// must still hold something at that name -- an inert link to itself -- so a
// generator that somehow reads it fails loudly instead of finding nothing (or,
// worse, finding whatever ordinary file a later run happened to leave there).
func TestASkippedNonRegularEntryLeavesAnInertStandIn(t *testing.T) {
	f := guardFixture(t).withModuleFiles()
	mkfifo(t, filepath.Join(f.dir, "unrelated", "devserver.sock"))

	var seenLink string
	var seenLinkErr error
	gen := &fakeGenerator{fn: func(workDir string) error {
		full := filepath.Join(workDir, "unrelated", "devserver.sock")
		seenLink, seenLinkErr = os.Readlink(full)
		return rewriteOutputs("generated")(workDir)
	}}
	opts := guardOptions(f, gen)
	if _, err := UpdateDriftRecord(context.Background(), opts); err != nil {
		t.Fatalf("the guard refused a tree containing a skipped FIFO: %v", err)
	}
	if !gen.ran {
		t.Fatal("the generator never ran, so the copy was never inspected")
	}
	t.Logf("CELL-OUTPUT: copy holds unrelated/devserver.sock -> %q, %v", seenLink, seenLinkErr)
	if seenLinkErr != nil {
		t.Fatalf("the copy carries nothing at all where the skipped entry was: %v", seenLinkErr)
	}
	if seenLink != "devserver.sock" {
		t.Fatalf("the copy carries unrelated/devserver.sock -> %q, want the inert self-loop %q", seenLink, "devserver.sock")
	}
}

// TestAnUndeclaredWriteIsRefusedEvenWhenItsBaseNameMatchesADeclaredOutput: the
// generator may rewrite go.mod, go.sum, go.work and go.work.sum inside its own
// copy without that being a finding (a `go mod tidy` in a throwaway copy is
// harmless and never copied back). Every OTHER base name the generator writes
// outside its declared surface is still a refusal, however it happens to be
// named -- naming a file "generated.go" does not make an undeclared write into
// a module-file rewrite.
func TestAnUndeclaredWriteIsRefusedEvenWhenItsBaseNameMatchesADeclaredOutput(t *testing.T) {
	f := guardFixture(t)
	gen := &fakeGenerator{fn: func(workDir string) error {
		if err := rewriteOutputs("generated")(workDir); err != nil {
			return err
		}
		return writeIn(workDir, "spurious/generated.go", "package spurious\n")
	}}
	before := f.digests()
	_, err := CheckDrift(context.Background(), guardOptions(f, gen))
	if err == nil {
		t.Fatal("an undeclared write named like a declared output was accepted")
	}
	if !strings.Contains(err.Error(), "spurious/generated.go") {
		t.Fatalf("the refusal does not name the undeclared write: %v", err)
	}
	assertUnchanged(t, before, f.digests(), "an undeclared write sharing a declared output's base name")
}

// TestTheCacheLocationQueryPinsGOWORKAndGOFLAGSRegardlessOfTheAmbientEnvironment:
// the one `go env` query the guard makes for its OWN effective cache locations
// pins GOWORK=off and GOFLAGS="" so that query cannot be redirected by
// whatever the operator's shell happens to have set. cacheQueryEnv is the
// whole of that pinning; asserting its return value is the only way to pin the
// property itself; a `go env GOCACHE` sample gives the same answer with or
// without the pin (measured), so a black-box run around it would prove
// nothing.
func TestTheCacheLocationQueryPinsGOWORKAndGOFLAGSRegardlessOfTheAmbientEnvironment(t *testing.T) {
	t.Setenv("GOWORK", "/nonexistent/somewhere.work")
	t.Setenv("GOFLAGS", "-mod=mod -overlay=/nonexistent/overlay.json")

	env := cacheQueryEnv()
	got := map[string]string{}
	for _, kv := range env {
		if k, v, ok := strings.Cut(kv, "="); ok {
			got[k] = v // exec.Cmd keeps the LAST value of a duplicated key
		}
	}
	if got["GOWORK"] != "off" {
		t.Fatalf("GOWORK = %q, want the pinned \"off\" regardless of the ambient GOWORK", got["GOWORK"])
	}
	if got["GOFLAGS"] != "" {
		t.Fatalf("GOFLAGS = %q, want the pinned \"\" regardless of the ambient GOFLAGS", got["GOFLAGS"])
	}
}

// TestGenerateRecreatesADeclaredOutputThatIsAbsentFromTheTree: a declared
// output that does not exist in the working tree yet -- deleted, or never
// generated -- is exactly what Change.Drifted() must call drift: "" (absent)
// differs from whatever the generator produced. Removing it and regenerating
// must bring it back.
func TestGenerateRecreatesADeclaredOutputThatIsAbsentFromTheTree(t *testing.T) {
	f := guardFixture(t)
	if err := os.Remove(filepath.Join(f.dir, "gen", "resolver.go")); err != nil {
		t.Fatal(err)
	}
	if f.exists("gen/resolver.go") {
		t.Fatal("setup: the output is still there")
	}
	gen := &fakeGenerator{fn: func(workDir string) error {
		if err := rewriteOutputs("generated")(workDir); err != nil {
			return err
		}
		return writeIn(workDir, "gen/resolver.go", markedResolver)
	}}
	if _, err := Generate(context.Background(), writeOptions(f, gen)); err != nil {
		t.Fatalf("generate refused to recreate a missing declared output: %v", err)
	}
	if !f.exists("gen/resolver.go") {
		t.Fatal("the missing declared output was not recreated")
	}
	if got := f.read("gen/resolver.go"); got != markedResolver {
		t.Fatalf("the recreated output does not hold what the generator produced:\n%s", got)
	}
}

// TestTheStaleSweepOnlyConsidersItsOwnPrefixedDirectories: the stale-copy
// sweep only ever looks at directories named "gqlgen-guard-*" -- TMPDIR is a
// shared directory, and something else's leftover that happens to carry a
// dead-pid owner marker beside it must never be swept.
func TestTheStaleSweepOnlyConsidersItsOwnPrefixedDirectories(t *testing.T) {
	f := guardFixture(t).withModuleFiles()
	opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	parent := opts.TempParent

	foreign := filepath.Join(parent, "unrelated-tool-leftover")
	if err := os.MkdirAll(filepath.Join(foreign, "payload"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, ownerMarkerOf("unrelated-tool-leftover")), []byte("2147483646\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if processAlive(2147483646) {
		t.Skip("pid 2147483646 is running on this host, so it is not a dead run")
	}

	if _, err := CheckDrift(context.Background(), opts); err != nil {
		t.Logf("CheckDrift: %v", err) // the missing record's own refusal is irrelevant here
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("a directory with no \"gqlgen-guard-\" prefix was swept: %v", err)
	}
	t.Logf("CELL-OUTPUT: accepted: %s still present after the sweep", foreign)
}

// TestAForeignOwnerMarkerIsNeverTreatedAsAnOrphan: the orphan-marker branch of
// the sweep removes a "*.owner" file whose directory is already gone, but only
// when that file is actually shaped like ONE OF THIS GUARD'S markers
// (".gqlgen-guard-<hex>.owner"). A marker-shaped file for something else that
// happens to live in the same temporary directory must be left alone.
func TestAForeignOwnerMarkerIsNeverTreatedAsAnOrphan(t *testing.T) {
	f := guardFixture(t).withModuleFiles()
	opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	parent := opts.TempParent

	foreignMarker := filepath.Join(parent, ".some-other-tools-lockfile.owner")
	if err := os.WriteFile(foreignMarker, []byte("2147483646\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := CheckDrift(context.Background(), opts); err != nil {
		t.Logf("CheckDrift: %v", err)
	}
	if _, err := os.Stat(foreignMarker); err != nil {
		t.Fatalf("a marker-shaped file that is not this guard's was swept as an orphan: %v", err)
	}
}

// TestThePrivateCopyIsNotGroupOrWorldWritable: the private copy is a whole
// working tree, including anything untracked in it, made in a shared
// temporary directory. Its mode must be 0o700 for the whole run, not whatever
// the umask happens to leave from a wider request.
func TestThePrivateCopyIsNotGroupOrWorldWritable(t *testing.T) {
	f := guardFixture(t)
	var mode os.FileMode
	gen := &fakeGenerator{fn: func(workDir string) error {
		info, err := os.Stat(workDir)
		if err != nil {
			return err
		}
		mode = info.Mode()
		return rewriteOutputs("generated")(workDir)
	}}
	if _, err := UpdateDriftRecord(context.Background(), guardOptions(f, gen)); err != nil {
		t.Fatalf("UpdateDriftRecord: %v", err)
	}
	if !gen.ran {
		t.Fatal("the generator never ran, so the copy's mode was never observed")
	}
	t.Logf("CELL-OUTPUT: private copy mode %s", mode.Perm())
	if mode.Perm() != 0o700 {
		t.Fatalf("the private copy's mode is %s, want 0700 (group/other must have no access)", mode.Perm())
	}
}

// TestWithinDistinguishesASiblingDirectoryWithAnOverlappingPrefix: within must
// use path-component containment (filepath.Rel), not a plain string prefix --
// "/x/foo" is not inside "/x/fo" even though the string "/x/foo" starts with
// the string "/x/fo".
func TestWithinDistinguishesASiblingDirectoryWithAnOverlappingPrefix(t *testing.T) {
	root := filepath.FromSlash("/x/fo")
	sibling := filepath.FromSlash("/x/foo/bar")
	if within(root, sibling) {
		t.Fatalf("within(%q, %q) = true, want false: %q is a sibling of %q's parent, not beneath it", root, sibling, sibling, root)
	}
	// The positive case alongside it, so a mutant that always returns false
	// cannot pass by accident.
	child := filepath.FromSlash("/x/fo/bar")
	if !within(root, child) {
		t.Fatalf("within(%q, %q) = false, want true", root, child)
	}
}

// TestACallerSuppliedGoToolIsReconfinedAgainstTheSettledModuleRoot: the
// command resolves and confines the go tool once, before the module root is
// even known, and passes that resolution in through Options.GoTool. The guard
// re-confines it against the root it actually settles on -- -module may have
// moved that root -- so a caller-supplied tool is never trusted un-checked.
func TestACallerSuppliedGoToolIsReconfinedAgainstTheSettledModuleRoot(t *testing.T) {
	f := guardFixture(t).withModuleFiles()
	opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	// A "vetted" tool whose path lies INSIDE this run's module root -- as if
	// it had been resolved and confined against some other directory earlier.
	// It never has to exist: the re-confinement must refuse before anything
	// tries to run it.
	opts.GoTool = GoTool{path: filepath.Join(f.dir, "bin", "go")}

	_, err := CheckDrift(context.Background(), opts)
	if err == nil {
		t.Fatal("a caller-supplied go tool resolving inside the module was accepted")
	}
	if !strings.Contains(err.Error(), "inside the module") {
		t.Fatalf("want a refusal naming the tool as inside the module, got: %v", err)
	}
}

// shortReadReader hands back at most n bytes per Read call, never signalling
// EOF early -- the shape a real short read has: less than requested, no error,
// more still available. A single Read against it proves nothing about what
// the rest of the window holds; only a loop (io.ReadFull) does.
type shortReadReader struct {
	data []byte
	pos  int
	n    int
}

func (r *shortReadReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, nil // callers of ReadFull/Read treat 0,nil as "try again"; use io.EOF below instead
	}
	max := r.n
	if max > len(p) {
		max = len(p)
	}
	remaining := len(r.data) - r.pos
	if max > remaining {
		max = remaining
	}
	copy(p, r.data[r.pos:r.pos+max])
	r.pos += max
	return max, nil
}

// TestProvenanceWindowIsFilledAcrossShortReads: readProvenanceWindow must loop
// (io.ReadFull) rather than trust a single Read, or a reader that hands back
// the generated header in small pieces -- the shape a short read has -- would
// be read as if the window ended after the first piece.
func TestProvenanceWindowIsFilledAcrossShortReads(t *testing.T) {
	body := []byte(generatedHeaderLine + "\n\npackage gen\n")
	padding := make([]byte, markerScanBytes-len(body))
	for i := range padding {
		padding[i] = ' '
	}
	full := append(append([]byte{}, body...), padding...)

	r := &shortReadReader{data: full, n: 3} // three bytes per Read call
	buf, n, err := readProvenanceWindow(r)
	if err != nil {
		t.Fatalf("readProvenanceWindow: %v", err)
	}
	if n != len(full) {
		t.Fatalf("read %d bytes from a reader handing back 3 at a time, want the whole %d-byte window", n, len(full))
	}
	if string(buf[:len(body)]) != string(body) {
		t.Fatalf("the window does not hold the generated header intact:\n%q", buf[:len(body)])
	}
}

// TestAMalformedDigestLineIsNotIndexed: digestLines indexes the record's
// "digest <status> <path> tree=<d> generated=<d>" lines by path. A line with
// too few or too many fields is not this shape and must not be indexed at
// all -- an index entry for a path the line did not actually name would let
// that path's hand-edit protection be judged against the wrong bytes.
func TestAMalformedDigestLineIsNotIndexed(t *testing.T) {
	record := strings.Join([]string{
		"digest drift gen/generated.go tree=" + strings.Repeat("a", 64) + " generated=" + strings.Repeat("b", 64),
		"digest drift gen/malformed.go", // missing both tree= and generated=
		"",
	}, "\n")
	got := digestLines(record)
	if _, ok := got["gen/generated.go"]; !ok {
		t.Fatalf("a well-formed digest line was not indexed: %#v", got)
	}
	if line, ok := got["gen/malformed.go"]; ok {
		t.Fatalf("a malformed digest line (wrong field count) was indexed anyway: %q", line)
	}
}
