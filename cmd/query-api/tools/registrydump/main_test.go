package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFixture writes src to a temp .go file and returns its path.
// enumerate only parses (go/parser), never type-checks, so these fixtures
// never need to import/define digestHex or compile -- they only need to be
// syntactically valid Go, matching what enumerate actually consumes.
func writeFixture(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "query_route.go")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

// TestEnumerate_HappyPath pins the ordinary case: N registered*Document
// consts, each referenced exactly once from digestByOperation, enumerate
// returns exactly N rows with the right operation/document/const_name.
// This is the case the whole tool exists to get right on every run against
// the real query_route.go -- a change here is a real regression, not a
// tightened check.
func TestEnumerate_HappyPath(t *testing.T) {
	src := `package main

const registeredFooDocument = "query Foo { foo }"
const registeredBarDocument = "query Bar { bar }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
		"Bar": digestHex(registeredBarDocument),
	}
	_ = digestByOperation
}
`
	docs, err := enumerate(writeFixture(t, src))
	if err != nil {
		t.Fatalf("enumerate: unexpected error: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("enumerate: got %d docs, want 2: %+v", len(docs), docs)
	}
	byOp := map[string]registeredDocument{}
	for _, d := range docs {
		byOp[d.Operation] = d
	}
	foo, ok := byOp["Foo"]
	if !ok {
		t.Fatalf("enumerate: missing operation Foo in %+v", docs)
	}
	if foo.Document != "query Foo { foo }" || foo.ConstName != "registeredFooDocument" {
		t.Errorf("enumerate: Foo row = %+v, want document/const_name matched", foo)
	}
	bar, ok := byOp["Bar"]
	if !ok {
		t.Fatalf("enumerate: missing operation Bar in %+v", docs)
	}
	if bar.Document != "query Bar { bar }" || bar.ConstName != "registeredBarDocument" {
		t.Errorf("enumerate: Bar row = %+v, want document/const_name matched", bar)
	}
}

// TestEnumerate_GroupedConstDecl pins the `const A, B = "x", "y"` form
// (regression coverage for the grouped-declaration gap the doc comment
// above documentByConstName's pass-1 loop describes finding by codex
// review).
func TestEnumerate_GroupedConstDecl(t *testing.T) {
	src := `package main

const registeredFooDocument, registeredBarDocument = "query Foo { foo }", "query Bar { bar }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
		"Bar": digestHex(registeredBarDocument),
	}
	_ = digestByOperation
}
`
	docs, err := enumerate(writeFixture(t, src))
	if err != nil {
		t.Fatalf("enumerate: unexpected error: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("enumerate: got %d docs, want 2: %+v", len(docs), docs)
	}
}

// TestEnumerate_OrphanConst pins the existing "const with no map entry"
// cross-check: a registered*Document const nobody's digestByOperation
// entry names must fail loudly, not silently vanish.
func TestEnumerate_OrphanConst(t *testing.T) {
	src := `package main

const registeredFooDocument = "query Foo { foo }"
const registeredOrphanDocument = "query Orphan { orphan }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
	}
	_ = digestByOperation
}
`
	_, err := enumerate(writeFixture(t, src))
	if err == nil {
		t.Fatal("enumerate: expected an error for an orphaned registered*Document const, got nil")
	}
	if !strings.Contains(err.Error(), "registeredOrphanDocument") {
		t.Errorf("enumerate: error %q does not name the orphaned const", err.Error())
	}
}

// TestEnumerate_UnresolvableMapEntry pins the other direction of the same
// cross-check: a digestByOperation entry naming a const that does not
// exist among the registered*Document consts.
func TestEnumerate_UnresolvableMapEntry(t *testing.T) {
	src := `package main

const registeredFooDocument = "query Foo { foo }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo":  digestHex(registeredFooDocument),
		"Ghost": digestHex(registeredGhostDocument),
	}
	_ = digestByOperation
}
`
	_, err := enumerate(writeFixture(t, src))
	if err == nil {
		t.Fatal("enumerate: expected an error for a map entry naming an unknown const, got nil")
	}
	if !strings.Contains(err.Error(), "registeredGhostDocument") || !strings.Contains(err.Error(), "not found among registered") {
		t.Errorf("enumerate: error %q does not clearly name the unresolvable const", err.Error())
	}
}

// TestEnumerate_AmbiguousDigestByOperation pins the round-4 codex finding:
// a second, unrelated local variable also named digestByOperation
// elsewhere in the file must not have its entries silently merged into
// the real map.
func TestEnumerate_AmbiguousDigestByOperation(t *testing.T) {
	src := `package main

const registeredFooDocument = "query Foo { foo }"
const registeredDeadDocument = "query Dead { dead }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
	}
	_ = digestByOperation
}

func deadHelper() {
	digestByOperation := map[string]string{
		"Dead": digestHex(registeredDeadDocument),
	}
	_ = digestByOperation
}
`
	_, err := enumerate(writeFixture(t, src))
	if err == nil {
		t.Fatal("enumerate: expected an error for two digestByOperation assignments, got nil")
	}
	if !strings.Contains(err.Error(), "MORE THAN ONE") {
		t.Errorf("enumerate: error %q does not report ambiguity", err.Error())
	}
}

// TestEnumerate_PostLiteralWrite_FailsLoudly is the round-5 codex finding
// (P2, 2026-08-30) this PR fixes: a write like
// `digestByOperation["runtimeOnly"] = digestHex(registeredRuntimeDocument)`
// AFTER the map literal is valid Go and previously vanished from
// enumerate's output with NO error -- production would register the
// operation, but this tool would silently discover fewer documents than
// the route actually serves. It must now fail loudly, naming the file and
// the exact line of the offending write, rather than pass with an
// undercount.
func TestEnumerate_PostLiteralWrite_FailsLoudly(t *testing.T) {
	src := `package main

const registeredFooDocument = "query Foo { foo }"
const registeredRuntimeDocument = "query Runtime { runtime }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
	}
	digestByOperation["runtimeOnly"] = digestHex(registeredRuntimeDocument)
	_ = digestByOperation
}
`
	path := writeFixture(t, src)
	docs, err := enumerate(path)
	if err == nil {
		t.Fatalf("enumerate: expected an error for a post-literal digestByOperation write, got docs=%+v", docs)
	}
	if !strings.Contains(err.Error(), "digestByOperation[") {
		t.Errorf("enumerate: error %q does not name the offending write", err.Error())
	}
	if !strings.Contains(err.Error(), "outside its map literal") {
		t.Errorf("enumerate: error %q does not explain the post-literal-write problem", err.Error())
	}
	// The offending statement is on line 10 of the fixture above (1-indexed,
	// counting the leading blank line after `package main`). Pin the exact
	// line so this test would catch a regression that reports the WRONG
	// line just as surely as one that reports no error at all.
	wantLine := "query_route.go:10:"
	if !strings.Contains(err.Error(), wantLine) {
		t.Errorf("enumerate: error %q does not cite %s (got a different line, or none)", err.Error(), wantLine)
	}
}

// TestEnumerate_PostLiteralWrite_CompoundAssignment pins that the same
// loud failure fires for a compound-assignment form
// (`digestByOperation["x"] += y`), not only a plain `=` -- the AssignStmt
// node covers both under different Tok values, and this tool's guard must
// not accidentally key off Tok.
func TestEnumerate_PostLiteralWrite_CompoundAssignment(t *testing.T) {
	src := `package main

const registeredFooDocument = "query Foo { foo }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
	}
	digestByOperation["Foo"] += "-suffix"
	_ = digestByOperation
}
`
	_, err := enumerate(writeFixture(t, src))
	if err == nil {
		t.Fatal("enumerate: expected an error for a compound-assignment post-literal write, got nil")
	}
	if !strings.Contains(err.Error(), "outside its map literal") {
		t.Errorf("enumerate: error %q does not explain the post-literal-write problem", err.Error())
	}
}

// TestEnumerate_KillProof_RemovingPostLiteralWriteRestoresPass is the
// paired kill/restore proof the lane brief asks for: mutate a passing
// fixture by adding a post-literal write (watch enumerate fail, naming
// it), then remove it (watch enumerate pass again) -- proving the new
// guard actually distinguishes the two states rather than always failing
// or always passing.
func TestEnumerate_KillProof_RemovingPostLiteralWriteRestoresPass(t *testing.T) {
	clean := `package main

const registeredFooDocument = "query Foo { foo }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
	}
	_ = digestByOperation
}
`
	mutated := `package main

const registeredFooDocument = "query Foo { foo }"
const registeredRuntimeDocument = "query Runtime { runtime }"

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
	}
	digestByOperation["runtimeOnly"] = digestHex(registeredRuntimeDocument)
	_ = digestByOperation
}
`
	docs, err := enumerate(writeFixture(t, clean))
	if err != nil {
		t.Fatalf("clean fixture: unexpected error: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("clean fixture: got %d docs, want 1: %+v", len(docs), docs)
	}

	_, err = enumerate(writeFixture(t, mutated))
	if err == nil {
		t.Fatal("mutated fixture: expected the post-literal write to be rejected, got nil error")
	}

	docsAgain, err := enumerate(writeFixture(t, clean))
	if err != nil {
		t.Fatalf("restored fixture: unexpected error: %v", err)
	}
	if len(docsAgain) != 1 {
		t.Fatalf("restored fixture: got %d docs, want 1: %+v", len(docsAgain), docsAgain)
	}
}

// TestEnumerate_NonLiteralAssignment pins that a whole-map reassignment
// to something other than a composite literal (e.g. a function call) is
// rejected -- this tool only understands a literal map[string]string{...}.
func TestEnumerate_NonLiteralAssignment(t *testing.T) {
	src := `package main

func buildMap() map[string]string { return nil }

func newQueryHandler() {
	digestByOperation := buildMap()
	_ = digestByOperation
}
`
	_, err := enumerate(writeFixture(t, src))
	if err == nil {
		t.Fatal("enumerate: expected an error for a non-composite-literal assignment, got nil")
	}
	if !strings.Contains(err.Error(), "not a composite literal") {
		t.Errorf("enumerate: error %q does not explain the non-literal problem", err.Error())
	}
}

// TestEnumerate_NoAssignmentFound pins that a file with no
// digestByOperation assignment at all fails loudly rather than
// enumerating zero documents silently.
func TestEnumerate_NoAssignmentFound(t *testing.T) {
	src := `package main

const registeredFooDocument = "query Foo { foo }"
`
	_, err := enumerate(writeFixture(t, src))
	if err == nil {
		t.Fatal("enumerate: expected an error when digestByOperation is never assigned, got nil")
	}
	if !strings.Contains(err.Error(), "found no digestByOperation assignment") {
		t.Errorf("enumerate: error %q does not explain the missing-assignment problem", err.Error())
	}
}
