package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/migrationmatrix"
)

// TestTheCommittedMatrixSatisfiesItsOwnContract runs the real -check against
// the real repository, so `go test ./...` fails the moment the committed page
// disagrees with the committed sources or a row claims a status its evidence
// column cannot support.
//
// This is separate from internal/migrationmatrix's unit tests on purpose:
// those prove the RULES work on synthetic rows, this proves the SHIPPED
// DOCUMENT obeys them. A repo can have a perfect validator and a lying page.
func TestTheCommittedMatrixSatisfiesItsOwnContract(t *testing.T) {
	root := repoRoot(t)
	if err := runCheck(root); err != nil {
		t.Fatalf("the committed migration matrix fails its own contract: %v\n\n"+
			"Re-render with:\n"+
			"  go run ./cmd/dev-health-migration-matrix -render -root . -routing <snapshot.json>\n"+
			"(-routing takes a JSON dump of go_api_routing_state; see routingFilePayload.)", err)
	}
}

// TestCheckFailsWhenTheDocIsEditedByHand is the RED half. A generated block is
// only as good as the check that notices someone improved it in place -- which
// is precisely how the ic_finalize citation came to name a file that had been
// deleted weeks earlier.
func TestCheckFailsWhenTheDocIsEditedByHand(t *testing.T) {
	root := t.TempDir()
	realRoot := repoRoot(t)

	for _, relative := range []string{
		docRelative,
		statusRelative,
		renderRelative,
		nativeRelative,
		digestPinRelative,
		providerMatrixRelative,
		dailyFamiliesRelative,
		remainingFamiliesRel,
		jobDailyPyRelative,
	} {
		copyInto(t, filepath.Join(realRoot, relative), filepath.Join(root, relative))
	}

	// Sanity: the copy still passes, so a failure below is caused by the
	// edit and not by the copying.
	if err := runCheck(root); err != nil {
		t.Fatalf("the copied tree should pass before it is tampered with: %v", err)
	}

	docPath := filepath.Join(root, docRelative)
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read copied doc: %v", err)
	}
	// The most tempting hand edit there is: soften the honest cell. Tampering
	// with the PARITY column rather than the deployed one on purpose -- the
	// deployed cell's text legitimately changes when the fleet is rebuilt
	// (it read "**unknown**" until the Compose COMMIT build-arg landed on
	// 2026-09-09, and a real sha after), so a test anchored on it would fail
	// for a reason that has nothing to do with tampering. "UNVERIFIED" is
	// present for as long as any family is unverified, which is the state
	// this whole section exists to keep visible.
	tampered := replaceFirst(string(raw), "| UNVERIFIED |", "| VERIFIED |")
	if tampered == string(raw) {
		t.Fatal("expected the rendered doc to contain an UNVERIFIED parity cell to tamper with")
	}
	if err := os.WriteFile(docPath, []byte(tampered), 0o644); err != nil {
		t.Fatalf("write tampered doc: %v", err)
	}

	if err := runCheck(root); err == nil {
		t.Fatal("hand-editing a generated cell must fail the check; it passed")
	}
}

func replaceFirst(haystack, old, replacement string) string {
	index := indexOf(haystack, old)
	if index < 0 {
		return haystack
	}
	return haystack[:index] + replacement + haystack[index+len(old):]
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func copyInto(t *testing.T, from, to string) {
	t.Helper()
	raw, err := os.ReadFile(from) //nolint:gosec // test-local path
	if err != nil {
		t.Fatalf("read %s: %v", from, err)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(to), err)
	}
	if err := os.WriteFile(to, raw, 0o644); err != nil {
		t.Fatalf("write %s: %v", to, err)
	}
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root above the test's working directory")
		}
		dir = parent
	}
}

// The offline -routing path exists so an operator without a DSN can still
// render the page. opus r5 (P2c) showed its instruction was unfollowable: it
// named an unexported function, so "produce rows with the reader's SQL" meant
// retyping the SQL. These two tests pin the two halves of the fix -- that the
// statement is reachable, and that a snapshot missing the routing KEY is
// refused instead of silently collapsing two documents into one row.

func TestTheOperatorCanObtainTheExactStatementTheReaderRuns(t *testing.T) {
	statement, err := migrationmatrix.RoutingStateSQL()
	if err != nil {
		t.Fatalf("build the routing statement: %v", err)
	}
	// Not a substring check: the whole point is that the offline rows and
	// the live rows come from the SAME text, so anything less than
	// equality with both mode arms of the shared predicate would let the
	// two derivations drift apart again.
	for _, mode := range []string{goapiproof.TargetModePrimary, goapiproof.TargetModeCanary} {
		clause, err := goapiproof.EnablementProofClause("pr", mode)
		if err != nil {
			t.Fatalf("build the %s predicate: %v", mode, err)
		}
		if !strings.Contains(statement, clause) {
			t.Fatalf("the printed statement does not contain the %s enablement clause;\nstatement:\n%s\n\nclause:\n%s", mode, statement, clause)
		}
	}
	// Deliberately NOT asserted here: that the statement SELECTS the
	// document digest. A `strings.Contains(statement, "rs.document_digest")`
	// looks like that assertion and is not one -- the join predicate
	// mentions the same identifier, so deleting the output column leaves
	// the substring in place and the test green. Measured: that exact
	// mutation survived. The column is pinned where it can only be pinned
	// honestly -- by reading it back out of PostgreSQL, in
	// TestReadRoutingStateKeepsTwoDocumentsOfOneOperationApart.
}

func TestARoutingSnapshotWithoutTheDocumentDigestIsRefused(t *testing.T) {
	dir := t.TempDir()

	// Two DIFFERENT documents of one operation at one schema digest -- the
	// exact shape Trap #120 is about. Accepting them without their digests
	// makes them indistinguishable, and ValidateRender's R8 rule then
	// reports a duplicate row on a page whose reason for existing is that
	// a silent death should not look like health.
	withoutDigest := filepath.Join(dir, "no-document-digest.json")
	if err := os.WriteFile(withoutDigest, []byte(`{
	  "proof_run_total": 2,
	  "rows": [
	    {"selected_operation":"featureFlags","mode":"primary","schema_digest":"sha256:pin","current_candidate_build":"b1","proof_run_id":"p1"},
	    {"selected_operation":"featureFlags","mode":"python","schema_digest":"sha256:pin","current_candidate_build":"b1","proof_run_id":""}
	  ]
	}`), 0o600); err != nil {
		t.Fatalf("write the snapshot: %v", err)
	}
	_, _, err := readRoutingFile(withoutDigest, "sha256:pin")
	if err == nil {
		t.Fatal("a routing snapshot with no document_digest was accepted; two documents of one operation are then one row")
	}
	if !strings.Contains(err.Error(), "document_digest") || !strings.Contains(err.Error(), "print-routing-sql") {
		t.Fatalf("the refusal must name the missing field AND how to regenerate the snapshot, got: %v", err)
	}

	// The control: the SAME two rows, with their digests, are two rows.
	withDigest := filepath.Join(dir, "with-document-digest.json")
	if err := os.WriteFile(withDigest, []byte(`{
	  "proof_run_total": 2,
	  "rows": [
	    {"selected_operation":"featureFlags","document_digest":"doc-new","mode":"primary","schema_digest":"sha256:pin","current_candidate_build":"b1","proof_run_id":"p1"},
	    {"selected_operation":"featureFlags","document_digest":"doc-old","mode":"python","schema_digest":"sha256:pin","current_candidate_build":"b1","proof_run_id":""}
	  ]
	}`), 0o600); err != nil {
		t.Fatalf("write the snapshot: %v", err)
	}
	rows, total, err := readRoutingFile(withDigest, "sha256:pin")
	if err != nil {
		t.Fatalf("a complete snapshot was refused: %v", err)
	}
	if total != 2 || len(rows) != 2 {
		t.Fatalf("expected 2 rows and a total of 2, got %d rows and %d", len(rows), total)
	}
	if rows[0].DocumentDigest == rows[1].DocumentDigest {
		t.Fatalf("both rows carry document digest %q; the two documents collapsed", rows[0].DocumentDigest)
	}
	if rows[0].Proven == rows[1].Proven {
		t.Fatalf("both rows derived proof %q, so the digests are being read but not used", rows[0].Proven)
	}
}
