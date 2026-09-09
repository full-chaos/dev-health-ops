package goapiproof

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func validRequest() RepointRequest {
	return RepointRequest{
		SchemaDigest:   "sha256:29d509cd",
		RunningBuild:   "ffd9e5d5dc8ee21de5befa1bae47ba9195be135e",
		RecordedBy:     "lane-stack-owner",
		ReviewEvidence: "CHAOS-5486 re-point to the running build",
	}
}

func TestRepointRequestRequiresEveryProvenanceField(t *testing.T) {
	for name, mutate := range map[string]func(*RepointRequest){
		"schema digest":   func(r *RepointRequest) { r.SchemaDigest = "" },
		"running build":   func(r *RepointRequest) { r.RunningBuild = "" },
		"recorded by":     func(r *RepointRequest) { r.RecordedBy = "" },
		"review evidence": func(r *RepointRequest) { r.ReviewEvidence = "" },
	} {
		t.Run(name, func(t *testing.T) {
			request := validRequest()
			mutate(&request)
			if err := request.validate(); err == nil {
				t.Fatalf("validate() = nil, want an error when %s is empty", name)
			}
		})
	}
}

// The cross-check flag may FAIL a run and may never supply the value that
// gets written -- the same rule go-api-prove's --candidate-build follows,
// and the reason a receipt cannot name a hand-typed sha.
func TestRepointRequestCrossCheckCanOnlyRefuse(t *testing.T) {
	request := validRequest()
	request.ExpectBuild = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	err := request.validate()
	if !errors.Is(err, ErrRepointBuildMismatch) {
		t.Fatalf("validate() = %v, want ErrRepointBuildMismatch", err)
	}
	if !strings.Contains(err.Error(), "b18e56fa") || !strings.Contains(err.Error(), "ffd9e5d5") {
		t.Fatalf("error must name both builds so the operator can see which is which, got %q", err)
	}

	request.ExpectBuild = request.RunningBuild
	if err := request.validate(); err != nil {
		t.Fatalf("a matching cross-check must pass, got %v", err)
	}
}

// mode/owner/rollout/eligible_orgs must not appear in the write. This is
// the whole contract of a mode-PRESERVING re-point, and it is cheap to
// pin against the statement text itself: a future edit that adds one of
// those columns to the SET list fails here rather than in production.
func TestRepointStatementTouchesProvenanceColumnsOnly(t *testing.T) {
	for _, forbidden := range []string{"mode", "owner", "rollout_percentage", "eligible_orgs"} {
		if strings.Contains(repointRoutingRowSQL, forbidden+" =") {
			t.Fatalf("repointRoutingRowSQL must never assign %q -- re-pointing is provenance, not reachability", forbidden)
		}
	}
	for _, required := range []string{"current_candidate_build =", "review_evidence =", "recorded_by =", "updated_at ="} {
		if !strings.Contains(repointRoutingRowSQL, required) {
			t.Fatalf("repointRoutingRowSQL must assign %q", required)
		}
	}
	if !strings.Contains(repointRoutingRowSQL, "WHERE schema_digest = $1") ||
		!strings.Contains(repointRoutingRowSQL, "AND document_digest = $2") ||
		!strings.Contains(repointRoutingRowSQL, "AND selected_operation = $3") {
		t.Fatal("the update must be keyed on the full 3-column primary key, or it can touch more than one row")
	}
}

// The candidate build must be registered before the routing row moves,
// because the routing row carries a 4-column FK to it. Registering second
// violates the constraint; the Python admin layer documents the same
// ordering and this pins the Go copy to it.
func TestRegisterCandidateBuildIsIdempotentAndComesFirst(t *testing.T) {
	if !strings.Contains(registerCandidateBuildSQL, "ON CONFLICT") ||
		!strings.Contains(registerCandidateBuildSQL, "DO NOTHING") {
		t.Fatal("registering a build twice must be a no-op, not an error: operators re-run recoveries")
	}
	if !strings.Contains(registerCandidateBuildSQL, "go_api_candidate_build") {
		t.Fatal("registration must target go_api_candidate_build")
	}
}

func TestSelectLocksTheRowsItIsAboutToMove(t *testing.T) {
	if !strings.Contains(selectRepointCandidatesSQL, "FOR UPDATE") {
		t.Fatal("candidates must be locked: a concurrent enable between the read and the write would be lost")
	}
}

// The mode assertion must lock what it asserts on, not lean on a lock taken by
// a different statement -- that lock stops covering it the moment the other
// statement's scope narrows.
//
// Re-pointed at the INVARIANT rather than a literal (codex r3, CONC-01).
// It used to grep this file for one hand-written SQL string; that
// statement is gone, replaced by a reuse of the shared ordered, LOCKING
// predicate -- which made the property STRONGER (the assertion cannot
// introduce a lock order of its own, and it now compares rows by their
// full identity rather than collapsing duplicates by operation). A
// source-text oracle that fails on a refactor which strengthens the
// property is measuring the text, not the claim.
func TestModeAssertionLocksTheRowsItReads(t *testing.T) {
	source, err := os.ReadFile("routing_repoint.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "goapiproof: re-read modes") {
		t.Fatal("the mode re-read is gone: this verb's contract is that it never touches reachability, and the assertion is what proves it every run")
	}
	if !strings.Contains(string(source), `readRoutingRows(ctx, tx, selectRepointCandidatesSQL, request.SchemaDigest)`) {
		t.Fatal("the mode re-read must run selectRepointCandidatesSQL -- the same ordered, LOCKING predicate the writes were driven from")
	}
	if !strings.Contains(string(source), "observed[rowKey{row.operation, row.documentDigest}]") {
		t.Fatal("the re-read must be keyed by the row's FULL identity: keyed by operation alone it collapses duplicate document digests and can compare the wrong row")
	}
	if !strings.Contains(selectRepointCandidatesSQL, "FOR UPDATE") {
		t.Fatal("the mode re-read must carry FOR UPDATE")
	}
}

func TestSummarizeReportsZerosExplicitly(t *testing.T) {
	summary := Summarize(nil)
	if summary.Total != 0 || summary.Changed != 0 || summary.Unchanged != 0 {
		t.Fatalf("empty summary = %+v, want all zeros", summary)
	}
	summary = Summarize([]RepointOutcome{
		{Operation: "a", Changed: true},
		{Operation: "b", Changed: false},
		{Operation: "c", Changed: false},
	})
	if summary.Total != 3 || summary.Changed != 1 || summary.Unchanged != 2 {
		t.Fatalf("summary = %+v, want Total 3 / Changed 1 / Unchanged 2", summary)
	}
}

func TestRepointRefusesNilPool(t *testing.T) {
	if _, err := Repoint(t.Context(), nil, validRequest()); err == nil {
		t.Fatal("Repoint(nil pool) = nil error, want a refusal")
	}
}

// Trap #120 sweep, CHAOS-5486: every site that KEYS or ORDERS by
// operation must carry the row's full identity, because this table's
// primary key is (schema_digest, document_digest, selected_operation) and
// one operation therefore has as many rows at a digest as it has document
// digests.
//
// `sort.Slice` is not stable, so an ordering that ties on operation alone
// leaves the tied rows in whatever order the algorithm happens to leave
// them: the report's row order stops being a function of the data, an
// operator diffing two runs sees rows swap for no reason, and no test can
// pin the output at all.
//
// The input below is REVERSED within each operation on purpose. That is
// what makes this a killer rather than a coincidence: with an
// operation-only comparator the two tied rows are already "in order" as
// far as the comparator can tell, so nothing moves them and the reversal
// survives into the result.
func TestRepointOutcomesAreOrderedByTheRowsFullIdentity(t *testing.T) {
	outcomes := []RepointOutcome{
		{Operation: "flowMatrix", DocumentDigest: "dddd"},
		{Operation: "flowMatrix", DocumentDigest: "bbbb"},
		{Operation: "featureFlags", DocumentDigest: "cccc"},
		{Operation: "featureFlags", DocumentDigest: "aaaa"},
	}
	sortOutcomes(outcomes)

	want := [][2]string{
		{"featureFlags", "aaaa"},
		{"featureFlags", "cccc"},
		{"flowMatrix", "bbbb"},
		{"flowMatrix", "dddd"},
	}
	for index, expected := range want {
		got := [2]string{outcomes[index].Operation, outcomes[index].DocumentDigest}
		if got != expected {
			t.Fatalf("outcome %d = %v, want %v -- the report's order must be a function of the ROW, not of the operation alone", index, got, expected)
		}
	}
}
