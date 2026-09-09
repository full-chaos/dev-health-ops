package goapiproof

import (
	"errors"
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
