package workgraph

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// recordingPreStep notes when it ran, relative to the other steps.
type recordingPreStep struct {
	name     string
	sequence *[]string
	fragment map[string]any
	err      error
}

func (step *recordingPreStep) Name() string { return step.name }

func (step *recordingPreStep) Run(context.Context, Claim) (map[string]any, error) {
	*step.sequence = append(*step.sequence, "prestep:"+step.name)
	return step.fragment, step.err
}

// TestBuildRunsPreStepsInOrder pins the ordering invariant NativePreStep
// documents: steps run in the order given. Before CHAOS-4924 this also
// asserted pre-steps ran BEFORE the Python bridge; there is no bridge left
// on Build to order against, so that half of the old test name/assertion is
// gone, not just renamed.
func TestBuildRunsPreStepsInOrder(t *testing.T) {
	var sequence []string
	store := &fakeStore{claim: testClaim(time.Minute)}
	handler, err := NewBuildHandler(
		store,
		[]NativePreStep{
			&recordingPreStep{name: "first", sequence: &sequence},
			&recordingPreStep{name: "second", sequence: &sequence},
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), buildExecution()); err != nil {
		t.Fatal(err)
	}

	want := []string{"prestep:first", "prestep:second"}
	if len(sequence) != len(want) {
		t.Fatalf("sequence = %v, want %v", sequence, want)
	}
	for index := range want {
		if sequence[index] != want[index] {
			t.Fatalf("sequence = %v, want %v", sequence, want)
		}
	}
	if store.completions != 1 || store.ambiguous != 0 {
		t.Fatalf("completions=%d ambiguous=%d", store.completions, store.ambiguous)
	}
}

// TestInvestmentHandlerCarriesNoPreSteps pins that the seam is build-only.
// Before CHAOS-4924 this was a runtime check on a shared handler type that
// COULD have carried pre-steps for either kind if not careful; buildHandler
// and materializeHandler are now separate types, and materializeHandler has
// no preSteps field AT ALL -- a structural, compile-time guarantee stronger
// than the runtime check this replaces.
func TestInvestmentHandlerCarriesNoPreSteps(t *testing.T) {
	store := &fakeStore{claim: testMaterializeClaim(time.Minute)}
	h, err := NewMaterializeHandler(store, blockingExecutor{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h.materializeHandler == nil {
		t.Fatal("NewMaterializeHandler returned a nil handler")
	}
	// h.materializeHandler has no preSteps field to check -- its absence
	// from the struct definition IS the assertion.
}

// TestPreStepFailureFailsTheBuildPermanently: the build must NOT continue past
// a failed mapping step. Continuing would emit an edge set silently missing
// every provider-attached link — a wrong answer that looks healthy. CHAOS-4924
// cutover: this is NEVER ambiguous for Build (no bridge means nothing
// half-applied to repair) — a non-transient error like this one is Permanent.
func TestPreStepFailureFailsTheBuildPermanently(t *testing.T) {
	var sequence []string
	store := &fakeStore{claim: testClaim(time.Minute)}
	handler, err := NewBuildHandler(
		store,
		[]NativePreStep{
			&recordingPreStep{name: "mapping", sequence: &sequence, err: errors.New("clickhouse refused the batch")},
		},
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	workErr := handler.Work(context.Background(), buildExecution())
	if workErr == nil {
		t.Fatal("Work returned nil after a pre-step failure")
	}
	if !strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work = %v, want category %s", workErr, jobruntime.CategoryPermanent)
	}
	if store.completions != 0 {
		t.Fatalf("completions=%d, want 0", store.completions)
	}
	if store.ambiguous != 0 {
		t.Fatalf("ambiguous=%d, want 0 -- Build never has a half-applied bridge write to repair", store.ambiguous)
	}
}

func TestNewBuildHandlerRejectsANilPreStep(t *testing.T) {
	// A nil step is a wiring bug that would silently skip ported compute —
	// exactly the failure this seam exists to prevent.
	if _, err := NewBuildHandler(&fakeStore{}, []NativePreStep{nil}, nil, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewBuildHandler with a nil pre-step = %v, want ErrUnavailable", err)
	}
}

func TestMergePreStepEvidence(t *testing.T) {
	fragments := map[string]map[string]any{"issue_pr_links": {"rows_written": 2493}}

	t.Run("merges under the step name", func(t *testing.T) {
		merged := mergePreStepEvidence([]byte(`{"outcome":"ok"}`), fragments)
		var payload map[string]any
		if err := json.Unmarshal(merged, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["outcome"] != "ok" {
			t.Fatalf("the executor's own evidence was lost: %s", merged)
		}
		step, ok := payload["issue_pr_links"].(map[string]any)
		if !ok {
			t.Fatalf("missing merged fragment: %s", merged)
		}
		if step["rows_written"] != float64(2493) {
			t.Fatalf("rows_written = %v", step["rows_written"])
		}
	})

	// The ledger row is durable execution evidence. Corrupting it to attach a
	// telemetry nicety is a bad trade, so a payload that is not a JSON object
	// is returned exactly as the executor produced it.
	for _, opaque := range []string{`[1,2,3]`, `"a string"`, `not json at all`} {
		t.Run("leaves a non-object payload untouched: "+opaque, func(t *testing.T) {
			merged := mergePreStepEvidence([]byte(opaque), fragments)
			if string(merged) != opaque {
				t.Fatalf("merged = %s, want the payload unchanged", merged)
			}
		})
	}

	t.Run("never overwrites a key the executor produced", func(t *testing.T) {
		merged := mergePreStepEvidence([]byte(`{"issue_pr_links":"python's own"}`), fragments)
		var payload map[string]any
		if err := json.Unmarshal(merged, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["issue_pr_links"] != "python's own" {
			t.Fatalf("the executor's key was overwritten: %s", merged)
		}
	})

	t.Run("no fragments leaves evidence identical", func(t *testing.T) {
		original := []byte(`{"outcome":"ok"}`)
		if merged := mergePreStepEvidence(original, nil); string(merged) != string(original) {
			t.Fatalf("merged = %s", merged)
		}
	})
}

// TestMergePreStepEvidencePreservesLargeIntegers is codex round-1 F3.
//
// Decoding the executor's payload into map[string]any turns every JSON number
// into a float64, which cannot represent integers above 2^53 exactly. Re-encoding
// then writes the ROUNDED value, and the result is still valid jsonb — so the
// ledger row silently stops matching what Python sent.
//
// `spend_limit_microunits` is a bigint the schema allows
// (0060_add_workgraph_investment_execution_state.py), and this repository
// already documents the same float64 hazard at publisher.go:137. Attaching a
// telemetry fragment must not corrupt the authoritative payload it is attached
// to.
func TestMergePreStepEvidencePreservesLargeIntegers(t *testing.T) {
	// 2^53 + 1: the smallest integer a float64 cannot represent.
	const exact = "9007199254740993"
	payload := []byte(`{"spend_limit_microunits":` + exact + `}`)

	merged := mergePreStepEvidence(payload, map[string]map[string]any{
		"issue_pr_links": {"rows_written": 2493},
	})

	if !strings.Contains(string(merged), exact) {
		t.Fatalf("merged evidence lost the exact integer %s: %s", exact, merged)
	}
	// And the fragment still arrived.
	if !strings.Contains(string(merged), "issue_pr_links") {
		t.Fatalf("merged evidence dropped the fragment: %s", merged)
	}
	if !json.Valid(merged) {
		t.Fatalf("merged evidence is not valid json: %s", merged)
	}
}
