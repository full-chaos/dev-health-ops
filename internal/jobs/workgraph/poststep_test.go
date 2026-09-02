package workgraph

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type recordingPostStep struct {
	name     string
	sequence *[]string
	fragment map[string]any
	err      error
}

func (step *recordingPostStep) Name() string { return step.name }

func (step *recordingPostStep) Run(context.Context, Claim) (map[string]any, error) {
	*step.sequence = append(*step.sequence, "poststep:"+step.name)
	return step.fragment, step.err
}

// TestPostStepsRunAfterTheBridgeAndPreStepsBeforeIt is the whole point of the
// second seam, so it is asserted on the observed sequence rather than inferred
// from where the call sits in the source.
//
// If a post-step ran before the executor it would be a pre-step with a
// different name, and the defect it exists to prevent — Python overwriting a
// deliberately divergent value — would be back with no test failing.
func TestPostStepsRunAfterTheBridgeAndPreStepsBeforeIt(t *testing.T) {
	var sequence []string
	store := &fakeStore{claim: testClaim(time.Minute)}

	handler, err := NewBuildHandler(
		store,
		sequencingExecutor{sequence: &sequence},
		[]NativePreStep{&recordingPreStep{name: "mapping", sequence: &sequence}},
		[]NativePostStep{&recordingPostStep{name: "edges", sequence: &sequence}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), buildExecution()); err != nil {
		t.Fatal(err)
	}

	want := []string{"prestep:mapping", "executor", "poststep:edges"}
	if len(sequence) != len(want) {
		t.Fatalf("sequence %v, want %v", sequence, want)
	}
	for index, step := range want {
		if sequence[index] != step {
			t.Fatalf("sequence %v, want %v", sequence, want)
		}
	}
}

// TestAPostStepFailureFailsTheBuild. The bridge has already written by this
// point, so the rows exist carrying PYTHON's values. That is a wrong answer
// that looks healthy, not a missing one, and reporting success would leave the
// build indistinguishable from one where the native policy applied.
func TestAPostStepFailureFailsTheBuild(t *testing.T) {
	var sequence []string
	store := &fakeStore{claim: testClaim(time.Minute)}

	handler, err := NewBuildHandler(
		store,
		sequencingExecutor{sequence: &sequence},
		nil,
		[]NativePostStep{&recordingPostStep{
			name: "edges", sequence: &sequence, err: errors.New("clickhouse refused the batch"),
		}},
	)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if err := handler.Work(context.Background(), buildExecution()); err == nil {
		t.Fatal("a post-step failure was reported as a successful build; the rows carry " +
			"Python's values and nothing would say so")
	}
	if store.completions != 0 {
		t.Errorf("completions=%d, want 0 — the build must not complete", store.completions)
	}
	if store.ambiguous != 1 {
		t.Errorf("ambiguous=%d, want 1 — a step that writes in batches may have written some rows",
			store.ambiguous)
	}
}

// TestANilPostStepIsRefused mirrors the pre-step refusal. A nil step is a
// wiring bug that silently skips ported compute.
func TestANilPostStepIsRefused(t *testing.T) {
	if _, err := NewBuildHandler(
		&fakeStore{}, blockingExecutor{}, nil, []NativePostStep{nil},
	); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a nil post-step was accepted, got %v", err)
	}
}

// TestPostStepEvidenceIsMergedUnderItsName, and does not displace the
// executor's own keys — the Python payload stays authoritative.
func TestPostStepEvidenceIsMergedUnderItsName(t *testing.T) {
	var sequence []string
	store := &fakeStore{claim: testClaim(time.Minute)}

	handler, err := NewBuildHandler(
		store,
		sequencingExecutor{sequence: &sequence, evidence: []byte(`{"outcome":"ok"}`)},
		nil,
		[]NativePostStep{&recordingPostStep{
			name: "edges", sequence: &sequence, fragment: map[string]any{"edges_written": 3},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), buildExecution()); err != nil {
		t.Fatal(err)
	}

	var payload map[string]any
	if err := json.Unmarshal(store.lastEvidence, &payload); err != nil {
		t.Fatalf("decode evidence: %v", err)
	}
	if payload["outcome"] != "ok" {
		t.Errorf("the executor's own key was lost: %v", payload)
	}
	fragment, present := payload["edges"].(map[string]any)
	if !present {
		t.Fatalf("no evidence fragment under the step's name: %v", payload)
	}
	if fragment["edges_written"] == nil {
		t.Errorf("fragment merged but empty: %v", fragment)
	}
}
