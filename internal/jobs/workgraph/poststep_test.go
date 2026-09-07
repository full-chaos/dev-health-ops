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

// TestPostStepsRunAfterPreSteps is the whole point of the second seam, so it
// is asserted on the observed sequence rather than inferred from where the
// call sits in the source. Before CHAOS-4924 this also ordered post-steps
// against the Python bridge in between; there is no bridge left on Build, so
// the sequence is just preSteps then postSteps now.
func TestPostStepsRunAfterPreSteps(t *testing.T) {
	var sequence []string
	store := &fakeStore{claim: testClaim(time.Minute)}

	handler, err := NewBuildHandler(
		store,
		[]NativePreStep{&recordingPreStep{name: "mapping", sequence: &sequence}},
		[]NativePostStep{&recordingPostStep{name: "edges", sequence: &sequence}},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), buildExecution()); err != nil {
		t.Fatal(err)
	}

	want := []string{"prestep:mapping", "poststep:edges"}
	if len(sequence) != len(want) {
		t.Fatalf("sequence %v, want %v", sequence, want)
	}
	for index, step := range want {
		if sequence[index] != step {
			t.Fatalf("sequence %v, want %v", sequence, want)
		}
	}
}

// TestAPostStepFailureFailsTheBuildPermanently. CHAOS-4924 cutover: a
// post-step failure classifies exactly like a pre-step failure now (no
// bridge means no half-applied Python write to worry about) — Permanent for
// a non-transient error, never Ambiguous. Before the cutover this was
// Ambiguous because the bridge had already written Python's values by the
// time a post-step ran; that reasoning no longer applies with no bridge.
func TestAPostStepFailureFailsTheBuildPermanently(t *testing.T) {
	var sequence []string
	store := &fakeStore{claim: testClaim(time.Minute)}

	handler, err := NewBuildHandler(
		store,
		nil,
		[]NativePostStep{&recordingPostStep{
			name: "edges", sequence: &sequence, err: errors.New("clickhouse refused the batch"),
		}},
		nil,
	)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	workErr := handler.Work(context.Background(), buildExecution())
	if workErr == nil {
		t.Fatal("a post-step failure was reported as a successful build")
	}
	if !strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work = %v, want category %s", workErr, jobruntime.CategoryPermanent)
	}
	if store.completions != 0 {
		t.Errorf("completions=%d, want 0 — the build must not complete", store.completions)
	}
	if store.ambiguous != 0 {
		t.Errorf("ambiguous=%d, want 0 -- Build never has a half-applied bridge write to repair",
			store.ambiguous)
	}
}

// TestANilPostStepIsRefused mirrors the pre-step refusal. A nil step is a
// wiring bug that silently skips ported compute.
func TestANilPostStepIsRefused(t *testing.T) {
	if _, err := NewBuildHandler(
		&fakeStore{}, nil, []NativePostStep{nil}, nil,
	); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a nil post-step was accepted, got %v", err)
	}
}

// TestPostStepEvidenceIsMergedUnderItsName. There is no bridge payload to
// displace anymore (CHAOS-4924) -- the base evidence is the "{}" sentinel
// buildHandler.work uses precisely so a fragment-only build still persists
// its fragments (mergePreStepEvidence returns an empty base UNCHANGED, which
// would otherwise silently drop every fragment).
func TestPostStepEvidenceIsMergedUnderItsName(t *testing.T) {
	var sequence []string
	store := &fakeStore{claim: testClaim(time.Minute)}

	handler, err := NewBuildHandler(
		store,
		nil,
		[]NativePostStep{&recordingPostStep{
			name: "edges", sequence: &sequence, fragment: map[string]any{"edges_written": 3},
		}},
		nil,
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
	fragment, present := payload["edges"].(map[string]any)
	if !present {
		t.Fatalf("no evidence fragment under the step's name: %v", payload)
	}
	if fragment["edges_written"] == nil {
		t.Errorf("fragment merged but empty: %v", fragment)
	}
}
