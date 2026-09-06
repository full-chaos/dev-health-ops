package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// TestIssuePREdgesStepNamesMatchDeclaredOrder pins the two ledger-evidence
// keys against buildPreStepOrder's own declaration -- same reasoning as
// TestPRCommitStepNamesMatchDeclaredOrder.
func TestIssuePREdgesStepNamesMatchDeclaredOrder(t *testing.T) {
	shared := newSharedIssuePREdgesWindow()
	fastPathStep, err := newIssuePREdgesFastPathPreStep(nil, shared)
	if err == nil {
		t.Fatal("newIssuePREdgesFastPathPreStep(nil, ...) should refuse a nil service")
	}
	if fastPathStep != nil {
		t.Fatal("refused construction must not return a non-nil step")
	}
	textParseStep, err := newIssuePREdgesTextParsePreStep(nil, shared)
	if err == nil {
		t.Fatal("newIssuePREdgesTextParsePreStep(nil, ...) should refuse a nil service")
	}
	if textParseStep != nil {
		t.Fatal("refused construction must not return a non-nil step")
	}
	heuristicStep, err := newIssuePREdgesHeuristicPreStep(nil, shared)
	if err == nil {
		t.Fatal("newIssuePREdgesHeuristicPreStep(nil, ...) should refuse a nil service")
	}
	if heuristicStep != nil {
		t.Fatal("refused construction must not return a non-nil step")
	}

	want := buildPreStepOrder()
	if len(want) != 8 || want[1] != "issue_pr_edges_fast_path" || want[2] != "issue_pr_edges_text_parse" || want[3] != "issue_pr_edges_heuristic" {
		t.Fatalf("buildPreStepOrder() = %v, want [issue_pr_links issue_pr_edges_fast_path issue_pr_edges_text_parse issue_pr_edges_heuristic ...]", want)
	}
}

// TestSharedIssuePREdgesWindowGivesAllThreeStepsTheExactFastPathWindow
// mirrors TestSharedPRCommitWindowGivesEdgesTheExactLinksWindow, extended to
// three consumers: fast_path stores, text_parse peeks (must NOT delete --
// heuristic still needs the entry), heuristic takes (deletes). All three
// must see the SAME window for one claim, regardless of how much wall-clock
// time passes between their Run() calls, closing the identical drift defect
// class chaos-5264-pr-r1 found for pr_commit_links/pr_commit_edges.
func TestSharedIssuePREdgesWindowGivesAllThreeStepsTheExactFastPathWindow(t *testing.T) {
	shared := newSharedIssuePREdgesWindow()
	requestID := "req-1"

	fastPathNow := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	fastPathWindow, err := issuePREdgesWindowFor(nil, func() time.Time { return fastPathNow })
	if err != nil {
		t.Fatalf("issuePREdgesWindowFor: %v", err)
	}
	shared.store(requestID, fastPathWindow)

	textParseWindow, ok := shared.peek(requestID)
	if !ok {
		t.Fatal("shared.peek should find the window the fast-path step stored")
	}
	if textParseWindow.From == nil || !textParseWindow.From.Equal(*fastPathWindow.From) {
		t.Fatalf("text-parse window.From = %v, want the exact fast-path window.From %v", textParseWindow.From, fastPathWindow.From)
	}
	if textParseWindow.To == nil || !textParseWindow.To.Equal(*fastPathWindow.To) {
		t.Fatalf("text-parse window.To = %v, want the exact fast-path window.To %v", textParseWindow.To, fastPathWindow.To)
	}

	// peek must NOT have consumed the entry -- heuristic still needs it.
	heuristicWindow, ok := shared.take(requestID)
	if !ok {
		t.Fatal("shared.take should still find the entry after a peek; peek must not delete")
	}
	if heuristicWindow.From == nil || !heuristicWindow.From.Equal(*fastPathWindow.From) {
		t.Fatalf("heuristic window.From = %v, want the exact fast-path window.From %v", heuristicWindow.From, fastPathWindow.From)
	}

	if _, ok := shared.take(requestID); ok {
		t.Fatal("shared.take should consume the entry; a second take for the same claim found one")
	}
}

// TestIssuePREdgesTextParsePreStepFailsLoudlyWhenNoSharedWindowExists mirrors
// TestPRCommitEdgesPreStepFailsLoudlyWhenNoSharedWindowExists: refuse loudly
// rather than silently recomputing an independent window.
func TestIssuePREdgesTextParsePreStepFailsLoudlyWhenNoSharedWindowExists(t *testing.T) {
	shared := newSharedIssuePREdgesWindow()
	step := &issuePREdgesTextParsePreStep{service: nil, shared: shared}

	_, err := step.Run(context.Background(), workgraph.Claim{
		Request: workgraph.Request{ID: "no-such-claim", OrganizationID: "org-a"},
	})
	if err == nil {
		t.Fatal("Run must refuse when no shared window was stored for this claim")
	}
	if !strings.Contains(err.Error(), "no-such-claim") || !strings.Contains(err.Error(), "org-a") {
		t.Fatalf("error should name the request id and org for diagnosis, got: %v", err)
	}
}

// TestIssuePREdgesHeuristicPreStepFailsLoudlyWhenNoSharedWindowExists is the
// same fail-loud requirement for the third (terminal) consumer.
func TestIssuePREdgesHeuristicPreStepFailsLoudlyWhenNoSharedWindowExists(t *testing.T) {
	shared := newSharedIssuePREdgesWindow()
	step := &issuePREdgesHeuristicPreStep{service: nil, shared: shared}

	_, err := step.Run(context.Background(), workgraph.Claim{
		Request: workgraph.Request{ID: "no-such-claim", OrganizationID: "org-a"},
	})
	if err == nil {
		t.Fatal("Run must refuse when no shared window was stored for this claim")
	}
	if !strings.Contains(err.Error(), "no-such-claim") || !strings.Contains(err.Error(), "org-a") {
		t.Fatalf("error should name the request id and org for diagnosis, got: %v", err)
	}
}

// TestIssuePREdgesFastPathPreStepDoesNotLeakAWindowOnFailure mirrors
// TestPRCommitLinksPreStepDoesNotLeakAWindowOnFailure: store must happen only
// after the produce call succeeds, or a claim that fails once and is never
// retried leaks its map entry forever.
func TestIssuePREdgesFastPathPreStepDoesNotLeakAWindowOnFailure(t *testing.T) {
	shared := newSharedIssuePREdgesWindow()
	step := &issuePREdgesFastPathPreStep{service: nil, shared: shared, now: time.Now}

	_, err := step.Run(context.Background(), workgraph.Claim{
		Request: workgraph.Request{ID: "leak-req", OrganizationID: "org-a"},
	})
	if err == nil {
		t.Fatal("expected ProduceFastPathEdges to fail with a nil service")
	}
	if _, ok := shared.take("leak-req"); ok {
		t.Fatal("a failed Run must not leave a window entry in the shared map")
	}
}

// TestIssuePREdgesTextParsePreStepDoesNotLeakAWindowOnFailure is the leak
// scenario unique to a THREE-consumer chain: text_parse only peeks (does not
// delete), so if ITS OWN produce call fails, the entry it peeked would
// otherwise survive forever -- issue_pr_edges_heuristic (the only step that
// ever calls take) never runs for this claim, since runPreSteps aborts the
// whole claim on any step error. text_parse's Run must clean the entry up
// itself on this specific failure path.
func TestIssuePREdgesTextParsePreStepDoesNotLeakAWindowOnFailure(t *testing.T) {
	shared := newSharedIssuePREdgesWindow()
	window, err := issuePREdgesWindowFor(nil, time.Now)
	if err != nil {
		t.Fatalf("issuePREdgesWindowFor: %v", err)
	}
	shared.store("leak-req-2", window)

	step := &issuePREdgesTextParsePreStep{service: nil, shared: shared}
	_, err = step.Run(context.Background(), workgraph.Claim{
		Request: workgraph.Request{ID: "leak-req-2", OrganizationID: "org-a"},
	})
	if err == nil {
		t.Fatal("expected ProduceTextParseEdges to fail with a nil service")
	}
	if _, ok := shared.take("leak-req-2"); ok {
		t.Fatal("a failed text-parse Run must not leave a window entry in the shared map, since heuristic will never run to clean it up")
	}
}

func TestIssuePREdgesWindowForDefaultsToThirtyDaysEndingNow(t *testing.T) {
	frozen := time.Date(2026, 9, 1, 12, 30, 45, 500_000_000, time.UTC)
	now := func() time.Time { return frozen }

	for _, scope := range [][]byte{nil, []byte(``), []byte(`{}`)} {
		window, err := issuePREdgesWindowFor(scope, now)
		if err != nil {
			t.Fatalf("issuePREdgesWindowFor(%q): %v", scope, err)
		}
		if window.To == nil || window.From == nil {
			t.Fatalf("issuePREdgesWindowFor(%q) left a bound unset: %+v", scope, window)
		}
		if !window.To.Equal(frozen) {
			t.Errorf("to = %s, want now (%s)", window.To, frozen)
		}
		if want := frozen.AddDate(0, 0, -30); !window.From.Equal(want) {
			t.Errorf("from = %s, want to-30d (%s)", window.From, want)
		}
		if window.RepoID != nil {
			t.Errorf("repo_id should be unset by default, got %v", window.RepoID)
		}
	}
}

// TestIssuePREdgesWindowForExplicitFromDateAvoidsDefaultUnderflow pins the
// derived-bound guard ordering fix applied proactively in this new function
// (see issuePREdgesWindowFor's doc comment): an explicit from_date/to_date
// pair that is each individually valid must not be rejected over a derived
// value that is never computed when from_date is supplied.
func TestIssuePREdgesWindowForExplicitFromDateAvoidsDefaultUnderflow(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	window, err := issuePREdgesWindowFor(
		[]byte(`{"from_date":"0001-01-01","to_date":"0001-01-01"}`), now)
	if err != nil {
		t.Fatalf("issuePREdgesWindowFor with explicit year-0001 bounds should not error, got: %v", err)
	}
	want := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	if window.From == nil || !window.From.Equal(want) {
		t.Errorf("From = %v, want %v", window.From, want)
	}
	if window.To == nil || !window.To.Equal(want) {
		t.Errorf("To = %v, want %v", window.To, want)
	}
}

func TestIssuePREdgesWindowForParsesExplicitRepoID(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	repo := uuid.New()
	scope := []byte(`{"repo_id":"` + repo.String() + `"}`)

	window, err := issuePREdgesWindowFor(scope, now)
	if err != nil {
		t.Fatalf("issuePREdgesWindowFor: %v", err)
	}
	if window.RepoID == nil || *window.RepoID != repo {
		t.Fatalf("repo_id = %v, want %s", window.RepoID, repo)
	}
}

func TestIssuePREdgesWindowForRefusesNullScope(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	if _, err := issuePREdgesWindowFor([]byte(`null`), now); err == nil {
		t.Fatal("a null scope must be refused, matching the bridge's own rejection")
	}
}

func TestIssuePREdgesWindowForRefusesUnsupportedField(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	if _, err := issuePREdgesWindowFor([]byte(`{"not_a_real_field":true}`), now); err == nil {
		t.Fatal("an unsupported scope field must be refused before the bridge would reject it")
	}
}
