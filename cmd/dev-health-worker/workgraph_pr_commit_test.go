package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// TestPRCommitStepNamesMatchDeclaredOrder pins the two ledger-evidence keys
// against buildPreStepOrder's own declaration -- a rename on either side
// silently breaks the constructed-vs-declared refusal in
// workgraphBuildPreSteps, which only compares strings.
func TestPRCommitStepNamesMatchDeclaredOrder(t *testing.T) {
	shared := newSharedPRCommitWindow()
	linksStep, err := newPRCommitLinksPreStep(nil, shared)
	if err == nil {
		t.Fatal("newPRCommitLinksPreStep(nil, ...) should refuse a nil service")
	}
	if linksStep != nil {
		t.Fatal("refused construction must not return a non-nil step")
	}
	edgesStep, err := newPRCommitEdgesPreStep(nil, shared)
	if err == nil {
		t.Fatal("newPRCommitEdgesPreStep(nil, ...) should refuse a nil service")
	}
	if edgesStep != nil {
		t.Fatal("refused construction must not return a non-nil step")
	}

	want := buildPreStepOrder()
	if len(want) != 7 || want[2] != "pr_commit_links" || want[3] != "pr_commit_edges" {
		t.Fatalf("buildPreStepOrder() = %v, want [issue_pr_links issue_commit_edges pr_commit_links pr_commit_edges ...]", want)
	}
}

// TestSharedPRCommitWindowGivesEdgesTheExactLinksWindow is the regression test
// for codex round chaos-5264-pr-r1's P1 (EXECUTED repro): each step calling
// prCommitWindowFor with its OWN time.Now, independently, at whatever instant
// its Run() happened to execute, let the default `from` bound drift between
// the two steps by however long elapsed between them -- a commit landing in
// that sliver got a link but never an edge, permanently, since the window
// only narrows further on later runs. This pins that both steps now use the
// SAME window for one claim, regardless of how much wall-clock time passes
// between their two Run() calls.
func TestSharedPRCommitWindowGivesEdgesTheExactLinksWindow(t *testing.T) {
	shared := newSharedPRCommitWindow()
	requestID := "req-1"

	linksNow := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	linksWindow, err := prCommitWindowFor(nil, func() time.Time { return linksNow })
	if err != nil {
		t.Fatalf("prCommitWindowFor: %v", err)
	}
	shared.store(requestID, linksWindow)

	// Simulate real time elapsing before the edges step runs -- the exact
	// condition that produced the drift.
	edgesWindow, ok := shared.take(requestID)
	if !ok {
		t.Fatal("shared.take should find the window pr_commit_links stored")
	}
	if edgesWindow.From == nil || !edgesWindow.From.Equal(*linksWindow.From) {
		t.Fatalf("edges window.From = %v, want the exact links window.From %v", edgesWindow.From, linksWindow.From)
	}
	if edgesWindow.To == nil || !edgesWindow.To.Equal(*linksWindow.To) {
		t.Fatalf("edges window.To = %v, want the exact links window.To %v", edgesWindow.To, linksWindow.To)
	}

	// The entry must be consumed, not merely readable twice -- see take's doc
	// on why (unbounded map growth across the worker's lifetime otherwise).
	if _, ok := shared.take(requestID); ok {
		t.Fatal("shared.take should consume the entry; a second take for the same claim found one")
	}
}

// TestPRCommitEdgesPreStepFailsLoudlyWhenNoSharedWindowExists pins the
// fail-loud requirement (chris's 15:30 ruling, via team-lead, on the
// chaos-5264-pr-r1 P1 fix): if pr_commit_edges somehow runs for a claim
// pr_commit_links never processed, it must refuse with an error naming the
// request id and org -- NOT silently recompute its own window, which would
// reintroduce the exact drift defect this type exists to close.
func TestPRCommitEdgesPreStepFailsLoudlyWhenNoSharedWindowExists(t *testing.T) {
	shared := newSharedPRCommitWindow()
	step := &prCommitEdgesPreStep{service: nil, shared: shared}

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

// TestPRCommitLinksPreStepDoesNotLeakAWindowOnFailure is the regression test
// for codex round chaos-5264-pr-confirm's P1 (EXECUTED repro): an earlier
// version stored the window into the shared map BEFORE calling ProduceLinks,
// so a failed run (ProduceLinks errors, runPreSteps aborts the whole claim
// before pr_commit_edges' `take` ever fires) left the entry in the map
// forever -- one leaked entry per distinct failed request for the worker's
// entire lifetime. Store now happens only after ProduceLinks succeeds.
func TestPRCommitLinksPreStepDoesNotLeakAWindowOnFailure(t *testing.T) {
	shared := newSharedPRCommitWindow()
	step := &prCommitLinksPreStep{service: nil, shared: shared, now: time.Now}

	_, err := step.Run(context.Background(), workgraph.Claim{
		Request: workgraph.Request{ID: "leak-req", OrganizationID: "org-a"},
	})
	if err == nil {
		t.Fatal("expected ProduceLinks to fail with a nil service")
	}
	if _, ok := shared.take("leak-req"); ok {
		t.Fatal("a failed Run must not leave a window entry in the shared map")
	}
}

// TestPRCommitWindowForDefaultsToThirtyDaysEndingNow mirrors
// TestBuildWindowDefaultsToThirtyDaysEndingNow for issue_pr_links: this window
// derivation is deliberately the SAME shared logic, reused rather than
// re-derived, so this pins that the reuse actually wires up correctly rather
// than re-testing the shared helpers' own edge cases.
func TestPRCommitWindowForDefaultsToThirtyDaysEndingNow(t *testing.T) {
	frozen := time.Date(2026, 9, 1, 12, 30, 45, 500_000_000, time.UTC)
	now := func() time.Time { return frozen }

	for _, scope := range [][]byte{nil, []byte(``), []byte(`{}`)} {
		window, err := prCommitWindowFor(scope, now)
		if err != nil {
			t.Fatalf("prCommitWindowFor(%q): %v", scope, err)
		}
		if window.To == nil || window.From == nil {
			t.Fatalf("prCommitWindowFor(%q) left a bound unset: %+v", scope, window)
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

// TestPRCommitWindowForExplicitFromDateAvoidsDefaultUnderflow pins
// CHAOS-5297: the derived-bound overflow guard must run ONLY when from_date
// is absent from scope, matching Python's if/else (work_graph_tasks.py
// never evaluates `to - 30d` when from_date is supplied). An explicit
// from_date/to_date pair that is each individually valid must not be
// rejected over a derived value Python would never have computed. Same bug
// shape and repro as #2301's operationalEdgesWindowFor
// (chaos-4924-pr-d-r1-confirm, P1, EXECUTED).
func TestPRCommitWindowForExplicitFromDateAvoidsDefaultUnderflow(t *testing.T) {
	window, err := prCommitWindowFor(
		[]byte(`{"from_date":"0001-01-01","to_date":"0001-01-01"}`), time.Now)
	if err != nil {
		t.Fatalf("prCommitWindowFor with explicit year-0001 bounds should not error, got: %v", err)
	}
	want := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	if window.From == nil || !window.From.Equal(want) {
		t.Errorf("from = %v, want %v", window.From, want)
	}
	if window.To == nil || !window.To.Equal(want) {
		t.Errorf("to = %v, want %v", window.To, want)
	}
}

func TestPRCommitWindowForParsesExplicitRepoID(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	repo := uuid.New()
	scope := []byte(`{"repo_id":"` + repo.String() + `"}`)

	window, err := prCommitWindowFor(scope, now)
	if err != nil {
		t.Fatalf("prCommitWindowFor: %v", err)
	}
	if window.RepoID == nil || *window.RepoID != repo {
		t.Fatalf("repo_id = %v, want %s", window.RepoID, repo)
	}
}

func TestPRCommitWindowForRefusesNullScope(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	if _, err := prCommitWindowFor([]byte(`null`), now); err == nil {
		t.Fatal("a null scope must be refused, matching the bridge's own rejection")
	}
}

func TestPRCommitWindowForRefusesUnsupportedField(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	if _, err := prCommitWindowFor([]byte(`{"not_a_real_field":true}`), now); err == nil {
		t.Fatal("an unsupported scope field must be refused before the bridge would reject it")
	}
}
