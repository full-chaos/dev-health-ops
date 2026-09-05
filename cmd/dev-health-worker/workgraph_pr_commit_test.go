package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestPRCommitStepNamesMatchDeclaredOrder pins the two ledger-evidence keys
// against buildPreStepOrder's own declaration -- a rename on either side
// silently breaks the constructed-vs-declared refusal in
// workgraphBuildPreSteps, which only compares strings.
func TestPRCommitStepNamesMatchDeclaredOrder(t *testing.T) {
	linksStep, err := newPRCommitLinksPreStep(nil)
	if err == nil {
		t.Fatal("newPRCommitLinksPreStep(nil) should refuse a nil service")
	}
	if linksStep != nil {
		t.Fatal("refused construction must not return a non-nil step")
	}
	edgesStep, err := newPRCommitEdgesPreStep(nil)
	if err == nil {
		t.Fatal("newPRCommitEdgesPreStep(nil) should refuse a nil service")
	}
	if edgesStep != nil {
		t.Fatal("refused construction must not return a non-nil step")
	}

	want := buildPreStepOrder()
	if len(want) != 3 || want[1] != "pr_commit_links" || want[2] != "pr_commit_edges" {
		t.Fatalf("buildPreStepOrder() = %v, want [issue_pr_links pr_commit_links pr_commit_edges]", want)
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
