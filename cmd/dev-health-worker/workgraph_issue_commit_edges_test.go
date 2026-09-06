package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestIssueCommitStepNamesMatchDeclaredOrder pins the two ledger-evidence keys
// this file adds against buildPreStepOrder's own declaration -- a rename on
// either side silently breaks the constructed-vs-declared refusal in
// workgraphBuildPreSteps, which only compares strings. Same shape as
// TestPRCommitStepNamesMatchDeclaredOrder.
func TestIssueCommitStepNamesMatchDeclaredOrder(t *testing.T) {
	if _, err := newIssueCommitEdgesPreStep(nil); err == nil {
		t.Fatal("newIssueCommitEdgesPreStep(nil) should refuse a nil service")
	}
	if _, err := newCommitFileEdgesPreStep(nil); err == nil {
		t.Fatal("newCommitFileEdgesPreStep(nil) should refuse a nil connection")
	}

	want := buildPreStepOrder()
	if len(want) != 12 || want[4] != "issue_commit_edges" || want[8] != "commit_file_edges" {
		t.Fatalf("buildPreStepOrder() = %v, want [... issue_commit_edges ... commit_file_edges ...]", want)
	}
}

func TestIssueCommitEdgesPreStepNameIsStable(t *testing.T) {
	// The name is a ledger evidence key; changing it orphans historical rows.
	if got := (&issueCommitEdgesPreStep{}).Name(); got != "issue_commit_edges" {
		t.Fatalf("Name() = %q", got)
	}
}

func TestCommitFileEdgesPreStepNameIsStable(t *testing.T) {
	if got := (&commitFileEdgesPreStep{}).Name(); got != "commit_file_edges" {
		t.Fatalf("Name() = %q", got)
	}
}

// TestIssueCommitWindowForDefaultsToThirtyDaysEndingNow: this window
// derivation is deliberately the SAME shared logic issue_pr_links/pr_commit
// reuse, so this pins that the reuse actually wires up correctly rather than
// re-testing the shared helpers' own edge cases.
func TestIssueCommitWindowForDefaultsToThirtyDaysEndingNow(t *testing.T) {
	frozen := time.Date(2026, 9, 1, 12, 30, 45, 500_000_000, time.UTC)
	now := func() time.Time { return frozen }

	for _, scope := range [][]byte{nil, []byte(``), []byte(`{}`)} {
		window, err := issueCommitWindowFor(scope, now)
		if err != nil {
			t.Fatalf("issueCommitWindowFor(%q): %v", scope, err)
		}
		if window.To == nil || window.From == nil {
			t.Fatalf("issueCommitWindowFor(%q) left a bound unset: %+v", scope, window)
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

// TestIssueCommitWindowForExplicitFromDateAvoidsDefaultUnderflow pins
// CHAOS-5297 for this reuse site too: the derived-bound overflow guard must
// run ONLY when from_date is absent from scope.
func TestIssueCommitWindowForExplicitFromDateAvoidsDefaultUnderflow(t *testing.T) {
	window, err := issueCommitWindowFor(
		[]byte(`{"from_date":"0001-01-01","to_date":"0001-01-01"}`), time.Now)
	if err != nil {
		t.Fatalf("issueCommitWindowFor with explicit year-0001 bounds should not error, got: %v", err)
	}
	want := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	if window.From == nil || !window.From.Equal(want) {
		t.Errorf("from = %v, want %v", window.From, want)
	}
	if window.To == nil || !window.To.Equal(want) {
		t.Errorf("to = %v, want %v", window.To, want)
	}
}

func TestIssueCommitWindowForParsesExplicitRepoID(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	repo := uuid.New()
	scope := []byte(`{"repo_id":"` + repo.String() + `"}`)

	window, err := issueCommitWindowFor(scope, now)
	if err != nil {
		t.Fatalf("issueCommitWindowFor: %v", err)
	}
	if window.RepoID == nil || *window.RepoID != repo {
		t.Fatalf("repo_id = %v, want %s", window.RepoID, repo)
	}
}

func TestIssueCommitWindowForRefusesNullScope(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	if _, err := issueCommitWindowFor([]byte(`null`), now); err == nil {
		t.Fatal("a null scope must be refused, matching the bridge's own rejection")
	}
}

func TestIssueCommitWindowForRefusesUnsupportedField(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC) }
	if _, err := issueCommitWindowFor([]byte(`{"not_a_real_field":true}`), now); err == nil {
		t.Fatal("an unsupported scope field must be refused before the bridge would reject it")
	}
}
