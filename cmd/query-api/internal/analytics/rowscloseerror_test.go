package analytics

import (
	"context"
	"errors"
	"testing"
	"time"
)

// errInjectedCloseFailure is a fixed sentinel for fakeRowScanner.closeErr,
// used across the table-driven Close()-only failure tests below -- the
// exact message never matters, only that Close() returns non-nil while
// Next/Scan/Err all succeed.
var errInjectedCloseFailure = errors.New("rowscloseerror_test: injected close failure")

// TestCloseOnlyFailure_IsReportedAcrossDecorators is table-driven across
// the three single-row telemetry decorators in this package that each
// discard rows.Close()'s return value: a stream that fails ONLY on
// Close() (Next/Scan/Err all succeed cleanly) must still be reported
// through the same failure path every other error branch uses. Before
// the fix, none of the three decorators observed a Close()-only failure
// at all: the value was discarded via a bare `defer rows.Close()`, so
// this failure mode was completely invisible (no counter, no warn log).
func TestCloseOnlyFailure_IsReportedAcrossDecorators(t *testing.T) {
	t.Run("RecordInvestmentRepoJoinDedupCollisions", func(t *testing.T) {
		resetRepoJoinDedupCollisionCooldown(t)

		var capturedOrgID string
		var capturedErr error
		orig := recordRepoJoinDedupCollisionFetchFailure
		recordRepoJoinDedupCollisionFetchFailure = func(_ context.Context, orgID string, err error) {
			capturedOrgID = orgID
			capturedErr = err
		}
		t.Cleanup(func() { recordRepoJoinDedupCollisionFetchFailure = orig })

		client := (&routingFakeClient{}).on("excess_repo_versions", &fakeRowScanner{
			rows:     [][]any{{int64(0)}},
			closeErr: errInjectedCloseFailure,
		})
		RecordInvestmentRepoJoinDedupCollisions(context.Background(), client, "org-close-fail")

		if capturedOrgID != "org-close-fail" {
			t.Errorf("captured org_id = %q, want %q -- a Close()-only failure must be reported", capturedOrgID, "org-close-fail")
		}
		if !errors.Is(capturedErr, errInjectedCloseFailure) {
			t.Errorf("captured err = %v, want it to wrap %v", capturedErr, errInjectedCloseFailure)
		}
	})

	t.Run("RecordArgMaxNullTransitionGuard", func(t *testing.T) {
		resetArgMaxNullTransitionGate(t)

		var capturedOrgID string
		var capturedErr error
		orig := recordArgMaxNullTransitionFetchFailure
		recordArgMaxNullTransitionFetchFailure = func(_ context.Context, orgID string, err error) {
			capturedOrgID = orgID
			capturedErr = err
		}
		t.Cleanup(func() { recordArgMaxNullTransitionFetchFailure = orig })

		client := &routingFakeClient{}
		client.on("HAVING count() > 1", &fakeRowScanner{
			rows:     [][]any{{int64(0), int64(0), int64(0), int64(0), int64(203)}},
			closeErr: errInjectedCloseFailure,
		})
		RecordArgMaxNullTransitionGuard(context.Background(), client, "org-close-fail", 30)

		if capturedOrgID != "org-close-fail" {
			t.Errorf("captured org_id = %q, want %q -- a Close()-only failure must be reported", capturedOrgID, "org-close-fail")
		}
		if !errors.Is(capturedErr, errInjectedCloseFailure) {
			t.Errorf("captured err = %v, want it to wrap %v", capturedErr, errInjectedCloseFailure)
		}
	})

	t.Run("FetchArgMaxNullTransitionState_direct", func(t *testing.T) {
		client := &routingFakeClient{}
		client.on("HAVING count() > 1", &fakeRowScanner{
			rows:     [][]any{{int64(1), int64(2), int64(3), int64(4), int64(203)}},
			closeErr: errInjectedCloseFailure,
		})

		got, err := FetchArgMaxNullTransitionState(context.Background(), client, "org-1", 30)
		if !errors.Is(err, errInjectedCloseFailure) {
			t.Fatalf("FetchArgMaxNullTransitionState error = %v, want it to wrap %v", err, errInjectedCloseFailure)
		}
		if got != (ArgMaxNullTransitionState{}) {
			t.Errorf("state = %+v on a Close() failure, want the zero value", got)
		}
	})

	t.Run("FetchInvestmentMembershipScopeState", func(t *testing.T) {
		client := &routingFakeClient{}
		client.on("SELECT scope_mode, lag_seconds", &fakeRowScanner{
			rows:     [][]any{{"unscoped_fallback", int64(4321)}},
			closeErr: errInjectedCloseFailure,
		})

		got, err := FetchInvestmentMembershipScopeState(context.Background(), client, "org-1", 30)
		if !errors.Is(err, errInjectedCloseFailure) {
			t.Fatalf("FetchInvestmentMembershipScopeState error = %v, want it to wrap %v", err, errInjectedCloseFailure)
		}
		want := InvestmentMembershipScopeState{ScopeMode: "unscoped_no_marker"}
		if got != want {
			t.Errorf("state = %+v on a Close() failure, want the safe fallback %+v", got, want)
		}
	})
}

// TestCloseOnlyFailure_ShortensCooldown is table-driven across the two
// decorators with a claim-then-maybe-shorten cooldown
// (RecordInvestmentRepoJoinDedupCollisions, RecordArgMaxNullTransitionGuard):
// a Close()-only failure must shorten the just-claimed FULL cooldown to
// the SHORT error cooldown, the same as every other failure branch,
// rather than leaving a legitimately-broken check silent for the full
// window. investmentmembershiptelemetry.go's decorator has no cooldown
// mechanism to shorten (see the sibling test above), so it is not part
// of this table.
func TestCloseOnlyFailure_ShortensCooldown(t *testing.T) {
	t.Run("RecordInvestmentRepoJoinDedupCollisions", func(t *testing.T) {
		resetRepoJoinDedupCollisionCooldown(t)
		now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
		repoJoinDedupCollisionNow = func() time.Time { return now }

		failingClient := (&routingFakeClient{}).on("excess_repo_versions", &fakeRowScanner{
			rows:     [][]any{{int64(0)}},
			closeErr: errInjectedCloseFailure,
		})
		RecordInvestmentRepoJoinDedupCollisions(context.Background(), failingClient, "org-1")

		// Barely past the SHORT error cooldown, still well inside the
		// full success cooldown -- must be allowed to retry.
		repoJoinDedupCollisionNow = func() time.Time { return now.Add(repoJoinDedupCollisionErrorCooldown + time.Second) }

		client := (&routingFakeClient{}).on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(0)}}})
		RecordInvestmentRepoJoinDedupCollisions(context.Background(), client, "org-1")
		if got := len(client.calls); got != 1 {
			t.Fatalf("retry after error cooldown: expected exactly one query, got %d -- a Close()-only failure must not consume the full cooldown", got)
		}
	})

	t.Run("RecordArgMaxNullTransitionGuard", func(t *testing.T) {
		resetArgMaxNullTransitionGate(t)
		now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
		argMaxNullTransitionGateClock = func() time.Time { return now }

		failingClient := &routingFakeClient{}
		failingClient.on("HAVING count() > 1", &fakeRowScanner{
			rows:     [][]any{{int64(0), int64(0), int64(0), int64(0), int64(10)}},
			closeErr: errInjectedCloseFailure,
		})
		RecordArgMaxNullTransitionGuard(context.Background(), failingClient, "org-1", 30)

		// Barely past the SHORT error cooldown, still well inside the
		// full success cooldown -- must be allowed to retry.
		now = now.Add(argMaxNullTransitionErrorCooldown + time.Second)

		client := &routingFakeClient{}
		client.on("HAVING count() > 1", &fakeRowScanner{rows: [][]any{
			{int64(0), int64(0), int64(0), int64(0), int64(10)},
		}})
		RecordArgMaxNullTransitionGuard(context.Background(), client, "org-1", 30)
		if got := len(client.calls); got != 1 {
			t.Fatalf("retry after error cooldown: expected exactly one query, got %d -- a Close()-only failure must not consume the full cooldown", got)
		}
	})
}
