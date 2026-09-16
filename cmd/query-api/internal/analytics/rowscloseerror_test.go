package analytics

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// capturedLogRecord is one slog record captured by captureSlog, reduced to
// the fields these tests assert on.
type capturedLogRecord struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

// captureSlogHandler is a minimal slog.Handler that appends every record
// it receives to a shared, mutex-guarded slice -- this package has no
// existing slog capture helper, so this is the "handler in the test"
// fallback.
type captureSlogHandler struct {
	mu      *sync.Mutex
	records *[]capturedLogRecord
}

func (h *captureSlogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureSlogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]any{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, capturedLogRecord{level: r.Level, msg: r.Message, attrs: attrs})
	return nil
}

func (h *captureSlogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureSlogHandler) WithGroup(string) slog.Handler      { return h }

// captureSlog installs a capturing handler as the process-wide slog
// default for the duration of the test and restores the original on
// cleanup. This package's tests never run with -parallel (no
// t.Parallel() call in the package), so swapping the process-wide
// default for one test body is safe.
func captureSlog(t *testing.T) *[]capturedLogRecord {
	t.Helper()
	var mu sync.Mutex
	var records []capturedLogRecord
	orig := slog.Default()
	slog.SetDefault(slog.New(&captureSlogHandler{mu: &mu, records: &records}))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &records
}

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

	// RecordStaleInvestmentMembershipScope has no cooldown to shorten, so
	// its only observable for a fetch failure is the warn log it reports
	// through -- this decorator's own report path, per its doc comment.
	t.Run("RecordStaleInvestmentMembershipScope_WarnsOnCloseOnlyFailure", func(t *testing.T) {
		records := captureSlog(t)

		client := &routingFakeClient{}
		client.on("SELECT scope_mode, lag_seconds", &fakeRowScanner{
			rows:     [][]any{{"unscoped_fallback", int64(4321)}},
			closeErr: errInjectedCloseFailure,
		})

		RecordStaleInvestmentMembershipScope(context.Background(), client, "org-close-fail", 30)

		var warn *capturedLogRecord
		for i := range *records {
			if (*records)[i].msg == "investment membership scope metric skipped" {
				warn = &(*records)[i]
				break
			}
		}
		if warn == nil {
			t.Fatalf("no %q record captured; records = %+v", "investment membership scope metric skipped", *records)
		}
		if warn.level != slog.LevelWarn {
			t.Errorf("level = %v, want %v -- a swallowed fetch error must be operator-visible at this platform's default log level", warn.level, slog.LevelWarn)
		}
		if got, _ := warn.attrs["org_id"].(string); got != "org-close-fail" {
			t.Errorf("org_id attr = %v, want %q", warn.attrs["org_id"], "org-close-fail")
		}
		gotErr, _ := warn.attrs["error"].(error)
		if gotErr == nil || !errors.Is(gotErr, errInjectedCloseFailure) {
			t.Errorf("error attr = %v, want it to wrap %v", warn.attrs["error"], errInjectedCloseFailure)
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
