//go:build integration

package workgraph

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// recordingWorkGraphLeaseObserver counts exactly what the production counter
// counts, so the test predicts the metric rather than merely proving Ambiguous
// returned the expected error.
type recordingWorkGraphLeaseObserver struct{ releaseLost int }

func (observer *recordingWorkGraphLeaseObserver) ObserveWorkGraphLeaseReleaseLost() error {
	observer.releaseLost++
	return nil
}

func startWorkGraphReleaseLostFixture(
	t *testing.T, ctx context.Context,
) (*pgxpool.Pool, *PostgresStore, *recordingWorkGraphLeaseObserver) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createExecutionTables(t, ctx, pool)
	observer := &recordingWorkGraphLeaseObserver{}
	store, err := NewPostgresStore(pool, observer)
	if err != nil {
		t.Fatal(err)
	}
	return pool, store, observer
}

func insertReleaseLostRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, idempotencyKey string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO work_graph_execution_requests (
    id, org_id, kind, scope, llm_concurrency, spend_limit_microunits,
    correlation_id, idempotency_key, state
) VALUES ($1,$2,'workgraph.build','{}',1,0,'release-lost-fixture',$3,'pending')`,
		id, testOrgID, idempotencyKey,
	); err != nil {
		t.Fatal(err)
	}
}

// TestWorkGraphAmbiguousRecordsReleaseLostOnlyOnAnExpiredLease is the
// CHAOS-4002 mutation-catching test: dropping the observeReleaseLost() call
// in Ambiguous must fail this. Both directions are seeded (input symmetry): a
// live-lease release that succeeds must NOT record anything, and an
// expired-lease release that fails with ErrLeaseLost MUST record exactly
// once. A second pass through the same scenario using a real
// jobruntime.MetricsCollector (TestWorkGraphAmbiguousRecordsReleaseLostInRealPrometheusExposition,
// below) additionally proves the PrometheusText wiring itself, which a
// bespoke recorder like the one used here cannot: it would keep passing even
// if writeWorkGraphLease were dropped from PrometheusText entirely.
//
// It also exercises the exact detail strings handler.go's two discard sites
// pass (releaseAmbiguous on org mismatch, and on an unknown compatibility
// outcome) as its two claims, one per site. It does not drive them through
// the handler itself: both fire synchronously right after Claim, with no
// time gap in which a lease genuinely expires, so the only way either one
// meets an expired lease in production is a real concurrent reclaim racing
// in between -- not something a mocked clock can reproduce honestly. That
// both sites reach Ambiguous/store.ambiguous at all is already pinned,
// unchanged by this ticket, by TestBuildRejectsTenantEnvelopeMismatchBeforeClaim
// and TestCompatibilityFailureIsAmbiguousNotRetried in handler_test.go. What
// this test adds is what Ambiguous itself does once reached: recording is
// centralized in the store, not duplicated per call site, so proving it here
// covers every caller by construction.
func TestWorkGraphAmbiguousRecordsReleaseLostOnlyOnAnExpiredLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, store, observer := startWorkGraphReleaseLostFixture(t, ctx)

	const liveRequestID = "00000000-0000-4000-8000-000000000201"
	const expiredRequestID = "00000000-0000-4000-8000-000000000202"
	insertReleaseLostRequest(t, ctx, pool, liveRequestID, "workgraph:release-lost-live")
	insertReleaseLostRequest(t, ctx, pool, expiredRequestID, "workgraph:release-lost-expired")

	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	// Still within the lease: Ambiguous succeeds and must not record
	// release_lost -- a claimant that still holds its lease standing its own
	// row down deliberately is not a stall signal. Detail text matches
	// handler.go's org-mismatch discard site.
	liveClaim, err := store.Claim(ctx, liveRequestID, KindBuild)
	if err != nil || liveClaim == nil {
		t.Fatalf("live claim = %#v, %v", liveClaim, err)
	}
	if err := store.Ambiguous(ctx, *liveClaim, "claimed request no longer matches River envelope"); err != nil {
		t.Fatalf("live-lease Ambiguous = %v", err)
	}
	if observer.releaseLost != 0 {
		t.Fatalf("live-lease release-lost count = %d, want 0", observer.releaseLost)
	}

	// Claim a second request, then let its lease expire before attempting to
	// release it -- the exact CHAOS-3991 shape: the claimant has outlived its
	// own lease and can no longer stand its row down. Detail text matches
	// handler.go's compatibility-execution-unknown discard site.
	expiredClaim, err := store.Claim(ctx, expiredRequestID, KindBuild)
	if err != nil || expiredClaim == nil {
		t.Fatalf("expired-lease claim = %#v, %v", expiredClaim, err)
	}
	var preReleaseToken string
	var preReleaseLeaseExpiry time.Time
	if err := pool.QueryRow(ctx, `SELECT claim_token::text, lease_expires_at FROM work_graph_execution_requests WHERE id = $1`, expiredRequestID).Scan(&preReleaseToken, &preReleaseLeaseExpiry); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.lease + time.Second)
	err = store.Ambiguous(ctx, *expiredClaim, "compatibility execution outcome is unknown")
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired-lease Ambiguous = %v, want ErrLeaseLost", err)
	}
	if observer.releaseLost != 1 {
		t.Fatalf("expired-lease release-lost count = %d, want 1", observer.releaseLost)
	}

	// The fence itself must be unchanged by the lost release: the row stays
	// exactly as the expired claimant left it -- state, claim_token, AND
	// lease_expires_at -- for a live claimant to reclaim. A regression that
	// clears the token or lease while leaving state alone would only be
	// caught by checking all three.
	var state, postReleaseToken string
	var postReleaseLeaseExpiry time.Time
	if err := pool.QueryRow(ctx, `SELECT state, claim_token::text, lease_expires_at FROM work_graph_execution_requests WHERE id = $1`, expiredRequestID).Scan(&state, &postReleaseToken, &postReleaseLeaseExpiry); err != nil {
		t.Fatal(err)
	}
	if state != "running" {
		t.Fatalf("expired-lease request state = %q, want unchanged 'running'", state)
	}
	if postReleaseToken != preReleaseToken {
		t.Fatalf("expired-lease claim_token = %q, want unchanged %q", postReleaseToken, preReleaseToken)
	}
	if !postReleaseLeaseExpiry.Equal(preReleaseLeaseExpiry) {
		t.Fatalf("expired-lease lease_expires_at = %v, want unchanged %v", postReleaseLeaseExpiry, preReleaseLeaseExpiry)
	}
}

// TestWorkGraphAmbiguousRecordsReleaseLostInRealPrometheusExposition proves
// the full production wiring end to end: a real jobruntime.MetricsCollector,
// not a bespoke recorder, so this fails if writeWorkGraphLease is ever
// dropped from PrometheusText -- a gap the bespoke-recorder test above
// cannot see (adversarial codex review, CHAOS-4002).
func TestWorkGraphAmbiguousRecordsReleaseLostInRealPrometheusExposition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createExecutionTables(t, ctx, pool)

	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(collector.PrometheusText(), "worker_workgraph_lease_release_lost_total 0\n") {
		t.Fatal("collector did not pre-seed worker_workgraph_lease_release_lost_total at 0")
	}

	store, err := NewPostgresStore(pool, collector)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	const requestID = "00000000-0000-4000-8000-000000000205"
	insertReleaseLostRequest(t, ctx, pool, requestID, "workgraph:release-lost-prometheus")
	claim, err := store.Claim(ctx, requestID, KindBuild)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	now = now.Add(store.lease + time.Second)
	if err := store.Ambiguous(ctx, *claim, "compatibility execution outcome is unknown"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Ambiguous = %v, want ErrLeaseLost", err)
	}
	if !strings.Contains(collector.PrometheusText(), "worker_workgraph_lease_release_lost_total 1\n") {
		t.Fatalf("PrometheusText did not reflect the release-lost observation:\n%s", collector.PrometheusText())
	}
}
