//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// recordingRemainingLeaseObserver counts exactly what the production counter
// counts, so the test predicts the metric rather than merely proving
// ReleasePartition returned the expected error.
type recordingRemainingLeaseObserver struct{ releaseLost int }

func (observer *recordingRemainingLeaseObserver) ObserveRemainingMetricsLeaseReleaseLost() error {
	observer.releaseLost++
	return nil
}

// TestReleasePartitionRecordsReleaseLostOnlyOnAnExpiredLease is the
// CHAOS-4002 mutation-catching test: dropping the observeReleaseLost() call
// in ReleasePartition must fail this. Both directions are seeded (input
// symmetry): a live-lease release that succeeds must NOT record anything, and
// an expired-lease release that fails with ErrLeaseLost MUST record exactly
// once. A second pass through the SAME scenario using a real
// jobruntime.MetricsCollector (TestReleasePartitionRecordsReleaseLostInRealPrometheusExposition,
// below) additionally proves the PrometheusText wiring itself, which a
// bespoke recorder like the one used here cannot: it would keep passing even
// if writeRemainingMetricsLease were dropped from PrometheusText entirely.
//
// Unlike work-graph's Ambiguous (a terminal transition), ReleasePartition
// sets status='failed', which ClaimPartition's own WHERE clause still
// accepts -- so the SAME partition is reclaimed for the second half of the
// test, proving the release genuinely returned it to the pool rather than
// merely returning a distinguishable error.
//
// handler.go has three releaseClaim discard sites (LoadRun error, validation
// mismatch, lease loss during work); all three fire either synchronously
// right after ClaimPartition or after runWithLeaseRenewal's own failure,
// with no time gap in which THIS claimant's lease genuinely expires before
// its own release attempt, so the only way any of them meets an
// already-expired lease in production is a real concurrent reclaim racing in
// between, not something a mocked clock can reproduce honestly. That all
// three sites reach ReleasePartition at all is pinned, unchanged by this
// ticket except for adding the one case that had no coverage before it
// (LoadRun error): TestPartitionHandlerRejectsCrossFamilyExecution
// (validation mismatch), TestPartitionHandlerLeaseLossCancelsCompatibility
// (lease loss), and TestPartitionHandlerReleasesClaimOnLoadRunFailure
// (LoadRun error, added by this ticket) in handler_test.go. What this test
// adds is what ReleasePartition itself does once reached: recording is
// centralized in the store, not duplicated per call site, so proving it here
// covers every caller by construction.
func TestReleasePartitionRecordsReleaseLostOnlyOnAnExpiredLease(t *testing.T) {
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
	createRemainingTables(t, ctx, pool)

	observer := &recordingRemainingLeaseObserver{}
	store, err := NewPostgresStore(pool, observer)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000110",
		Family:         "capacity",
		Generation:     "release-lost-fixture",
		ScopeKey:       "all-teams",
		GenerationSeed: int64Pointer(7),
		Scopes:         []json.RawMessage{json.RawMessage(`{"version":1,"all_teams":true,"history_days":90,"simulations":10000}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)

	// Still within the lease: ReleasePartition succeeds and must not record
	// release_lost -- a claimant that still holds its lease standing its own
	// row down deliberately is not a stall signal.
	liveClaim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || liveClaim == nil {
		t.Fatalf("live claim = %#v, %v", liveClaim, err)
	}
	if err := store.ReleasePartition(ctx, *liveClaim); err != nil {
		t.Fatalf("live-lease ReleasePartition = %v", err)
	}
	if observer.releaseLost != 0 {
		t.Fatalf("live-lease release-lost count = %d, want 0", observer.releaseLost)
	}

	// Reclaim the same partition (status='failed' is still claimable), then
	// let its lease expire before attempting to release it -- the exact
	// CHAOS-3991 shape: the claimant has outlived its own lease and can no
	// longer stand its row down.
	expiredClaim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || expiredClaim == nil {
		t.Fatalf("expired-lease claim = %#v, %v", expiredClaim, err)
	}
	var preReleaseToken string
	var preReleaseLeaseExpiry time.Time
	if err := pool.QueryRow(ctx, `SELECT claim_token::text, lease_expires_at FROM remaining_metric_partitions WHERE id = $1::uuid`, partitionID).Scan(&preReleaseToken, &preReleaseLeaseExpiry); err != nil {
		t.Fatal(err)
	}

	now = now.Add(store.lease + time.Second)
	err = store.ReleasePartition(ctx, *expiredClaim)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired-lease ReleasePartition = %v, want ErrLeaseLost", err)
	}
	if observer.releaseLost != 1 {
		t.Fatalf("expired-lease release-lost count = %d, want 1", observer.releaseLost)
	}

	// The fence itself must be unchanged by the lost release: the row stays
	// exactly as the expired claimant left it -- status, claim_token, AND
	// lease_expires_at -- for a live claimant to reclaim. A regression that
	// clears the token or lease while leaving status alone would only be
	// caught by checking all three.
	var status, postReleaseToken string
	var postReleaseLeaseExpiry time.Time
	if err := pool.QueryRow(ctx, `SELECT status, claim_token::text, lease_expires_at FROM remaining_metric_partitions WHERE id = $1::uuid`, partitionID).Scan(&status, &postReleaseToken, &postReleaseLeaseExpiry); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("expired-lease partition status = %q, want unchanged 'running'", status)
	}
	if postReleaseToken != preReleaseToken {
		t.Fatalf("expired-lease claim_token = %q, want unchanged %q", postReleaseToken, preReleaseToken)
	}
	if !postReleaseLeaseExpiry.Equal(preReleaseLeaseExpiry) {
		t.Fatalf("expired-lease lease_expires_at = %v, want unchanged %v", postReleaseLeaseExpiry, preReleaseLeaseExpiry)
	}
}

// TestReleasePartitionRecordsReleaseLostInRealPrometheusExposition proves the
// full production wiring end to end: a real jobruntime.MetricsCollector, not
// a bespoke recorder, so this fails if writeRemainingMetricsLease is ever
// dropped from PrometheusText -- a gap the bespoke-recorder test above cannot
// see (adversarial codex review, CHAOS-4002).
func TestReleasePartitionRecordsReleaseLostInRealPrometheusExposition(t *testing.T) {
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
	createRemainingTables(t, ctx, pool)

	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(collector.PrometheusText(), "worker_remaining_metrics_lease_release_lost_total 0\n") {
		t.Fatal("collector did not pre-seed worker_remaining_metrics_lease_release_lost_total at 0")
	}

	store, err := NewPostgresStore(pool, collector)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	run, err := store.StartRun(ctx, StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000111",
		Family:         "capacity",
		Generation:     "release-lost-prometheus-fixture",
		ScopeKey:       "all-teams",
		GenerationSeed: int64Pointer(11),
		Scopes:         []json.RawMessage{json.RawMessage(`{"version":1,"all_teams":true,"history_days":90,"simulations":10000}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	partitionID := deterministicPartitionID(run.ID, 1)
	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	now = now.Add(store.lease + time.Second)
	if err := store.ReleasePartition(ctx, *claim); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ReleasePartition = %v, want ErrLeaseLost", err)
	}
	if !strings.Contains(collector.PrometheusText(), "worker_remaining_metrics_lease_release_lost_total 1\n") {
		t.Fatalf("PrometheusText did not reflect the release-lost observation:\n%s", collector.PrometheusText())
	}
}
