//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPostgresFailAndCompleteSurfaceDatasetUnavailabilityOnIntegrationDataset
// pins CHAOS-4048: a repeated provider_dataset_unavailable terminalization
// (the pagerduty/teams shape -- an account-level capability loss, not a
// transient error) must land on the IntegrationDataset row the owner-facing
// API reads, not only on the per-unit failure rows nobody watches. It must
// also self-heal: the next successful unit for the same dataset clears it,
// with no operator action required.
func TestPostgresFailAndCompleteSurfaceDatasetUnavailabilityOnIntegrationDataset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)

	claim := func(unitID string, at time.Time) Claim {
		t.Helper()
		claim, err := repository.Claim(ctx, ClaimRequest{
			UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: at,
			LeaseDuration: time.Minute, AllowExpiredRecovery: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return claim
	}

	// seedProviderSyncFixture only plants firstUnitID; the second and third
	// units in this test need their own rows for the same integration/source/
	// dataset, matching the shape Claim expects.
	insertAndClaimUnit := func(unitID string, at time.Time) Claim {
		t.Helper()
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
	id, org_id, sync_run_id, integration_id, source_id, provider,
	dataset_key, cost_class, mode, since_at, before_at, status,
	processor_flags, updated_at
) VALUES (
	$1, 'org-acme', $2, $3, $4, 'github', 'commits', 'medium',
	'incremental', '2026-07-22T12:00:00Z', '2026-12-01T00:00:00Z',
	'dispatching', '{"sync_git":true,"sync_commits":true}', NOW()
)`, unitID, firstRunID, firstIntegrationID, firstSourceID); err != nil {
			t.Fatal(err)
		}
		return claim(unitID, at)
	}

	marker := func() (reason *string, since, lastSeen *time.Time) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
SELECT unavailable_reason, unavailable_since, unavailable_last_seen_at
FROM public.integration_datasets
WHERE org_id = 'org-acme' AND integration_id = $1 AND dataset_key = 'commits'`,
			firstIntegrationID).Scan(&reason, &since, &lastSeen); err != nil {
			t.Fatal(err)
		}
		return reason, since, lastSeen
	}

	// The pre-existing seeded unit (firstUnitID) fails first: the account-level
	// capability is gone, so this is the first-attempt terminal classification
	// pagerduty_teams_route.go / providerunit.go produce in production.
	first := claim(firstUnitID, now)
	firstFailedAt := now.Add(time.Second)
	if err := repository.Fail(
		ctx, first, ProviderDatasetUnavailableCategory, now, firstFailedAt,
	); err != nil {
		t.Fatal(err)
	}
	reason, since, lastSeen := marker()
	if reason == nil || *reason != ProviderDatasetUnavailableCategory {
		t.Fatalf("unavailable_reason=%v, want %q", reason, ProviderDatasetUnavailableCategory)
	}
	if since == nil || !since.Equal(firstFailedAt) {
		t.Fatalf("unavailable_since=%v, want %s", since, firstFailedAt)
	}
	if lastSeen == nil || !lastSeen.Equal(firstFailedAt) {
		t.Fatalf("unavailable_last_seen_at=%v, want %s", lastSeen, firstFailedAt)
	}

	// A second unit for the SAME dataset fails again, later. unavailable_since
	// must NOT move -- it marks when the outage started, not the latest
	// failure -- but unavailable_last_seen_at must advance.
	secondUnitID := uuid.NewString()
	second := insertAndClaimUnit(secondUnitID, now.Add(2*time.Hour))
	secondFailedAt := now.Add(2*time.Hour + time.Second)
	if err := repository.Fail(
		ctx, second, ProviderDatasetUnavailableCategory, now.Add(2*time.Hour), secondFailedAt,
	); err != nil {
		t.Fatal(err)
	}
	reason, since, lastSeen = marker()
	if reason == nil || *reason != ProviderDatasetUnavailableCategory {
		t.Fatalf("unavailable_reason=%v after second failure, want %q", reason, ProviderDatasetUnavailableCategory)
	}
	if since == nil || !since.Equal(firstFailedAt) {
		t.Fatalf("unavailable_since=%v moved off the first outage instant %s", since, firstFailedAt)
	}
	if lastSeen == nil || !lastSeen.Equal(secondFailedAt) {
		t.Fatalf("unavailable_last_seen_at=%v, want it advanced to %s", lastSeen, secondFailedAt)
	}

	// A straggler unit commits its failure LAST but carries an EARLIER
	// completedAt than either prior write -- e.g. a slow attempt that started
	// before the others and only just lost its race to the database. LEAST/
	// GREATEST must still resolve to the true earliest/latest instants rather
	// than "whichever write landed last": unavailable_since moves earlier,
	// but unavailable_last_seen_at must NOT regress behind secondFailedAt.
	stragglerUnitID := uuid.NewString()
	straggler := insertAndClaimUnit(stragglerUnitID, now.Add(-time.Hour))
	stragglerFailedAt := now.Add(-time.Hour + time.Second)
	if err := repository.Fail(
		ctx, straggler, ProviderDatasetUnavailableCategory, now.Add(-time.Hour), stragglerFailedAt,
	); err != nil {
		t.Fatal(err)
	}
	reason, since, lastSeen = marker()
	if reason == nil || *reason != ProviderDatasetUnavailableCategory {
		t.Fatalf("unavailable_reason=%v after straggler failure, want %q", reason, ProviderDatasetUnavailableCategory)
	}
	if since == nil || !since.Equal(stragglerFailedAt) {
		t.Fatalf("unavailable_since=%v, want it to move to the earlier straggler instant %s", since, stragglerFailedAt)
	}
	if lastSeen == nil || !lastSeen.Equal(secondFailedAt) {
		t.Fatalf("unavailable_last_seen_at=%v regressed behind %s on a late-committing but chronologically earlier failure", lastSeen, secondFailedAt)
	}

	// A third unit for the same dataset SUCCEEDS -- the provider capability
	// came back. Self-heal: Complete must clear the marker unconditionally,
	// with no operator action.
	thirdUnitID := uuid.NewString()
	third := insertAndClaimUnit(thirdUnitID, now.Add(4*time.Hour))
	watermark := now.Add(4*time.Hour + time.Second)
	if err := repository.Complete(
		ctx, third, map[string]any{"records": 1}, &watermark,
		now.Add(4*time.Hour), now.Add(4*time.Hour+2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	reason, since, lastSeen = marker()
	if reason != nil || since != nil || lastSeen != nil {
		t.Fatalf("marker not cleared by success: reason=%v since=%v last_seen=%v", reason, since, lastSeen)
	}
}
