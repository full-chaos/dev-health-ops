//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresPreparedRouteSnapshotLifecycleAndFences(t *testing.T) {
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

	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	unitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, unitID, "incremental", `{
		"sync_prs":true,"family_dataset_work_items":true
	}`)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch := preparedGitHubWorkItemsFixture(t, claim)
	state, err := repository.PrepareRouteSnapshot(
		ctx, claim, batch, ShadowComparison{Match: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if state.SchemaVersion != "v2" || state.PreparedSnapshot == nil {
		t.Fatalf("prepared state=%+v", state)
	}
	manifest, err := repository.LoadRouteSnapshot(ctx, claim, state, now.Add(time.Second))
	if err != nil || len(manifest.Batch.Effects) != len(workItemRouteDestinations()) {
		t.Fatalf("loaded manifest=%+v error=%v", manifest, err)
	}

	wrongTenant := claim
	wrongTenant.OrgID = "org-other"
	if _, err := repository.LoadRouteSnapshot(ctx, wrongTenant, state, now.Add(time.Second)); !errors.Is(err, ErrPreparedRouteSnapshotNotFound) {
		t.Fatalf("wrong tenant error=%v", err)
	}
	wrongGeneration := claim
	wrongGeneration.ID = uuid.NewString()
	if _, err := repository.LoadRouteSnapshot(ctx, wrongGeneration, state, now.Add(time.Second)); !errors.Is(err, ErrPreparedRouteSnapshotNotFound) {
		t.Fatalf("wrong generation error=%v", err)
	}

	var originalPayload []byte
	if err := pool.QueryRow(ctx, `
SELECT payload FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	).Scan(&originalPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_unit_effect_snapshots
SET payload = payload || decode('00', 'hex'), payload_bytes = payload_bytes + 1
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.LoadRouteSnapshot(ctx, claim, state, now.Add(time.Second)); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("tamper error=%v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_unit_effect_snapshots
SET payload = $4, payload_bytes = $5
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		claim.OrgID, claim.ID, claim.GenerationKey(), originalPayload, len(originalPayload),
	); err != nil {
		t.Fatal(err)
	}

	if err := repository.ReleaseForRetry(ctx, claim, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	assertPreparedSnapshotCount(t, ctx, pool, claim, 1)
	// The snapshot is retained across the release, but it is not readable
	// without a live lease. The unit is `dispatching` here, so the status
	// fence alone must refuse the load even though the row is still present
	// and the tenant/unit/generation key still matches exactly.
	if _, err := repository.LoadRouteSnapshot(
		ctx, claim, state, now.Add(2*time.Second),
	); !errors.Is(err, ErrPreparedRouteSnapshotNotFound) {
		t.Fatalf("released-lease load error=%v", err)
	}
	reclaimed, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now.Add(3 * time.Second),
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Positive control first: the generation survives the reclaim, so the new
	// owner inside its lease window CAN load the same snapshot. Without this
	// the two refusals below would also pass if the row had simply become
	// unreadable for some unrelated reason.
	if _, err := repository.LoadRouteSnapshot(
		ctx, reclaimed, state, now.Add(4*time.Second),
	); err != nil {
		t.Fatalf("reclaimed owner load error=%v", err)
	}
	// A worker holding the superseded lease must not be able to read the
	// snapshot the new owner is now responsible for.
	if _, err := repository.LoadRouteSnapshot(
		ctx, claim, state, now.Add(4*time.Second),
	); !errors.Is(err, ErrPreparedRouteSnapshotNotFound) {
		t.Fatalf("stale owner load error=%v", err)
	}
	// Now isolate the three lease fences one at a time. ReleaseForRetry clears
	// owner, expiry and status together, so the release above can never say
	// WHICH fence refused -- and a single missing fence would hide behind the
	// other two. Each iteration breaks exactly one column, proves the refusal,
	// restores it, and re-proves the load succeeds again, so no refusal can be
	// left over from the previous iteration.
	assertLeaseFencesIsolateTheSnapshotLoad(t, ctx, pool, repository, reclaimed, state, now)
	if err := repository.Complete(
		ctx, reclaimed, map[string]any{"records": 16}, nil,
		now.Add(3*time.Second), now.Add(4*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	assertPreparedSnapshotCount(t, ctx, pool, reclaimed, 0)

	atomicUnitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, atomicUnitID, "incremental", `{
		"sync_prs":true,"family_dataset_work_items":true
	}`)
	atomicClaim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: atomicUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now.Add(5 * time.Second),
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
CREATE FUNCTION reject_snapshot_ledger_update() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'simulated ledger update failure';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER reject_snapshot_ledger_update
BEFORE UPDATE ON public.sync_run_units
FOR EACH ROW EXECUTE FUNCTION reject_snapshot_ledger_update()`); err != nil {
		t.Fatal(err)
	}
	atomicBatch := preparedGitHubWorkItemsFixture(t, atomicClaim)
	if _, err := repository.PrepareRouteSnapshot(
		ctx, atomicClaim, atomicBatch, ShadowComparison{Match: true}, now.Add(5*time.Second),
	); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("atomic prepare error=%v", err)
	}
	assertPreparedSnapshotCount(t, ctx, pool, atomicClaim, 0)
	if _, err := pool.Exec(ctx, `
DROP TRIGGER reject_snapshot_ledger_update ON public.sync_run_units;
DROP FUNCTION reject_snapshot_ledger_update()`); err != nil {
		t.Fatal(err)
	}

	rollbackUnitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, rollbackUnitID, "incremental", `{
		"sync_prs":true,"family_dataset_work_items":true
	}`)
	rollbackClaim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: rollbackUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now.Add(6 * time.Second),
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rollbackBatch := preparedGitHubWorkItemsFixture(t, rollbackClaim)
	if _, err := repository.PrepareRouteSnapshot(
		ctx, rollbackClaim, rollbackBatch, ShadowComparison{Match: true}, now.Add(6*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DROP TABLE public.sync_dispatch_outbox"); err != nil {
		t.Fatal(err)
	}
	if err := repository.Complete(
		ctx, rollbackClaim, map[string]any{"records": 16}, nil,
		now.Add(6*time.Second), now.Add(7*time.Second),
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("rollback completion error=%v", err)
	}
	assertPreparedSnapshotCount(t, ctx, pool, rollbackClaim, 1)
	var status string
	if err := pool.QueryRow(
		ctx, "SELECT status FROM public.sync_run_units WHERE id = $1", rollbackClaim.ID,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("rolled-back unit status=%q", status)
	}
}

func assertPreparedSnapshotCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	claim Claim,
	want int,
) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("snapshot count=%d want=%d", got, want)
	}
}

// assertLeaseFencesIsolateTheSnapshotLoad breaks one lease column at a time so
// each fence in loadPreparedRouteSnapshotSQL is measured on its own. A test
// that only releases the lease exercises all three at once, which is exactly
// the shape that lets one of them be silently absent.
func assertLeaseFencesIsolateTheSnapshotLoad(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	repository *PostgresRepository,
	claim Claim,
	state EffectLedgerState,
	now time.Time,
) {
	t.Helper()
	var (
		liveOwner   string
		liveExpires time.Time
		liveStatus  string
	)
	if err := pool.QueryRow(ctx, `
SELECT lease_owner, lease_expires_at, status
FROM public.sync_run_units WHERE id = $1`, claim.ID,
	).Scan(&liveOwner, &liveExpires, &liveStatus); err != nil {
		t.Fatal(err)
	}
	at := now.Add(4 * time.Second)
	for _, fence := range []struct {
		name      string
		statement string
		argument  any
	}{
		{
			"lease owner",
			"UPDATE public.sync_run_units SET lease_owner = $2 WHERE id = $1",
			uuid.NewString(),
		},
		{
			"lease expiry",
			"UPDATE public.sync_run_units SET lease_expires_at = $2 WHERE id = $1",
			at.Add(-time.Second),
		},
		{
			"unit status",
			"UPDATE public.sync_run_units SET status = $2 WHERE id = $1",
			"dispatching",
		},
	} {
		if _, err := pool.Exec(ctx, fence.statement, claim.ID, fence.argument); err != nil {
			t.Fatal(err)
		}
		if _, err := repository.LoadRouteSnapshot(ctx, claim, state, at); !errors.Is(
			err, ErrPreparedRouteSnapshotNotFound,
		) {
			t.Fatalf("%s fence did not refuse the load: error=%v", fence.name, err)
		}
		if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET lease_owner = $2, lease_expires_at = $3, status = $4
WHERE id = $1`, claim.ID, liveOwner, liveExpires, liveStatus); err != nil {
			t.Fatal(err)
		}
		// The restore has to be proven, not assumed: without this the next
		// iteration's refusal could be left over from this one.
		if _, err := repository.LoadRouteSnapshot(ctx, claim, state, at); err != nil {
			t.Fatalf("%s fence restore left the load refused: error=%v", fence.name, err)
		}
	}
}
