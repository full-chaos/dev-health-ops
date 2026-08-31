//go:build integration

package providersync

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
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
		"sync_prs":true,
		"family_dataset_work_items":true,
		"family_dataset_work_item_labels":true,
		"family_dataset_work_item_projects":true,
		"family_dataset_work_item_history":true,
		"family_dataset_work_item_comments":true
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
	// The GITHUB list. This unit is inserted as ('github', 'work-items') below,
	// and github writes two destinations the shared work-item family does not.
	if err != nil || len(manifest.Batch.Effects) != len(githubWorkItemRouteDestinations()) {
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
	// The case above grows the payload, so payload_bytes stops matching and the
	// LENGTH comparison rejects it -- the digest never gets consulted. A
	// length-preserving edit is the one that isolates the digest fence, and it
	// is also the realistic corruption: a flipped byte, not an appended one.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_unit_effect_snapshots
SET payload = decode('00', 'hex') || substring(payload from 2)
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	); err != nil {
		t.Fatal(err)
	}
	var storedBytes, actualLength int
	if err := pool.QueryRow(ctx, `
SELECT payload_bytes, length(payload) FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	).Scan(&storedBytes, &actualLength); err != nil {
		t.Fatal(err)
	}
	if storedBytes != actualLength {
		t.Fatalf(
			"fixture is not length-preserving: payload_bytes=%d length=%d -- the length "+
				"fence would fire and the digest fence would go unexercised",
			storedBytes, actualLength,
		)
	}
	if _, err := repository.LoadRouteSnapshot(ctx, claim, state, now.Add(time.Second)); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("length-preserving tamper error=%v", err)
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
	// without a live lease. The refusal must be ErrLeaseLost and NOT
	// ErrPreparedRouteSnapshotNotFound: the row is still present and its
	// tenant/unit/generation key still matches exactly, so "not found" would
	// be a false statement about durable state. The wrong-tenant and
	// wrong-generation probes above are the cases that genuinely find nothing,
	// and they are the ones that keep reporting NotFound -- that contrast is
	// the whole point of separating the two.
	if _, err := repository.LoadRouteSnapshot(
		ctx, claim, state, now.Add(2*time.Second),
	); !errors.Is(err, ErrLeaseLost) {
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
	// snapshot the new owner is now responsible for -- and must be told it
	// lost the lease, not that the snapshot vanished.
	if _, err := repository.LoadRouteSnapshot(
		ctx, claim, state, now.Add(4*time.Second),
	); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner load error=%v", err)
	}
	// Now isolate the three lease fences one at a time. ReleaseForRetry clears
	// owner, expiry and status together, so the release above can never say
	// WHICH fence refused -- and a single missing fence would hide behind the
	// other two. Each iteration breaks exactly one column, proves the refusal,
	// restores it, and re-proves the load succeeds again, so no refusal can be
	// left over from the previous iteration.
	assertLeaseFencesIsolateTheSnapshotLoad(t, ctx, pool, repository, reclaimed, state, now)

	// The terminal-run fence, which lives on sync_runs rather than the unit.
	// loadEffectLedgerSQL has always excluded finished runs; this query gained
	// the same exclusion so the two cannot disagree about entitlement.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_runs SET status = 'success'
WHERE id = (SELECT sync_run_id FROM public.sync_run_units WHERE id = $1)`, reclaimed.ID,
	); err != nil {
		t.Fatal(err)
	}
	// A finalized run reports ITS OWN error, not lease loss -- the lease here
	// is live and correct, and saying "lease lost" would send an operator to
	// the wrong place entirely.
	if _, err := repository.LoadRouteSnapshot(
		ctx, reclaimed, state, now.Add(4*time.Second),
	); !errors.Is(err, ErrPreparedRouteSnapshotRunTerminal) {
		t.Fatalf("terminal-run fence load error=%v", err)
	}
	// partial_failed is a distinct element of the NOT IN list and a distinct
	// production state; covering only 'success' left the other two elements
	// pinned by nothing but a whole-predicate mutation.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_runs SET status = 'partial_failed'
WHERE id = (SELECT sync_run_id FROM public.sync_run_units WHERE id = $1)`, reclaimed.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.LoadRouteSnapshot(
		ctx, reclaimed, state, now.Add(4*time.Second),
	); !errors.Is(err, ErrPreparedRouteSnapshotRunTerminal) {
		t.Fatalf("partial_failed run fence load error=%v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_runs SET status = 'failed'
WHERE id = (SELECT sync_run_id FROM public.sync_run_units WHERE id = $1)`, reclaimed.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.LoadRouteSnapshot(
		ctx, reclaimed, state, now.Add(4*time.Second),
	); !errors.Is(err, ErrPreparedRouteSnapshotRunTerminal) {
		t.Fatalf("failed run fence load error=%v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_runs SET status = 'running'
WHERE id = (SELECT sync_run_id FROM public.sync_run_units WHERE id = $1)`, reclaimed.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.LoadRouteSnapshot(
		ctx, reclaimed, state, now.Add(4*time.Second),
	); err != nil {
		t.Fatalf("terminal-run fence restore left the load refused: %v", err)
	}

	// verifyPreparedRouteSnapshotRow's payload comparison: a stored payload
	// that no longer matches what this attempt would write must be refused on
	// re-prepare, not silently accepted because the ledger metadata still
	// agrees. Only the ledger-exists branch reads the row back, so a first
	// prepare never exercises this.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_unit_effect_snapshots
SET payload = decode('00', 'hex') || substring(payload from 2)
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		reclaimed.OrgID, reclaimed.ID, reclaimed.GenerationKey(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.PrepareRouteSnapshot(
		ctx, reclaimed, preparedGitHubWorkItemsFixture(t, reclaimed),
		ShadowComparison{Match: true}, now,
	); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("re-prepare over a corrupted payload error=%v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_unit_effect_snapshots
SET payload = decode('7b', 'hex') || substring(payload from 2)
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		reclaimed.OrgID, reclaimed.ID, reclaimed.GenerationKey(),
	); err != nil {
		t.Fatal(err)
	}

	// The ErrNoRows arm of verifyPreparedRouteSnapshotRow: ledger present,
	// sidecar row gone. That is a genuine ledger/sidecar disagreement and must
	// map to ErrEffectLedgerConflict -- distinct from the pass-through the
	// same function now uses for real database errors. Reverting that
	// remapping used to be silently green.
	var savedPayload []byte
	if err := pool.QueryRow(ctx, `
SELECT payload FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		reclaimed.OrgID, reclaimed.ID, reclaimed.GenerationKey(),
	).Scan(&savedPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
DELETE FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		reclaimed.OrgID, reclaimed.ID, reclaimed.GenerationKey(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.PrepareRouteSnapshot(
		ctx, reclaimed, preparedGitHubWorkItemsFixture(t, reclaimed),
		ShadowComparison{Match: true}, now,
	); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("re-prepare with the sidecar row deleted: error=%v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_unit_effect_snapshots (
    org_id, sync_run_unit_id, generation, provider, dataset_key,
    schema_version, content_digest, payload_bytes, payload, created_at)
VALUES ($1, $2, $3, 'github', 'work-items', $4, $5, $6, $7, $8)`,
		reclaimed.OrgID, reclaimed.ID, reclaimed.GenerationKey(),
		state.PreparedSnapshot.SchemaVersion, state.PreparedSnapshot.ContentDigest,
		len(savedPayload), savedPayload, now,
	); err != nil {
		t.Fatal(err)
	}
	if err := repository.Complete(
		ctx, reclaimed, map[string]any{
			"records":    16,
			"incomplete": []GitHubWorkItemsIncomplete{},
		}, nil,
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
		"sync_prs":true,
		"family_dataset_work_items":true,
		"family_dataset_work_item_labels":true,
		"family_dataset_work_item_projects":true,
		"family_dataset_work_item_history":true,
		"family_dataset_work_item_comments":true
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
		ctx, rollbackClaim, map[string]any{
			"records":    16,
			"incomplete": []GitHubWorkItemsIncomplete{},
		}, nil,
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
			err, ErrLeaseLost,
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

// TestPostgresPreparedRouteSnapshotRunsUnderTheRestrictedDomainRole executes
// the snapshot statements as the ACTUAL domain role rather than the table
// owner.
//
// Every other test in this file connects as the owner, who bypasses privilege
// checks entirely. That missing venue is precisely why a `FOR UPDATE` on the
// snapshot row survived review, a full mutation plan and a green gate: any
// row-locking clause requires an UPDATE-class privilege, the domain role holds
// only SELECT/INSERT/DELETE here, and so the re-prepare path failed with
// "permission denied" on every production attempt while passing locally 100%
// of the time. Owner-privileged tests cannot observe a privilege bug.
func TestPostgresPreparedRouteSnapshotRunsUnderTheRestrictedDomainRole(t *testing.T) {
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
	ownerPool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer ownerPool.Close()
	createProviderSyncFixture(t, ctx, ownerPool)
	seedProviderSyncFixture(t, ctx, ownerPool)
	if _, err := ownerPool.Exec(ctx, `
CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY);
INSERT INTO public.alembic_version (version_num) VALUES ('0093')`); err != nil {
		t.Fatal(err)
	}

	// The venue isolates the runtime snapshot contract: the snapshot table has
	// no UPDATE-class privilege and the domain role cannot read Alembic's
	// privileged migration ledger. Everything else is granted broadly on
	// purpose -- the role-posture manifests in internal/storage/postgres own the
	// full posture audit, not this venue. Successful
	// prepare/load/complete below is therefore readiness evidence derived from
	// the domain-accessible snapshot surface, not public.alembic_version.
	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). This role already DROPped
	// itself first, which makes a lone re-run safe, but two concurrent lanes
	// on the same shared kiac cluster still race on the fixed name and one
	// can drop the role out from under the other mid-test. Deriving it from
	// this call's own database identity removes the collision outright.
	role, err := containers.RoleName("providersync_domain_probe", instance)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP ROLE IF EXISTS ` + role,
		`CREATE ROLE ` + role + ` LOGIN PASSWORD 'probe'`,
		`GRANT USAGE ON SCHEMA public TO ` + role,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ` + role,
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ` + role,
		`REVOKE ALL PRIVILEGES ON TABLE public.alembic_version FROM ` + role,
		`REVOKE UPDATE ON TABLE public.sync_run_unit_effect_snapshots FROM ` + role,
	} {
		if _, err := ownerPool.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	restrictedURI, err := restrictedRoleURI(instance.URI, role, "probe")
	if err != nil {
		t.Fatal(err)
	}
	restrictedPool, err := pgxpool.New(ctx, restrictedURI)
	if err != nil {
		t.Fatal(err)
	}
	defer restrictedPool.Close()
	var current string
	if err := restrictedPool.QueryRow(ctx, "SELECT current_user").Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current != role {
		t.Fatalf("connected as %q, want the restricted role %q", current, role)
	}
	if _, err := restrictedPool.Exec(
		ctx, "SELECT version_num FROM public.alembic_version",
	); err == nil {
		t.Fatal("domain role unexpectedly gained SELECT on public.alembic_version")
	}

	repository, err := NewPostgresRepository(restrictedPool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	unitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, ownerPool, unitID, "incremental", `{
		"sync_prs":true,
		"family_dataset_work_items":true,
		"family_dataset_work_item_labels":true,
		"family_dataset_work_item_projects":true,
		"family_dataset_work_item_history":true,
		"family_dataset_work_item_comments":true
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
		t.Fatalf("prepare under the restricted role: %v", err)
	}

	// THE regression: the second prepare takes the ledger-exists branch, which
	// is the only path that reads the snapshot row back inside the
	// transaction. That read is what used to demand a row lock the role may
	// not take. A first prepare alone never reaches it, which is how this
	// stayed invisible.
	reprepared, err := repository.PrepareRouteSnapshot(
		ctx, claim, batch, ShadowComparison{Match: true}, now,
	)
	if err != nil {
		t.Fatalf("re-prepare under the restricted role: %v", err)
	}
	if !sameEffectManifest(state, reprepared) {
		t.Fatalf("re-prepare returned a different manifest: %+v vs %+v", state, reprepared)
	}
	if _, err := repository.LoadRouteSnapshot(ctx, claim, state, now.Add(time.Second)); err != nil {
		t.Fatalf("load under the restricted role: %v", err)
	}
	if err := repository.Complete(
		ctx, claim, map[string]any{
			"records":    16,
			"incomplete": []GitHubWorkItemsIncomplete{},
		}, nil, now, now.Add(time.Second),
	); err != nil {
		t.Fatalf("complete under the restricted role: %v", err)
	}
	assertPreparedSnapshotCount(t, ctx, ownerPool, claim, 0)
}

// restrictedRoleURI rewrites a connection URI's credentials, leaving host,
// port, database and parameters untouched.
func restrictedRoleURI(base string, role string, password string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String(), nil
}

// TestPostgresFailedUnitKeepsTheSnapshotReachable covers contract point 5's
// other half: a failed unit RETAINS its snapshot. Retention is only
// meaningful if the reference survives too. failUnitSQL used to replace the
// result document wholesale, which deleted the go_effect_ledger_v1 key and
// with it the prepared-snapshot reference -- leaving a row up to 64 MiB that
// nothing could ever find again, reclaimed only if the owning unit was
// eventually deleted and the CASCADE fired.
func TestPostgresFailedUnitKeepsTheSnapshotReachable(t *testing.T) {
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
	state, err := repository.PrepareRouteSnapshot(
		ctx, claim, preparedGitHubWorkItemsFixture(t, claim),
		ShadowComparison{Match: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Stamp the claimable-state keys a real unit accumulates before failing:
	// markExpiredLeaseRetryingSQL, the soft-timeout retry path, rate-limit
	// deferral and the budget guard all write into this same document, and
	// NOTHING clears them on claim. A terminal stamp that preserves them
	// freezes a live "will retry at T" claim onto a dead unit -- and the admin
	// integrations API projects these keys with no status gate.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET result = (COALESCE(result::jsonb, '{}'::jsonb) || $2::jsonb)::json
WHERE id = $1`, claim.ID, `{
		"next_retry_at": "2026-08-04T13:00:00Z",
		"retry_exhausted": false,
		"retry_reason": "provider_rate_limited",
		"rate_limit_deferred_until": "2026-08-04T13:30:00Z",
		"budget_deferred": true
	}`); err != nil {
		t.Fatal(err)
	}
	if err := repository.Fail(
		ctx, claim, "provider_unit_failed", now, now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}

	assertPreparedSnapshotCount(t, ctx, pool, claim, 1)
	var leftoverKeys []string
	if err := pool.QueryRow(ctx, `
SELECT COALESCE(array_agg(key ORDER BY key), ARRAY[]::text[])
FROM public.sync_run_units AS unit,
     LATERAL jsonb_object_keys(unit.result::jsonb) AS key
WHERE unit.id = $1
  AND key IN ('next_retry_at', 'retry_exhausted', 'retry_reason',
              'rate_limit_deferred_until', 'budget_deferred')`, claim.ID,
	).Scan(&leftoverKeys); err != nil {
		t.Fatal(err)
	}
	if len(leftoverKeys) != 0 {
		t.Fatalf(
			"terminal Fail preserved claimable-state keys %v; the admin API reads "+
				"these with no status gate and would report a failed unit as retrying",
			leftoverKeys,
		)
	}
	var status, category string
	var ledger []byte
	if err := pool.QueryRow(ctx, `
SELECT unit.status,
       COALESCE(unit.result::jsonb ->> 'error_category', ''),
       COALESCE((unit.result::jsonb -> 'go_effect_ledger_v1')::text, '')
FROM public.sync_run_units AS unit WHERE unit.id = $1`, claim.ID,
	).Scan(&status, &category, &ledger); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || category != "provider_unit_failed" {
		t.Fatalf("status=%q category=%q", status, category)
	}
	// The failure category is recorded AND the ledger survives. Asserting only
	// the category would pass against the wholesale replace this test exists
	// to catch.
	if len(ledger) == 0 {
		t.Fatal("Fail destroyed the go_effect_ledger_v1 key; the retained snapshot is unreachable")
	}
	survived, err := decodeEffectLedgerState(ledger)
	if err != nil {
		t.Fatalf("decode surviving ledger: %v", err)
	}
	if survived.SchemaVersion != "v2" || survived.PreparedSnapshot == nil ||
		!samePreparedRouteSnapshotReference(survived.PreparedSnapshot, state.PreparedSnapshot) {
		t.Fatalf("surviving ledger lost the snapshot reference: %+v", survived)
	}
}

// TestPostgresSnapshotTenancyIsEnforcedByCompositeForeignKey proves the Go
// integration schema carries 0093's structural tenant fence, not merely the
// reader's defensive join.
func TestPostgresSnapshotTenancyIsEnforcedByCompositeForeignKey(t *testing.T) {
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
	state, err := repository.PrepareRouteSnapshot(
		ctx, claim, preparedGitHubWorkItemsFixture(t, claim),
		ShadowComparison{Match: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}

	// The same globally unique unit UUID cannot be borrowed by another org.
	_, err = pool.Exec(ctx, `
INSERT INTO public.sync_run_unit_effect_snapshots (
    org_id, sync_run_unit_id, generation, provider, dataset_key,
    schema_version, content_digest, payload_bytes, payload, created_at)
SELECT 'org-intruder', sync_run_unit_id, generation, provider, dataset_key,
       schema_version, content_digest, payload_bytes, payload, created_at
FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("cross-tenant insert error=%v, want 23503 foreign_key_violation", err)
	}

	// The rejected insert does not disturb the owning tenant's durable row.
	if _, err := repository.LoadRouteSnapshot(ctx, claim, state, now.Add(time.Second)); err != nil {
		t.Fatalf("owning tenant load: %v", err)
	}
	// The reader independently retains its tenant join as defense in depth.
	intruder := claim
	intruder.OrgID = "org-intruder"
	if _, err := repository.LoadRouteSnapshot(
		ctx, intruder, state, now.Add(time.Second),
	); !errors.Is(err, ErrPreparedRouteSnapshotNotFound) {
		t.Fatalf("intruder load error=%v, want not-found", err)
	}
}

// TestPostgresFailedUnitSurvivesAJSONNullResult covers the sa.JSON hazard:
// result is a JSON column, so it can legitimately hold the literal `null`.
// `'null'::jsonb || '{}'::jsonb` raises a non-object-operand error, and that
// raise surfaces out of Fail() as ErrLeaseLost -- a failure to record a
// failure, reported as a lease problem, on a unit whose lease is fine.
func TestPostgresFailedUnitSurvivesAJSONNullResult(t *testing.T) {
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

	for _, predecessor := range []struct {
		name  string
		value any
	}{
		{"SQL NULL", nil},
		{"JSON literal null", "null"},
	} {
		t.Run(predecessor.name, func(t *testing.T) {
			unitID := uuid.NewString()
			seedWorkItemAliasUnit(t, ctx, pool, unitID, "incremental", `{
				"sync_prs":true,"family_dataset_work_items":true
			}`)
			if _, err := pool.Exec(ctx,
				"UPDATE public.sync_run_units SET result = $2 WHERE id = $1",
				unitID, predecessor.value,
			); err != nil {
				t.Fatal(err)
			}
			claim, err := repository.Claim(ctx, ClaimRequest{
				UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
				LeaseDuration: time.Minute, AllowExpiredRecovery: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := repository.Fail(
				ctx, claim, "provider_unit_failed", now, now.Add(time.Second),
			); err != nil {
				t.Fatalf("Fail over a %s result: %v", predecessor.name, err)
			}
			var status, category string
			if err := pool.QueryRow(ctx, `
SELECT status, COALESCE(result::jsonb ->> 'error_category', '')
FROM public.sync_run_units WHERE id = $1`, unitID,
			).Scan(&status, &category); err != nil {
				t.Fatal(err)
			}
			if status != "failed" || category != "provider_unit_failed" {
				t.Fatalf("status=%q category=%q", status, category)
			}
			// And the stamp must not INVENT a ledger key. Without the guard,
			// `-> 'go_effect_ledger_v1'` on a predecessor that has none yields
			// SQL NULL, and jsonb_build_object turns that into an explicit
			// `"go_effect_ledger_v1": null` -- a snapshot reference that reads
			// as present and decodes to nothing.
			var hasLedger bool
			if err := pool.QueryRow(ctx, `
SELECT COALESCE(result::jsonb ? 'go_effect_ledger_v1', false)
FROM public.sync_run_units WHERE id = $1`, unitID,
			).Scan(&hasLedger); err != nil {
				t.Fatal(err)
			}
			if hasLedger {
				t.Fatal("terminal Fail invented a go_effect_ledger_v1 key on a unit that never had one")
			}
		})
	}

	// A non-object result document is NOT a state Fail has to survive, and the
	// reason is worth pinning rather than leaving to inference: Claim refuses
	// such a unit outright, so no lease is ever held over one and Fail is
	// unreachable. Measured -- an earlier version of this test carried a JSON
	// scalar case that failed here, at Claim, not at Fail.
	scalarUnitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, scalarUnitID, "incremental", `{
		"sync_prs":true,"family_dataset_work_items":true
	}`)
	if _, err := pool.Exec(ctx,
		"UPDATE public.sync_run_units SET result = $2 WHERE id = $1",
		scalarUnitID, `"a string, not an object"`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Claim(ctx, ClaimRequest{
		UnitID: scalarUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	}); err == nil {
		t.Fatal("Claim accepted a unit whose result is a JSON scalar; Fail would then have to handle it")
	}
}
