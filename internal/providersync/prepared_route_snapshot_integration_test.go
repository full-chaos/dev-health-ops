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
	if _, err := repository.LoadRouteSnapshot(
		ctx, reclaimed, state, now.Add(4*time.Second),
	); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("terminal-run fence load error=%v", err)
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

	// The venue isolates ONE property: the snapshot table carries no
	// UPDATE-class privilege. Everything else is granted broadly on purpose --
	// this test is not a posture audit (internal/domaingrants owns that), and
	// a venue that also under-grants unrelated tables would fail for reasons
	// that say nothing about the defect class under test. Granting wide and
	// revoking exactly UPDATE on the snapshot table makes any failure here
	// attributable to that single missing privilege.
	const role = "providersync_domain_probe"
	for _, statement := range []string{
		`DROP ROLE IF EXISTS ` + role,
		`CREATE ROLE ` + role + ` LOGIN PASSWORD 'probe'`,
		`GRANT USAGE ON SCHEMA public TO ` + role,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ` + role,
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ` + role,
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

	repository, err := NewPostgresRepository(restrictedPool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	unitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, ownerPool, unitID, "incremental", `{
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
		ctx, claim, map[string]any{"records": 16}, nil, now, now.Add(time.Second),
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
	if err := repository.Fail(
		ctx, claim, "provider_unit_failed", now, now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}

	assertPreparedSnapshotCount(t, ctx, pool, claim, 1)
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

// TestPostgresSnapshotTenancyIsEnforcedByReadsNotByStructure pins a KNOWN and
// currently accepted gap, so it is a measured fact rather than an assumption.
//
// The table's FK is (sync_run_unit_id) -> sync_run_units(id), which says
// nothing about org_id. So a row can be INSERTED claiming an org that does not
// own the unit. Nothing in production can write one -- the route is
// unregistered and the only writer derives org_id from the claim -- but the
// schema does not prevent it.
//
// Reads are safe, and that is what this test proves rather than asserts: the
// load joins on BOTH unit.id and unit.org_id, so a mismatched row is
// unreachable no matter how it got there.
//
// Making it structural needs a composite FK to sync_run_units(org_id, id) and
// therefore a UNIQUE index on that pair -- a CONCURRENTLY build plus a
// NOT VALID/VALIDATE FK against a hot table. That is deliberately NOT done in
// this PR, which activates nothing; it is recorded as pre-activation debt.
func TestPostgresSnapshotTenancyIsEnforcedByReadsNotByStructure(t *testing.T) {
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

	// The gap, executed: a second row for the SAME unit under a DIFFERENT org.
	// If a future migration makes this structural, this INSERT starts failing
	// and this test is the thing that tells you the debt was paid.
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_unit_effect_snapshots (
    org_id, sync_run_unit_id, generation, provider, dataset_key,
    schema_version, content_digest, payload_bytes, payload, created_at)
SELECT 'org-intruder', sync_run_unit_id, generation, provider, dataset_key,
       schema_version, content_digest, payload_bytes, payload, created_at
FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3`,
		claim.OrgID, claim.ID, claim.GenerationKey(),
	); err != nil {
		t.Fatalf(
			"cross-tenant insert was refused: %v -- if a composite FK landed, "+
				"delete this test and the debt note in the PR body", err,
		)
	}

	// Reads stay correct: the owning tenant still loads its own row...
	if _, err := repository.LoadRouteSnapshot(ctx, claim, state, now.Add(time.Second)); err != nil {
		t.Fatalf("owning tenant load: %v", err)
	}
	// ...and the intruder's row is unreachable, because the join requires the
	// unit's org_id to match the snapshot's.
	intruder := claim
	intruder.OrgID = "org-intruder"
	if _, err := repository.LoadRouteSnapshot(
		ctx, intruder, state, now.Add(time.Second),
	); !errors.Is(err, ErrPreparedRouteSnapshotNotFound) {
		t.Fatalf("intruder load error=%v, want not-found", err)
	}
}
