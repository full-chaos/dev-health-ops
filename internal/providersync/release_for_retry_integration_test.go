//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestReleaseForRetryPreservesAnInFlightEffectLedger is codex H1's second
// half (CHAOS-3122): ReleaseForRetry previously overwrote the whole `result`
// jsonb column with just {"error_category": ...}, deleting the
// go_effect_ledger_v1 key. If an earlier attempt had already begun writing
// an EffectReadbackRequired effect (WriteEffect landed in ClickHouse, then
// the process died before CommitEffect) and a LATER attempt then failed for
// any reason -- a capped paginated fetch being the concrete case that made
// this newly, deterministically reachable -- ReleaseForRetry wiped the
// ledger recording that in-flight write. The next attempt's LoadEffects then
// returned ErrEffectLedgerNotFound, forgot the frozen normalizedAt/digest,
// and rebuilt a brand new manifest with no way to classify the earlier
// ClickHouse row as exact, absent, or conflict -- the exact evidence
// InspectEffect-based recovery depends on.
func TestReleaseForRetryPreservesAnInFlightEffectLedger(t *testing.T) {
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
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Build a minimal EffectReadbackRequired effect and drive it to
	// GenerationBlockWriting -- the state a real WriteEffect-then-crash
	// leaves behind, per GitHubPullRequestClickHouseEffects and
	// GitHubRepositoryClickHouseEffects's own doc comments.
	effect, err := BuildEffectBatch(
		"repos", EffectReadbackRequired,
		[]json.RawMessage{json.RawMessage(`{"probe":"value"}`)},
	)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewEffectLedgerState(claim, []EffectBatch{effect}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.PrepareEffects(ctx, claim, state, now); err != nil {
		t.Fatal(err)
	}
	if err := repository.BeginEffect(
		ctx, claim, 0, effect.ContentDigest, now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}

	// Simulate the failure path: Collect (or anything else in Execute)
	// errors on this attempt, and the job handler releases the unit for a
	// bounded retry -- exactly what cmd's providerunit.go does for any
	// non-deterministic, non-exhausted error.
	releasedAt := now.Add(10 * time.Second)
	if err := repository.ReleaseForRetry(ctx, claim, releasedAt); err != nil {
		t.Fatal(err)
	}

	// A fresh attempt reclaims the unit (ReleaseForRetry returns it to
	// `dispatching` with no lease owner, so a normal claim -- not expired-
	// lease recovery -- picks it up next).
	reclaimedAt := releasedAt.Add(time.Second)
	reclaimed, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: reclaimedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The whole point: LoadEffects must still find the ledger, not
	// ErrEffectLedgerNotFound, and it must be the SAME manifest at the SAME
	// frozen instant -- proving a later attempt can still reuse
	// normalizedAt and reach InspectEffect-based reconciliation instead of
	// silently starting a brand new, unreconciled write.
	persisted, err := repository.LoadEffects(ctx, reclaimed, reclaimedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("LoadEffects after ReleaseForRetry: %v (ledger was wiped)", err)
	}
	if !persisted.CreatedAt.UTC().Equal(now.UTC()) {
		t.Fatalf("persisted.CreatedAt=%v want %v (frozen normalizedAt lost)", persisted.CreatedAt, now)
	}
	if len(persisted.Effects) != 1 || persisted.Effects[0].Status != GenerationBlockWriting ||
		persisted.Effects[0].ContentDigest != effect.ContentDigest {
		t.Fatalf("persisted effect=%+v want status=writing digest=%s",
			persisted.Effects[0], effect.ContentDigest)
	}

	// error_category must still be set (ReleaseForRetry's own contract),
	// alongside the preserved ledger -- this is a merge, not a swap to a
	// second exclusive key.
	var errorCategory string
	if err := pool.QueryRow(ctx, `
SELECT result::jsonb ->> 'error_category' FROM public.sync_run_units WHERE id = $1`,
		firstUnitID,
	).Scan(&errorCategory); err != nil {
		t.Fatal(err)
	}
	if errorCategory != "provider_unit_retryable" {
		t.Fatalf("error_category=%q want provider_unit_retryable", errorCategory)
	}
}
