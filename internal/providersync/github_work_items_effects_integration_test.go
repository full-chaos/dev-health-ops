//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestGitHubWorkItemEffectRecoveryHoldsCompletionAndWatermarks exercises the
// real Postgres effect ledger around both crash windows owned by this layer:
// one destination lands before its CommitEffect acknowledgement, then all 16
// effects commit before the worker can call PostgresRepository.Complete.
// Neither window may advance an alias watermark.
func TestGitHubWorkItemEffectRecoveryHoldsCompletionAndWatermarks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	unitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, unitID, "incremental", `{
		"family_dataset_work_items": true,
		"family_dataset_work_item_labels": true,
		"family_dataset_work_item_history": true,
		"family_dataset_work_item_comments": true
	}`)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	first, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{
		WorkItems: []json.RawMessage{
			json.RawMessage(`{"org_id":"org-acme","work_item_id":"gh:acme/api#1"}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := newSemanticWorkItemEffectBackend()
	backend.failAfterWrite = "work_item_interactions"
	backend.failure = errors.New("process exited after ClickHouse accepted rows")
	firstSink := githubWorkItemEffectsFixture(backend)
	firstSink.Lease = leaseGuardAt(repository, first, now)
	_, err = (EffectCommitter{
		Ledger: repository, Sink: firstSink, Readback: firstSink,
		Now: func() time.Time { return now },
	}).Commit(ctx, first, effects, now)
	if !errors.Is(err, backend.failure) {
		t.Fatalf("first commit error=%v", err)
	}
	assertWorkItemUnitStillRunningWithoutWatermark(t, ctx, pool, unitID)

	recoveryNow := now.Add(61 * time.Second)
	recovered, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: recoveryNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend.failAfterWrite = ""
	recoveredSink := githubWorkItemEffectsFixture(backend)
	recoveredSink.Lease = leaseGuardAt(repository, recovered, recoveryNow)
	result, err := (EffectCommitter{
		Ledger: repository, Sink: recoveredSink, Readback: recoveredSink,
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recovered, effects, now)
	if err != nil || result.MarkedCommitted != 1 ||
		result.MarkedCommitted+result.Skipped+result.Written != 16 {
		t.Fatalf("recovery result=%+v error=%v", result, err)
	}
	assertWorkItemUnitStillRunningWithoutWatermark(t, ctx, pool, unitID)

	// Simulate a second death after every effect was durably committed but
	// before Complete atomically stamps SUCCESS + family watermarks.
	thirdNow := recoveryNow.Add(61 * time.Second)
	third, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: thirdNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	thirdSink := githubWorkItemEffectsFixture(backend)
	thirdSink.Lease = leaseGuardAt(repository, third, thirdNow)
	result, err = (EffectCommitter{
		Ledger: repository, Sink: thirdSink, Readback: thirdSink,
		Now: func() time.Time { return thirdNow },
	}).Commit(ctx, third, effects, now)
	if err != nil || result.Skipped != 16 || result.Written != 0 ||
		result.MarkedCommitted != 0 {
		t.Fatalf("all-committed recovery result=%+v error=%v", result, err)
	}
	assertWorkItemUnitStillRunningWithoutWatermark(t, ctx, pool, unitID)
}

func assertWorkItemUnitStillRunningWithoutWatermark(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	unitID string,
) {
	t.Helper()
	var status string
	var watermarkCount int
	if err := pool.QueryRow(ctx, `
SELECT status FROM public.sync_run_units WHERE id = $1`, unitID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.sync_watermarks
WHERE org_id = 'org-acme'
  AND source_id = 'acme/api'
  AND dataset_key IN (
    'work-items', 'work-item-labels', 'work-item-projects',
    'work-item-history', 'work-item-comments'
  )`).Scan(&watermarkCount); err != nil {
		t.Fatal(err)
	}
	if status != "running" || watermarkCount != 0 {
		t.Fatalf("status=%q work-item watermarks=%d", status, watermarkCount)
	}
}
