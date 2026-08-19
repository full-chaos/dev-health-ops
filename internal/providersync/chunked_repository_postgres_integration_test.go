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

func TestChunkedPostgresCheckpointResumeFinalizationAndLeaseHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if closeErr := instance.Close(closeContext); closeErr != nil {
			t.Errorf("terminate PostgreSQL: %v", closeErr)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET dataset_key='cicd', processor_flags='{"sync_git":true}'::jsonb WHERE id=$1`, firstUnitID); err != nil {
		t.Fatal(err)
	}

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute, AllowExpiredRecovery: true})
	if err != nil {
		t.Fatal(err)
	}
	effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"1"}`)
	first, err := repository.PrepareChunk(ctx, claim, PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 0, TotalChunks: 0, CursorAfter: "cursor-1", Effects: []EffectBatch{effect},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if first.TotalChunks != 0 {
		t.Fatalf("provisional total=%d", first.TotalChunks)
	}
	if err := repository.BeginChunkEffect(ctx, claim, 0, 0, effect.ContentDigest, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.CommitChunkEffect(ctx, claim, 0, 0, effect.ContentDigest, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkChunkCommitted(ctx, claim, 0, first.ManifestDigest, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}

	secondEffect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"2"}`)
	second, err := repository.PrepareChunk(ctx, claim, PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 1, TotalChunks: 0, Effects: []EffectBatch{secondEffect}, InventoryComplete: true,
		Result: map[string]any{"complete": true},
	}, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.BeginChunkEffect(ctx, claim, 1, 0, secondEffect.ContentDigest, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.CommitChunkEffect(ctx, claim, 1, 0, secondEffect.ContentDigest, now.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkChunkCommitted(ctx, claim, 1, second.ManifestDigest, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkInventoryComplete(ctx, claim, now.Add(8*time.Second)); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := repository.LoadChunkCheckpoint(ctx, claim, now.Add(9*time.Second))
	if err != nil || !checkpoint.InventoryComplete || checkpoint.TotalChunks != 2 || checkpoint.NextOrdinal != 2 {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	final, err := repository.LoadPreparedChunk(ctx, claim, 1, now.Add(9*time.Second))
	if err != nil || final.TotalChunks != 2 || !final.InventoryComplete {
		t.Fatalf("final=%+v err=%v", final, err)
	}

	other := claim
	other.OrgID = "other-tenant"
	if _, err := repository.LoadChunkCheckpoint(ctx, other, now.Add(9*time.Second)); !errors.Is(err, ErrChunkCheckpointNotFound) {
		t.Fatalf("cross-tenant checkpoint read=%v", err)
	}
	if _, err := repository.LoadPreparedChunk(ctx, other, 1, now.Add(9*time.Second)); !errors.Is(err, ErrPreparedChunkNotFound) {
		t.Fatalf("cross-tenant sidecar read=%v", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET lease_expires_at=$2 WHERE id=$1`, firstUnitID, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	handoff, err := repository.Claim(ctx, ClaimRequest{UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now.Add(10 * time.Second), LeaseDuration: time.Minute, AllowExpiredRecovery: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.LoadChunkCheckpoint(ctx, handoff, now.Add(10*time.Second)); err != nil {
		t.Fatalf("lease-owner handoff lost checkpoint: %v", err)
	}
}

// A streaming emission that splits into several prepared sub-chunks carries the
// provider continuation ONLY on its last sub-chunk. The statements must leave
// the durable cursor alone for the others: writing their empty CursorAfter
// resets the route to the start of the provider inventory on the next resume.
//
// This asserts the real statements. The in-memory ChunkedEffectStore double
// already guards both writes, so an executor-level test cannot observe a
// regression here.
func TestChunkedPostgresCursorSurvivesEmptyContinuationAndFailReclaimsSidecars(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if closeErr := instance.Close(closeContext); closeErr != nil {
			t.Errorf("terminate PostgreSQL: %v", closeErr)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET dataset_key='cicd', processor_flags='{"sync_git":true}'::jsonb WHERE id=$1`, firstUnitID); err != nil {
		t.Fatal(err)
	}

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute, AllowExpiredRecovery: true})
	if err != nil {
		t.Fatal(err)
	}

	prepareAndCommit := func(ordinal int, at time.Time, cursorAfter string, rowID string) {
		t.Helper()
		effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"`+rowID+`"}`)
		prepared, prepareErr := repository.PrepareChunk(ctx, claim, PreparedProviderChunk{
			SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
			Ordinal: ordinal, TotalChunks: 0, CursorAfter: cursorAfter, Effects: []EffectBatch{effect},
		}, at)
		if prepareErr != nil {
			t.Fatalf("prepare ordinal=%d: %v", ordinal, prepareErr)
		}
		if err := repository.BeginChunkEffect(ctx, claim, ordinal, 0, effect.ContentDigest, at.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := repository.CommitChunkEffect(ctx, claim, ordinal, 0, effect.ContentDigest, at.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := repository.MarkChunkCommitted(ctx, claim, ordinal, prepared.ManifestDigest, at.Add(3*time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	// Last sub-chunk of emission one: publishes the provider continuation.
	prepareAndCommit(0, now, "cursor-page-1", "1")
	checkpoint, err := repository.LoadChunkCheckpoint(ctx, claim, now.Add(4*time.Second))
	if err != nil || checkpoint.NextCursor != "cursor-page-1" {
		t.Fatalf("published cursor checkpoint=%+v err=%v", checkpoint, err)
	}

	// Non-final sub-chunk of emission two: no continuation of its own.
	effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"2"}`)
	prepared, err := repository.PrepareChunk(ctx, claim, PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 1, TotalChunks: 0, CursorAfter: "", Effects: []EffectBatch{effect},
	}, now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err = repository.LoadChunkCheckpoint(ctx, claim, now.Add(6*time.Second))
	if err != nil || checkpoint.NextCursor != "cursor-page-1" {
		t.Fatalf("PrepareChunk regressed the resume cursor: checkpoint=%+v err=%v", checkpoint, err)
	}
	if err := repository.BeginChunkEffect(ctx, claim, 1, 0, effect.ContentDigest, now.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.CommitChunkEffect(ctx, claim, 1, 0, effect.ContentDigest, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkChunkCommitted(ctx, claim, 1, prepared.ManifestDigest, now.Add(8*time.Second)); err != nil {
		t.Fatal(err)
	}
	checkpoint, err = repository.LoadChunkCheckpoint(ctx, claim, now.Add(9*time.Second))
	if err != nil || checkpoint.NextCursor != "cursor-page-1" || checkpoint.NextOrdinal != 2 {
		t.Fatalf("MarkChunkCommitted regressed the resume cursor: checkpoint=%+v err=%v", checkpoint, err)
	}

	// The aggregate belongs to the FINAL chunk. Earlier chunks have Result=nil
	// and must leave the column SQL NULL — storing the JSONB scalar `null`
	// instead would satisfy the first-writer-wins guard and permanently drop
	// the real aggregate, because `'null'::jsonb IS NULL` is false.
	var aggregateIsSQLNull bool
	if err := pool.QueryRow(ctx,
		`SELECT aggregate_result IS NULL FROM public.sync_run_unit_chunk_checkpoints
		 WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3`,
		claim.OrgID, claim.ID, claim.GenerationKey()).Scan(&aggregateIsSQLNull); err != nil {
		t.Fatal(err)
	}
	if !aggregateIsSQLNull {
		t.Fatal("non-final chunks stored a non-NULL aggregate_result; the final aggregate can no longer be written")
	}
	finalEffect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"3"}`)
	finalChunk, err := repository.PrepareChunk(ctx, claim, PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 2, TotalChunks: 0, CursorAfter: "cursor-page-2", Effects: []EffectBatch{finalEffect},
		InventoryComplete: true, Result: map[string]any{"pipeline_runs_synced": 3},
	}, now.Add(9*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err = repository.LoadChunkCheckpoint(ctx, claim, now.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.AggregateResult == nil || checkpoint.AggregateDigest == "" ||
		checkpoint.AggregateDigest == chunkResultDigest(nil) {
		t.Fatalf("final aggregate was not persisted: result=%v digest=%q",
			checkpoint.AggregateResult, checkpoint.AggregateDigest)
	}
	if err := repository.BeginChunkEffect(ctx, claim, 2, 0, finalEffect.ContentDigest, now.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.CommitChunkEffect(ctx, claim, 2, 0, finalEffect.ContentDigest, now.Add(12*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.MarkChunkCommitted(ctx, claim, 2, finalChunk.ManifestDigest, now.Add(13*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Terminalizing the unit makes every sidecar unreachable, so Fail must
	// reclaim them; nothing else ever will.
	if err := repository.Fail(ctx, claim, "provider_unit_exhausted", now, now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	var checkpoints, sidecars int
	if err := pool.QueryRow(ctx, `SELECT
	  (SELECT count(*) FROM public.sync_run_unit_chunk_checkpoints WHERE sync_run_unit_id=$1::uuid),
	  (SELECT count(*) FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id=$1::uuid)`,
		firstUnitID).Scan(&checkpoints, &sidecars); err != nil {
		t.Fatal(err)
	}
	if checkpoints != 0 || sidecars != 0 {
		t.Fatalf("Fail retained chunk state: checkpoints=%d sidecars=%d", checkpoints, sidecars)
	}
}
