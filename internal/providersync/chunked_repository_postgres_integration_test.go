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
	if err != nil { t.Fatal(err) }
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if closeErr := instance.Close(closeContext); closeErr != nil { t.Errorf("terminate PostgreSQL: %v", closeErr) }
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil { t.Fatal(err) }
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET dataset_key='cicd', processor_flags='{"sync_git":true}'::jsonb WHERE id=$1`, firstUnitID); err != nil { t.Fatal(err) }

	repository, err := NewPostgresRepository(pool)
	if err != nil { t.Fatal(err) }
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute, AllowExpiredRecovery: true})
	if err != nil { t.Fatal(err) }
	effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"1"}`)
	first, err := repository.PrepareChunk(ctx, claim, PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 0, TotalChunks: 0, CursorAfter: "cursor-1", Effects: []EffectBatch{effect},
	}, now)
	if err != nil { t.Fatal(err) }
	if first.TotalChunks != 0 { t.Fatalf("provisional total=%d", first.TotalChunks) }
	if err := repository.BeginChunkEffect(ctx, claim, 0, 0, effect.ContentDigest, now.Add(time.Second)); err != nil { t.Fatal(err) }
	if err := repository.CommitChunkEffect(ctx, claim, 0, 0, effect.ContentDigest, now.Add(2*time.Second)); err != nil { t.Fatal(err) }
	if err := repository.MarkChunkCommitted(ctx, claim, 0, first.ManifestDigest, now.Add(3*time.Second)); err != nil { t.Fatal(err) }

	secondEffect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"2"}`)
	second, err := repository.PrepareChunk(ctx, claim, PreparedProviderChunk{
		SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
		Ordinal: 1, TotalChunks: 0, Effects: []EffectBatch{secondEffect}, InventoryComplete: true,
		Result: map[string]any{"complete": true},
	}, now.Add(4*time.Second))
	if err != nil { t.Fatal(err) }
	if err := repository.BeginChunkEffect(ctx, claim, 1, 0, secondEffect.ContentDigest, now.Add(5*time.Second)); err != nil { t.Fatal(err) }
	if err := repository.CommitChunkEffect(ctx, claim, 1, 0, secondEffect.ContentDigest, now.Add(6*time.Second)); err != nil { t.Fatal(err) }
	if err := repository.MarkChunkCommitted(ctx, claim, 1, second.ManifestDigest, now.Add(7*time.Second)); err != nil { t.Fatal(err) }
	if err := repository.MarkInventoryComplete(ctx, claim, now.Add(8*time.Second)); err != nil { t.Fatal(err) }
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

	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET lease_expires_at=$2 WHERE id=$1`, firstUnitID, now.Add(-time.Second)); err != nil { t.Fatal(err) }
	handoff, err := repository.Claim(ctx, ClaimRequest{UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now.Add(10*time.Second), LeaseDuration: time.Minute, AllowExpiredRecovery: true})
	if err != nil { t.Fatal(err) }
	if _, err := repository.LoadChunkCheckpoint(ctx, handoff, now.Add(10*time.Second)); err != nil {
		t.Fatalf("lease-owner handoff lost checkpoint: %v", err)
	}
}
