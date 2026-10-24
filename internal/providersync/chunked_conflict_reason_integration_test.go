//go:build integration

package providersync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestChunkedPostgresConflictsCarrySites(t *testing.T) {
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
	effect := func(run string) EffectBatch {
		return effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired, `{"org_id":"org-acme","run_id":"`+run+`"}`)
	}
	chunk := func(ordinal, total int, run string) PreparedProviderChunk {
		return PreparedProviderChunk{
			SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
			Ordinal: ordinal, TotalChunks: total, InventoryComplete: true, CursorAfter: "end",
			Effects: []EffectBatch{effect(run)}, Result: map[string]any{"complete": true},
		}
	}
	require := func(err error, site string, detail ...string) {
		t.Helper()
		if !errors.Is(err, ErrChunkCheckpointConflict) {
			t.Fatalf("err = %v, want a checkpoint conflict", err)
		}
		var store *ChunkStoreError
		if errors.As(err, &store) {
			t.Fatalf("err = %v: a state mismatch must not look like a store error", err)
		}
		if got := ChunkConflictSite(err); got != site {
			t.Fatalf("site = %q, want %q (err %v)", got, site, err)
		}
		for _, want := range detail {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want detail %q", err, want)
			}
		}
	}

	first, err := repository.PrepareChunk(ctx, claim, chunk(0, 1, "1"), now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repository.PrepareChunk(ctx, claim, chunk(1, 2, "2"), now.Add(time.Second))
	require(err, "prepareChunkInTx.1", "checkpoint_total=1", "chunk_total=2", "ordinal=1", "prepared=1")

	_, err = repository.PrepareChunk(ctx, claim, chunk(0, 1, "other"), now.Add(2*time.Second))
	require(err, "prepareChunkInTx.3", "ordinal=0", "prepared=1", "digest_match=false")

	if _, err := repository.LoadPreparedChunk(ctx, claim, 0, now.Add(3*time.Second)); err != nil {
		t.Fatalf("an intact row must load: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_unit_effect_chunks SET manifest_digest=$2 WHERE sync_run_unit_id=$1 AND ordinal=0`, firstUnitID, first.ManifestDigest+"-changed"); err != nil {
		t.Fatal(err)
	}
	_, err = repository.LoadPreparedChunk(ctx, claim, 0, now.Add(4*time.Second))
	require(err, "loadPreparedChunkRow.1", "digest_match=false", "ordinal=0", "stored_ordinal=0", "route_match=true")
}
