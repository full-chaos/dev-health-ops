//go:build integration

package providersync

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestChunkedPostgresFinalizeOfManyChunksKeepsTheLease pins CHAOS-14242. MarkInventoryComplete holds the unit row lock
// (FOR UPDATE OF unit) for its whole transaction, and the heartbeat's renewal updates that same row. When the finalize
// stamped total_chunks on every chunk, a unit with ~140,000 chunks held the lock past its lease: the renewal waited, the
// lease expired, the attempt ended with ErrLeaseLost and lease repair failed the unit as worker_lost.
//
// Time is driven by a per-row trigger on the chunk table and the lease constants: every chunk row the finalize UPDATE
// touches costs 100 ms, and the session's lease is 2 s with a 500 ms heartbeat. Stamping the 60 earlier chunks costs
// 6 s, three leases; stamping only the final chunk costs 100 ms.
func TestChunkedPostgresFinalizeOfManyChunksKeepsTheLease(t *testing.T) {
	const (
		earlierChunks = 60
		lease         = 2 * time.Second
		heartbeat     = 500 * time.Millisecond
	)
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
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: time.Now(),
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	total := earlierChunks + 1
	for ordinal := 0; ordinal < total; ordinal++ {
		final := ordinal == total-1
		effect := effectBatchFixture(t, "ci_pipeline_runs", EffectReadbackRequired,
			`{"org_id":"org-acme","run_id":"`+strconv.Itoa(ordinal)+`"}`)
		chunk := PreparedProviderChunk{
			SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion,
			Ordinal: ordinal, CursorAfter: "cursor-" + strconv.Itoa(ordinal), Effects: []EffectBatch{effect},
			InventoryComplete: final,
		}
		if final {
			chunk.Result = map[string]any{"complete": true}
		}
		prepared, err := repository.PrepareChunk(ctx, claim, chunk, time.Now())
		if err != nil {
			t.Fatalf("prepare ordinal=%d: %v", ordinal, err)
		}
		if err := repository.BeginChunkEffect(ctx, claim, ordinal, 0, effect.ContentDigest, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := repository.CommitChunkEffect(ctx, claim, ordinal, 0, effect.ContentDigest, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := repository.MarkChunkCommitted(ctx, claim, ordinal, prepared.ManifestDigest, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	type chunkRow struct {
		total   int
		updated time.Time
	}
	readChunks := func() map[int]chunkRow {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT ordinal, total_chunks, updated_at FROM public.sync_run_unit_effect_chunks
			WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3`, claim.OrgID, claim.ID, claim.GenerationKey())
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[int]chunkRow{}
		for rows.Next() {
			var ordinal int
			var row chunkRow
			if err := rows.Scan(&ordinal, &row.total, &row.updated); err != nil {
				t.Fatal(err)
			}
			out[ordinal] = row
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := readChunks()
	if len(before) != total {
		t.Fatalf("seeded %d chunks, stored %d", total, len(before))
	}
	if _, err := pool.Exec(ctx, `
CREATE FUNCTION slow_chunk_update_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_sleep(0.1); RETURN NEW; END $$;
CREATE TRIGGER slow_chunk_update BEFORE UPDATE ON public.sync_run_unit_effect_chunks
FOR EACH ROW EXECUTE FUNCTION slow_chunk_update_fn();`); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	if err := repository.Renew(ctx, claim, now, now.Add(lease)); err != nil {
		t.Fatal(err)
	}
	claim.LeaseExpiresAt = now.Add(lease)
	session := &LeaseSession{Repository: repository, Claim: claim, LeaseDuration: lease, Deadline: now.Add(10 * time.Minute)}
	runErr := session.Run(ctx, heartbeat, func(workContext context.Context, _ providerfoundation.LeaseGuard) error {
		return repository.MarkInventoryComplete(workContext, claim, time.Now())
	})
	if runErr != nil {
		var expired bool
		_ = pool.QueryRow(ctx, `SELECT lease_expires_at <= now() FROM public.sync_run_units WHERE id=$1`, firstUnitID).Scan(&expired)
		t.Fatalf("finalize of %d chunks under a %s lease: %v (lease expired: %v, so lease repair would fail the unit)",
			total, lease, runErr, expired)
	}

	after := readChunks()
	for ordinal := 0; ordinal < total-1; ordinal++ {
		if after[ordinal] != before[ordinal] {
			t.Fatalf("finalize rewrote earlier chunk %d: before %+v after %+v", ordinal, before[ordinal], after[ordinal])
		}
	}
	if after[total-1].total != total {
		t.Fatalf("final chunk total_chunks=%d, want %d", after[total-1].total, total)
	}
	checkpoint, err := repository.LoadChunkCheckpoint(ctx, claim, time.Now())
	if err != nil || !checkpoint.InventoryComplete || checkpoint.TotalChunks != total {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	var result CompleteRouteExecutionResult
	if err := loadChunkedFinalResult(ctx, claim, repository, nil, checkpoint.TotalChunks, &result, time.Now()); err != nil {
		t.Fatalf("final result from the stamped final chunk: %v", err)
	}

	startedAt := now.Add(-time.Minute)
	if err := repository.Complete(ctx, claim, result.Result, nil, startedAt, time.Now()); err != nil {
		t.Fatalf("complete after finalize: %v", err)
	}
	var status string
	var checkpoints, chunks, finalize int
	if err := pool.QueryRow(ctx, `SELECT status FROM public.sync_run_units WHERE id=$1`, firstUnitID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_run_unit_chunk_checkpoints WHERE sync_run_unit_id=$1::uuid`, firstUnitID).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id=$1::uuid`, firstUnitID).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_dispatch_outbox
		WHERE sync_run_id=$1::uuid AND kind='finalize_sync_run' AND status='pending'`, firstRunID).Scan(&finalize); err != nil {
		t.Fatal(err)
	}
	if status != "success" || checkpoints != 0 || chunks != 0 || finalize != 1 {
		t.Fatalf("after Complete: status=%q checkpoints=%d chunks=%d finalize_outbox=%d, want success 0 0 1",
			status, checkpoints, chunks, finalize)
	}
}
