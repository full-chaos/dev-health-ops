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

// CHAOS-11772 (T5): chunks whose sidecar length sits at a power-of-ten crossing go through the real store. The
// payload_bytes column, the length of the stored payload bytes and the payload_bytes inside the JSON must be one
// number, and the first Begin / Load of the chunk must succeed. With the three-pass loop the load fails with the
// named ChunkSidecarMismatchError.
func TestChunkedPostgresStoresACrossingChunkWithAConsistentPayloadBytes(t *testing.T) {
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
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now, LeaseDuration: time.Hour, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	for ordinal, target := range []int{1001, 10002, 99999, 100003} {
		chunk := chunkEndingAt(t, target)
		chunk.Ordinal = ordinal
		chunk.ManifestDigest = preparedChunkDigest(chunk)
		at := now.Add(time.Duration(ordinal+1) * time.Second)
		group, err := repository.PrepareChunkGroup(ctx, claim, []PreparedProviderChunk{chunk}, at)
		if err != nil {
			t.Fatalf("length %d: prepare: %v", target, err)
		}
		digest := group[0].Effects[0].ContentDigest
		if err := repository.BeginChunkEffect(ctx, claim, ordinal, 0, digest, at); err != nil {
			var mismatch *ChunkSidecarMismatchError
			if errors.As(err, &mismatch) {
				t.Errorf("length %d: payload_bytes column=%d payload=%d", target, mismatch.Stored, mismatch.Payload)
			} else {
				t.Errorf("length %d: begin: %v", target, err)
			}
			continue
		}
		if _, err := repository.LoadPreparedChunk(ctx, claim, ordinal, at); err != nil {
			t.Errorf("length %d: load: %v", target, err)
		}
		encoded, err := encodedPreparedChunkPayload(chunk)
		if err != nil {
			t.Fatal(err)
		}
		var column, embedded int
		if err := pool.QueryRow(ctx, `SELECT payload_bytes, (payload->>'payload_bytes')::int FROM public.sync_run_unit_effect_chunks
WHERE org_id=$1 AND sync_run_unit_id=$2::uuid AND generation=$3 AND ordinal=$4`,
			claim.OrgID, claim.ID, claim.GenerationKey(), ordinal).Scan(&column, &embedded); err != nil {
			t.Fatal(err)
		}
		if column != len(encoded) || embedded != len(encoded) {
			t.Errorf("length %d: column=%d embedded=%d len(payload)=%d: want one number", target, column, embedded, len(encoded))
		}
	}
}
