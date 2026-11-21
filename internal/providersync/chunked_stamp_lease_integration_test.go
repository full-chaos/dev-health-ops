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

// The lease fence of PrepareChunkGroup reads leaseNow, not the split's stamp instant: a lease that expired while the split ran refuses the
// group (stampAt still inside the lease); a stampAt past the lease with a leaseNow inside it prepares, and the stored ledger and the
// checkpoint carry stampAt (CHAOS-14411).
func TestChunkStampPrepareFencesTheLeaseWithLeaseNow(t *testing.T) {
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
	claimedAt := time.Now().UTC().Truncate(time.Microsecond)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: claimedAt,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	emissions := stampScript(t, []stampShape{{cases: 3, filled: true}}, false, 0, 0)
	inside, expired := claimedAt.Add(time.Second), claimedAt.Add(2*time.Minute)
	split := func(stampAt time.Time) []PreparedProviderChunk {
		batch, stamps := stampsOf(t, claim, emissions[0], 0, stampAt)
		chunks, err := splitStampedEmission(batch, stamps, DefaultChunkPolicy())
		if err != nil {
			t.Fatal(err)
		}
		return chunks
	}
	if _, err := repository.PrepareChunkGroup(ctx, claim, split(inside), inside, expired); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("lease expired during the split: err=%v, want ErrLeaseLost", err)
	}
	stampAt := expired
	prepared, err := repository.PrepareChunkGroup(ctx, claim, split(stampAt), stampAt, inside)
	if err != nil {
		t.Fatalf("leaseNow inside the lease: %v", err)
	}
	stored, err := repository.LoadPreparedChunk(ctx, claim, prepared[0].Ordinal, inside)
	if err != nil || !stored.Ledger.CreatedAt.Equal(stampAt) {
		t.Fatalf("stored ledger created_at %s err=%v, want stampAt %s", stored.Ledger.CreatedAt, err, stampAt)
	}
	checkpoint, err := repository.LoadChunkCheckpoint(ctx, claim, inside)
	if err != nil || !checkpoint.NormalizedAt.Equal(stampAt) {
		t.Fatalf("checkpoint normalized_at %s err=%v, want stampAt %s", checkpoint.NormalizedAt, err, stampAt)
	}
}
