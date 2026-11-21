//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// finalWatermarkPostgresCrash fails MarkChunkCommitted once: the old binary prepared its final chunk in PostgreSQL and
// died before it marked it committed.
type finalWatermarkPostgresCrash struct {
	*PostgresRepository
	failMark bool
}

func (store *finalWatermarkPostgresCrash) MarkChunkCommitted(ctx context.Context, claim Claim, ordinal int, digest string, now time.Time) error {
	if store.failMark {
		store.failMark = false
		return errFinalWatermarkCrash
	}
	return store.PostgresRepository.MarkChunkCommitted(ctx, claim, ordinal, digest, now)
}

// CHAOS-14422 on the real ledger: a GitLab cicd final chunk an older binary prepared in PostgreSQL (done cursor with no
// window start, watermark BeforeAt) and did not publish. The recovery reads it back from PostgreSQL, asks the route
// nothing, publishes no watermark, and Complete writes no sync_watermarks row for the unit.
func TestChunkedPostgresRecoveredGitLabFinalWritesNoOldWatermark(t *testing.T) {
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
	capability, ok := Capability("gitlab", "cicd")
	if !ok {
		t.Fatal("capability")
	}
	flags, err := json.Marshal(capability.ProcessorFlags)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE public.integration_sources SET external_id = '123' WHERE id = $1`, firstSourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET provider = 'gitlab', dataset_key = 'cicd', cost_class = $2, processor_flags = $3::jsonb,
    since_at = '2026-07-01T00:00:00Z', before_at = '2026-07-31T23:59:59Z'
WHERE id = $1`, firstUnitID, string(capability.CostClass), string(flags)); err != nil {
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
	if claim.BeforeAt == nil {
		t.Fatal("premise: the unit has no BeforeAt")
	}
	descriptor, ok := Descriptor("gitlab", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatal("descriptor")
	}
	session := func() *LeaseSession {
		return &LeaseSession{Repository: repository, Claim: claim, LeaseDuration: time.Minute, Deadline: time.Now().Add(10 * time.Minute)}
	}
	executor := func(handler CompleteRouteHandler, store EffectLedger) CompleteRouteExecutor {
		built := finalWatermarkExecutor(time.Now(), claim, handler, store)
		built.Now, built.Committer.Now = nil, time.Now
		return built
	}

	crash := &finalWatermarkPostgresCrash{PostgresRepository: repository, failMark: true}
	if _, err := executor(oldGitLabFinal(claim), crash).Execute(ctx, session(), descriptor); !errors.Is(err, errFinalWatermarkCrash) {
		t.Fatalf("old attempt err=%v, want the simulated crash", err)
	}
	checkpoint, err := repository.LoadChunkCheckpoint(ctx, claim, time.Now())
	if err != nil || checkpoint.PreparedChunks != 1 || checkpoint.NextOrdinal != 0 || checkpoint.InventoryComplete {
		t.Fatalf("premise: checkpoint=%+v err=%v", checkpoint, err)
	}
	final, err := repository.LoadPreparedChunk(ctx, claim, 0, time.Now())
	if err != nil || !final.InventoryComplete || final.Watermark == nil || !final.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("premise: stored final=%+v err=%v", final, err)
	}

	calls := 0
	startedAt := time.Now()
	result, err := executor(countingChunkedRoute{handler: GitLabTestsRouteHandler{}, calls: &calls}, repository).
		Execute(ctx, session(), descriptor)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if calls != 0 {
		t.Fatalf("recovery asked the route %d time(s)", calls)
	}
	if result.Watermark != nil {
		t.Fatalf("recovery published the old watermark %s", finalWatermarkText(result.Watermark))
	}
	if err := repository.Complete(ctx, claim, result.Result, result.Watermark, startedAt, time.Now()); err != nil {
		t.Fatalf("complete: %v", err)
	}
	var watermarks int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_watermarks WHERE org_id = $1 AND source_id = $2`,
		claim.OrgID, claim.SourceExternalID).Scan(&watermarks); err != nil {
		t.Fatal(err)
	}
	if watermarks != 0 {
		t.Fatalf("Complete wrote %d sync_watermarks row(s) for the recovered unit, want 0", watermarks)
	}
}
