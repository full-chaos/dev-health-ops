//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func putRateLimitTestCheckpoint(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, generation string, prepared, next int, now time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_unit_chunk_checkpoints
(org_id, sync_run_unit_id, generation, provider, dataset_key, route_version, normalized_at,
 next_ordinal, prepared_chunks, owner, lease_expires_at, created_at, updated_at)
VALUES ('org-acme', $1::uuid, $2, 'github', 'tests', 'provider-chunk.v1', $5, $4, $3, 'owner', $5, $5, $5)
ON CONFLICT (org_id, sync_run_unit_id, generation)
DO UPDATE SET next_ordinal = EXCLUDED.next_ordinal, prepared_chunks = EXCLUDED.prepared_chunks`,
		firstUnitID, generation, prepared, next, now); err != nil {
		t.Fatal(err)
	}
}

type storedRateLimitEpisode struct {
	deferrals       int
	firstSeenAt     *time.Time
	resultDeferrals *int
	mark            []byte
}

func readStoredRateLimitEpisode(t *testing.T, ctx context.Context, pool *pgxpool.Pool) storedRateLimitEpisode {
	t.Helper()
	var stored storedRateLimitEpisode
	if err := pool.QueryRow(ctx, `
SELECT rate_limit_deferrals, rate_limit_first_seen_at,
       (result::jsonb ->> 'rate_limit_deferrals')::int,
       result::jsonb -> 'rate_limit_progress_mark'
FROM public.sync_run_units WHERE id = $1`, firstUnitID).Scan(
		&stored.deferrals, &stored.firstSeenAt, &stored.resultDeferrals, &stored.mark,
	); err != nil {
		t.Fatal(err)
	}
	return stored
}

// TestRateLimitEpisodeRestartsOnlyOnCheckpointProgress (CHAOS-14252) runs the
// episode read and the deferral stamp against Postgres: the live checkpoint is
// read only from the unit's own generation (LEFT JOIN: none = no checkpoint),
// a restart resets both counters, and a step without a mark keeps the stored one.
func TestRateLimitEpisodeRestartsOnlyOnCheckpointProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
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
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
	seededFirstSeen := now.Add(-6 * time.Hour)
	claim := claimForBudgetEpisode(t, ctx, repository, now)

	episode, err := repository.RateLimitEpisode(ctx, claim)
	if err != nil {
		t.Fatalf("a unit without a checkpoint must still read its episode: %v", err)
	}
	if episode.Deferrals != 3 || episode.Checkpoint != nil || episode.Mark != nil || episode.Progressed() {
		t.Fatalf("no checkpoint: episode=%+v", episode)
	}

	putRateLimitTestCheckpoint(t, ctx, pool, "sync-unit:other-generation", 9, 9, now)
	if episode, err = repository.RateLimitEpisode(ctx, claim); err != nil || episode.Checkpoint != nil {
		t.Fatalf("another generation's checkpoint was read: %+v err=%v", episode, err)
	}

	// A row deferred before the mark existed: the first deferral counts on
	// and writes the mark.
	putRateLimitTestCheckpoint(t, ctx, pool, claim.GenerationKey(), 4, 4, now)
	if episode, err = repository.RateLimitEpisode(ctx, claim); err != nil ||
		episode.Checkpoint == nil || *episode.Checkpoint != (RateLimitProgressMark{PreparedChunks: 4, NextOrdinal: 4}) ||
		episode.Mark != nil || episode.Progressed() {
		t.Fatalf("pre-deploy row: episode=%+v err=%v", episode, err)
	}
	if err := repository.DeferForRateLimit(ctx, claim, now.Add(time.Minute), now, RateLimitObservation{},
		RateLimitEpisodeStep{Mark: episode.Checkpoint}); err != nil {
		t.Fatal(err)
	}
	stored := readStoredRateLimitEpisode(t, ctx, pool)
	if stored.deferrals != 4 || stored.resultDeferrals == nil || *stored.resultDeferrals != 4 ||
		stored.firstSeenAt == nil || !stored.firstSeenAt.Equal(seededFirstSeen) ||
		string(stored.mark) != `{"next_ordinal": 4, "prepared_chunks": 4}` {
		t.Fatalf("pre-deploy deferral stored %+v mark=%s", stored, stored.mark)
	}

	// Progress since the mark: the read reports it, and a restart starts a
	// fresh episode at this deferral.
	later := now.Add(time.Hour)
	claim = claimForBudgetEpisode(t, ctx, repository, later)
	putRateLimitTestCheckpoint(t, ctx, pool, claim.GenerationKey(), 6, 5, later)
	if episode, err = repository.RateLimitEpisode(ctx, claim); err != nil || !episode.Progressed() ||
		*episode.Mark != (RateLimitProgressMark{PreparedChunks: 4, NextOrdinal: 4}) {
		t.Fatalf("progress not read: episode=%+v err=%v", episode, err)
	}
	if err := repository.DeferForRateLimit(ctx, claim, later.Add(time.Minute), later, RateLimitObservation{},
		RateLimitEpisodeStep{Restart: true, Mark: episode.Checkpoint}); err != nil {
		t.Fatal(err)
	}
	stored = readStoredRateLimitEpisode(t, ctx, pool)
	if stored.deferrals != 1 || stored.resultDeferrals == nil || *stored.resultDeferrals != 1 ||
		stored.firstSeenAt == nil || !stored.firstSeenAt.Equal(later) ||
		string(stored.mark) != `{"next_ordinal": 5, "prepared_chunks": 6}` {
		t.Fatalf("restart stored %+v mark=%s", stored, stored.mark)
	}

	// No progress: the episode counts on from the restart.
	evenLater := later.Add(time.Hour)
	claim = claimForBudgetEpisode(t, ctx, repository, evenLater)
	if episode, err = repository.RateLimitEpisode(ctx, claim); err != nil || episode.Progressed() {
		t.Fatalf("an unmoved checkpoint read as progress: episode=%+v err=%v", episode, err)
	}
	if err := repository.DeferForRateLimit(ctx, claim, evenLater.Add(time.Minute), evenLater, RateLimitObservation{},
		RateLimitEpisodeStep{Mark: episode.Checkpoint}); err != nil {
		t.Fatal(err)
	}
	stored = readStoredRateLimitEpisode(t, ctx, pool)
	if stored.deferrals != 2 || *stored.resultDeferrals != 2 || !stored.firstSeenAt.Equal(later) {
		t.Fatalf("no-progress deferral stored %+v", stored)
	}

	// A step without a mark keeps the stored mark.
	last := evenLater.Add(time.Hour)
	claim = claimForBudgetEpisode(t, ctx, repository, last)
	if err := repository.DeferForRateLimit(ctx, claim, last.Add(time.Minute), last, RateLimitObservation{},
		RateLimitEpisodeStep{}); err != nil {
		t.Fatal(err)
	}
	stored = readStoredRateLimitEpisode(t, ctx, pool)
	if stored.deferrals != 3 || string(stored.mark) != `{"next_ordinal": 5, "prepared_chunks": 6}` {
		t.Fatalf("markless deferral stored %+v mark=%s", stored, stored.mark)
	}

	// A malformed stored mark reads as no mark: no restart.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET result = (result::jsonb || '{"rate_limit_progress_mark":"x"}'::jsonb)::json WHERE id = $1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	claim = claimForBudgetEpisode(t, ctx, repository, last.Add(time.Hour))
	if episode, err = repository.RateLimitEpisode(ctx, claim); err != nil || episode.Mark != nil || episode.Progressed() {
		t.Fatalf("malformed mark: episode=%+v err=%v", episode, err)
	}
}
