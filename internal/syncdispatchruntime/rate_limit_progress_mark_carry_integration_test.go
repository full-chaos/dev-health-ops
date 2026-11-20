//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/jackc/pgx/v5/pgxpool"
)

const progressMarkCarryUnit = "00000000-0000-4000-8000-0000000142a1"

func putProgressMarkCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, claim providersync.Claim, prepared, next int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_unit_chunk_checkpoints
(org_id, sync_run_unit_id, generation, provider, dataset_key, route_version, normalized_at,
 next_ordinal, prepared_chunks, owner, lease_expires_at, created_at, updated_at)
VALUES ($1, $2::uuid, $3, 'github', 'commits', 'provider-chunk.v1', now(), $5, $4, 'owner', now(), now(), now())
ON CONFLICT (org_id, sync_run_unit_id, generation)
DO UPDATE SET next_ordinal = EXCLUDED.next_ordinal, prepared_chunks = EXCLUDED.prepared_chunks`,
		claim.OrgID, claim.ID, claim.GenerationKey(), prepared, next); err != nil {
		t.Fatal(err)
	}
}

// staleAndRedispatch ages the deferred unit past the stale-dispatch cutoff,
// runs one dispatcher write on it, then stands in for the dispatcher's claim
// (retrying -> dispatching, which does not touch result) and leases it again.
func staleAndRedispatch(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	write func(now time.Time) bool,
) providersync.Claim {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET updated_at = now() - interval '2 hours' WHERE id = $1`, progressMarkCarryUnit); err != nil {
		t.Fatal(err)
	}
	if !write(pgNow()) {
		t.Fatal("the dispatcher write did not reach the stale dispatching unit")
	}
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET status = 'dispatching', available_at = NULL WHERE id = $1`, progressMarkCarryUnit); err != nil {
		t.Fatal(err)
	}
	return leaseUnitForCompletion(t, ctx, pool, progressMarkCarryUnit)
}

func storedProgressMark(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, int, *time.Time) {
	t.Helper()
	var mark *string
	var deferrals int
	var firstSeen *time.Time
	if err := pool.QueryRow(ctx, `
SELECT (result::jsonb -> 'rate_limit_progress_mark')::text, rate_limit_deferrals, rate_limit_first_seen_at
FROM public.sync_run_units WHERE id = $1`, progressMarkCarryUnit).Scan(&mark, &deferrals, &firstSeen); err != nil {
		t.Fatal(err)
	}
	if mark == nil {
		return "", deferrals, firstSeen
	}
	return *mark, deferrals, firstSeen
}

// TestDispatcherDeferralsCarryTheWorkerProgressMark (CHAOS-14252): a unit the
// worker deferred for a provider rate limit can sit past the stale-dispatch
// cutoff, and the dispatcher's cooldown and budget writes then rewrite its
// result. Both must carry rate_limit_progress_mark, so the next deferral after
// real checkpoint progress still restarts the worker's episode.
func TestDispatcherDeferralsCarryTheWorkerProgressMark(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(ctx context.Context, pool *pgxpool.Pool, now time.Time) (bool, error)
		// counterReset: the write clears the rate-limit pair, so the next
		// deferral starts a fresh episode whether or not it restarts.
		counterReset bool
	}{
		{
			name: "cooldown",
			write: func(ctx context.Context, pool *pgxpool.Pool, now time.Time) (bool, error) {
				tx, err := pool.Begin(ctx)
				if err != nil {
					return false, err
				}
				defer func() { _ = tx.Rollback(ctx) }()
				deferral := rateLimitDeferralPlan{notBefore: now.Add(30 * time.Second), attempts: 1, firstSeenAt: now}
				_, ok, err := applyCooldownDeferral(ctx, tx, nil, budgetUnit{id: progressMarkCarryUnit}, deferral, 0, now)
				if err != nil || !ok {
					return ok, err
				}
				return true, tx.Commit(ctx)
			},
		},
		{
			name: "budget",
			write: func(ctx context.Context, pool *pgxpool.Pool, now time.Time) (bool, error) {
				tx, err := pool.Begin(ctx)
				if err != nil {
					return false, err
				}
				defer func() { _ = tx.Rollback(ctx) }()
				ok, err := deferUnitForBudget(ctx, tx, budgetUnit{id: progressMarkCarryUnit}, now.Add(time.Minute), now, nil)
				if err != nil || !ok {
					return ok, err
				}
				return true, tx.Commit(ctx)
			},
			counterReset: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				seedDispatchRoute(t, ctx, pool)
				if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'dispatching',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now())`,
					progressMarkCarryUnit, discoveryTestOrg, discoveryTestRun, dispatchTestSource); err != nil {
					t.Fatal(err)
				}
				repository, err := providersync.NewPostgresRepository(pool)
				if err != nil {
					t.Fatal(err)
				}
				defer repository.Close()

				claim := leaseUnitForCompletion(t, ctx, pool, progressMarkCarryUnit)
				putProgressMarkCheckpoint(t, ctx, pool, claim, 4, 4)
				episode, err := repository.RateLimitEpisode(ctx, claim)
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC()
				if err := repository.DeferForRateLimit(ctx, claim, now.Add(time.Hour), now, providersync.RateLimitObservation{},
					providersync.RateLimitEpisodeStep{Mark: episode.Checkpoint}); err != nil {
					t.Fatal(err)
				}

				claim = staleAndRedispatch(t, ctx, pool, func(at time.Time) bool {
					ok, err := test.write(ctx, pool, at)
					if err != nil {
						t.Fatal(err)
					}
					return ok
				})
				mark, deferrals, firstSeen := storedProgressMark(t, ctx, pool)
				if mark != `{"next_ordinal": 4, "prepared_chunks": 4}` {
					t.Fatalf("the %s write dropped the worker's mark: stored %q", test.name, mark)
				}
				if test.counterReset && (deferrals != 0 || firstSeen != nil) {
					t.Fatalf("the %s write kept the rate-limit pair: deferrals=%d first_seen=%v", test.name, deferrals, firstSeen)
				}

				putProgressMarkCheckpoint(t, ctx, pool, claim, 6, 6)
				episode, err = repository.RateLimitEpisode(ctx, claim)
				if err != nil {
					t.Fatal(err)
				}
				if !episode.Progressed() {
					t.Fatalf("checkpoint progress after a %s write did not read as progress: %+v mark=%v", test.name, episode, episode.Mark)
				}
				later := time.Now().UTC()
				if err := repository.DeferForRateLimit(ctx, claim, later.Add(time.Hour), later, providersync.RateLimitObservation{},
					providersync.RateLimitEpisodeStep{Restart: true, Mark: episode.Checkpoint}); err != nil {
					t.Fatal(err)
				}
				mark, deferrals, firstSeen = storedProgressMark(t, ctx, pool)
				if deferrals != 1 || firstSeen == nil || mark != `{"next_ordinal": 6, "prepared_chunks": 6}` {
					t.Fatalf("restart after a %s write stored deferrals=%d first_seen=%v mark=%q", test.name, deferrals, firstSeen, mark)
				}
			})
		})
	}
}

// TestACounterResetWriteStartsAFreshEpisodeWithoutProgress: the budget write
// clears the rate-limit pair, so the next rate-limit deferral starts a fresh
// episode (1, now) even when the checkpoint did not move and no restart is sent.
func TestACounterResetWriteStartsAFreshEpisodeWithoutProgress(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'dispatching',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now())`,
			progressMarkCarryUnit, discoveryTestOrg, discoveryTestRun, dispatchTestSource); err != nil {
			t.Fatal(err)
		}
		repository, err := providersync.NewPostgresRepository(pool)
		if err != nil {
			t.Fatal(err)
		}
		defer repository.Close()
		claim := leaseUnitForCompletion(t, ctx, pool, progressMarkCarryUnit)
		putProgressMarkCheckpoint(t, ctx, pool, claim, 4, 4)
		if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units SET rate_limit_deferrals = 9, rate_limit_first_seen_at = now() - interval '90 minutes' WHERE id = $1`,
			progressMarkCarryUnit); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		if err := repository.DeferForRateLimit(ctx, claim, now.Add(time.Hour), now, providersync.RateLimitObservation{},
			providersync.RateLimitEpisodeStep{Mark: &providersync.RateLimitProgressMark{PreparedChunks: 4, NextOrdinal: 4}}); err != nil {
			t.Fatal(err)
		}
		claim = staleAndRedispatch(t, ctx, pool, func(at time.Time) bool {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			ok, err := deferUnitForBudget(ctx, tx, budgetUnit{id: progressMarkCarryUnit}, at.Add(time.Minute), at, nil)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			}
			return ok
		})
		episode, err := repository.RateLimitEpisode(ctx, claim)
		if err != nil {
			t.Fatal(err)
		}
		if episode.Progressed() || episode.Deferrals != 0 || episode.FirstSeenAt != nil {
			t.Fatalf("after a counter reset with no progress: %+v", episode)
		}
		later := time.Now().UTC()
		if err := repository.DeferForRateLimit(ctx, claim, later.Add(time.Hour), later, providersync.RateLimitObservation{},
			providersync.RateLimitEpisodeStep{Mark: episode.Checkpoint}); err != nil {
			t.Fatal(err)
		}
		_, deferrals, firstSeen := storedProgressMark(t, ctx, pool)
		if deferrals != 1 || firstSeen == nil || !firstSeen.After(now) {
			t.Fatalf("no fresh episode after the counter reset: deferrals=%d first_seen=%v", deferrals, firstSeen)
		}
	})
}
