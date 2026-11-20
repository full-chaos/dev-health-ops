//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The post_sync fan-out on the transport-fence rig (transport_fence_integration_test.go): a pgx tracer hook runs the
// competing write on a second pool at the exact end of a pass statement, so no case orders by sleeping.

const (
	postSyncFenceOrg         = "20000000-0000-4000-8000-000000000001"
	postSyncFenceRun         = "20000000-0000-4000-8000-000000000002"
	postSyncFenceOutbox      = "20000000-0000-4000-8000-000000000003"
	postSyncFenceIntegration = "20000000-0000-4000-8000-000000000004"
	postSyncFenceRepository  = "20000000-0000-4000-8000-000000000005"
)

// postSyncPassOptions wires the optional parts of the fan-out one case reads.
type postSyncPassOptions struct {
	logs     *lockedLogBuffer
	observer jobruntime.PostSyncFanoutObserver
	touched  *countingTouchedDaysStore
}

func postSyncFenceKind(options postSyncPassOptions) fenceKind {
	return fenceKind{
		name: "post_sync", kind: outboxKindPostSync, org: postSyncFenceOrg, run: postSyncFenceRun, outbox: postSyncFenceOutbox,
		generation: postSyncRouteGeneration,
		seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			createPostSyncTables(t, ctx, pool)
			seedPostSync(t, ctx, pool, postSyncFenceOrg, postSyncFenceRun, postSyncFenceOutbox, postSyncFenceIntegration, postSyncFenceRepository)
		},
		pass: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
			var logger *synclog.Logger
			if options.logs != nil {
				logger = synclog.New(slog.New(slog.NewTextHandler(options.logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			}
			service, err := NewNativePostSyncService(pool, markerDaily{}, markerRemaining{}, markerWorkGraph{}, markerTeam{}, markerTeamRepoOwnership{}, logger)
			if err != nil {
				t.Fatal(err)
			}
			service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
			if options.observer != nil {
				service.SetFanoutObserver(options.observer)
			}
			if options.touched != nil {
				if err := service.SetTouchedDays(options.touched, noopTouchedDayWriter{}); err != nil {
					t.Fatal(err)
				}
			}
			return service.Fanout(ctx, PostSyncArgs{TransportArgs: TransportArgs{
				Version: ContractVersionV1, OrgID: postSyncFenceOrg, RunID: postSyncFenceRun,
				DispatchOutbox: postSyncFenceOutbox, RouteGeneration: postSyncRouteGeneration, DeliveryAttempt: attempt,
			}})
		},
		wrote: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) bool {
			var markers int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM post_sync_markers WHERE sync_run_id = $1`, postSyncFenceRun).Scan(&markers); err != nil {
				t.Fatal(err)
			}
			return markers > 0
		},
	}
}

// busyPassBound bounds a pass that meets a held row. It never orders anything: a NOWAIT check returns long before it.
const busyPassBound = 30 * time.Second

// countingTouchedDaysStore counts every call into the touched-day record (ClickHouse in production).
type countingTouchedDaysStore struct{ calls int }

func (store *countingTouchedDaysStore) RecordTouched(context.Context, string, time.Time, []time.Time) (uint64, error) {
	store.calls++
	return 0, nil
}

func (store *countingTouchedDaysStore) RecordTouchedGit(context.Context, string, time.Time) (uint64, error) {
	store.calls++
	return 0, nil
}

func (store *countingTouchedDaysStore) RecordTouchedTests(context.Context, string, time.Time) (uint64, error) {
	store.calls++
	return 0, nil
}

func (store *countingTouchedDaysStore) PendingDays(context.Context, string, int) (TouchedDaysPending, error) {
	store.calls++
	return TouchedDaysPending{}, nil
}

func (store *countingTouchedDaysStore) PendingRepositories(context.Context, string, []time.Time, int) (map[string][]string, error) {
	store.calls++
	return map[string][]string{}, nil
}

func (store *countingTouchedDaysStore) MarkDispatched(context.Context, string, time.Time, []time.Time, []TouchedDayKey) error {
	store.calls++
	return nil
}

type noopTouchedDayWriter struct{}

func (noopTouchedDayWriter) FullOrganizationDays(PostSyncPlan) []time.Time { return nil }
func (noopTouchedDayWriter) RepositoryLimit() int                          { return 10 }
func (noopTouchedDayWriter) StartTouchedDayTx(context.Context, pgx.Tx, PostSyncPlan, time.Time, []string) (bool, error) {
	return true, nil
}
func (noopTouchedDayWriter) StampTakeTx(context.Context, pgx.Tx, PostSyncPlan, time.Time) error {
	return nil
}

// holdOutboxRow takes the post_sync row FOR UPDATE on the second pool and keeps it to the end of the case.
func holdOutboxRow(t *testing.T, ctx context.Context, rig *fenceRig) {
	tx, err := rig.other.Begin(ctx)
	if err != nil {
		t.Errorf("holder begin: %v", err)
		return
	}
	rig.release = append(rig.release, func() { _ = tx.Rollback(context.Background()) })
	var id string
	if err := tx.QueryRow(ctx, `SELECT id::text FROM sync_dispatch_outbox WHERE id = $1 FOR UPDATE`, rig.kind.outbox).Scan(&id); err != nil {
		t.Errorf("holder lock: %v", err)
	}
}

// TestPostSyncFence: a superseded post_sync delivery commits nothing; the current delivery commits, also when a
// same-attempt wake re-armed its row after the guard (F1: the wake stays armed).
func TestPostSyncFence(t *testing.T) {
	kind := postSyncFenceKind(postSyncPassOptions{})
	t.Run("current_first_attempt", func(t *testing.T) {
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			if err := kind.pass(t, ctx, rig.pool, 1); err != nil || !kind.wrote(t, ctx, rig.other) {
				t.Fatalf("current delivery: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, rig.other))
			}
		})
	})
	t.Run("current_redelivery", func(t *testing.T) {
		withFenceRig(t, kind, 2, func(ctx context.Context, rig *fenceRig) {
			if err := kind.pass(t, ctx, rig.pool, 2); err != nil || !kind.wrote(t, ctx, rig.other) {
				t.Fatalf("current redelivery: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, rig.other))
			}
		})
	})
	t.Run("redelivery_committed_before_guard", func(t *testing.T) {
		withFenceRig(t, kind, 2, func(ctx context.Context, rig *fenceRig) {
			if err := kind.pass(t, ctx, rig.pool, 1); err != nil || kind.wrote(t, ctx, rig.other) {
				t.Fatalf("delivery attempt 1 on a row at attempt 2: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
			}
		})
	})
	t.Run("redelivery_committed_after_guard", func(t *testing.T) {
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			hook := rig.on(isStartGuard, func(ctx context.Context) {
				if _, err := rig.other.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = attempts + 1 WHERE id = $1`, kind.outbox); err != nil {
					t.Errorf("redelivery: %v", err)
				}
			})
			err := kind.pass(t, ctx, rig.pool, 1)
			if !hook.fired {
				t.Fatal("the start guard never ran")
			}
			if err != nil || kind.wrote(t, ctx, rig.other) {
				t.Fatalf("redelivered after the guard: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
			}
		})
	})
	t.Run("rearmed_after_guard", func(t *testing.T) {
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			hook := rig.on(isStartGuard, func(ctx context.Context) {
				if err := rig.rearm(ctx, rig.other); err != nil {
					t.Errorf("wake: %v", err)
				}
			})
			err := kind.pass(t, ctx, rig.pool, 1)
			if !hook.fired {
				t.Fatal("the start guard never ran")
			}
			if err != nil || !kind.wrote(t, ctx, rig.other) {
				t.Fatalf("re-armed after the guard: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, rig.other))
			}
			if status, attempts := rig.outboxState(t, ctx); status != "pending" || attempts != 1 {
				t.Fatalf("outbox status=%q attempts=%d after the wake, want pending/1 (the wake must survive)", status, attempts)
			}
		})
	})
	t.Run("rearmed_then_claimed_after_guard", func(t *testing.T) {
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			hook := rig.on(isStartGuard, func(ctx context.Context) {
				if err := rig.rearm(ctx, rig.other); err != nil {
					t.Errorf("wake: %v", err)
					return
				}
				if _, err := rig.other.Exec(ctx, `
UPDATE sync_dispatch_outbox
SET claim_token = gen_random_uuid()::text, claim_expires_at = now() + interval '1 minute',
    claim_transport = 'river', claim_route_generation = $2, attempts = attempts + 1, updated_at = now()
WHERE id = $1`, kind.outbox, kind.generation); err != nil {
					t.Errorf("claim: %v", err)
				}
			})
			err := kind.pass(t, ctx, rig.pool, 1)
			if !hook.fired {
				t.Fatal("the start guard never ran")
			}
			if err != nil || kind.wrote(t, ctx, rig.other) {
				t.Fatalf("re-armed and claimed after the guard: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
			}
		})
	})
	t.Run("rearmed_and_route_moved_after_guard", func(t *testing.T) {
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			hook := rig.on(isStartGuard, func(ctx context.Context) {
				if err := rig.rearm(ctx, rig.other); err != nil {
					t.Errorf("wake: %v", err)
					return
				}
				paused := pgseed.SyncTransportRoute(ctx, t, rig.other, kind.kind, "river", kind.generation+1, true, "celery")
				resumed := pgseed.SyncTransportRoute(ctx, t, rig.other, kind.kind, "river", paused+1, false, "celery")
				if paused <= kind.generation || resumed <= paused {
					t.Errorf("route generations paused=%d resumed=%d, want > %d and increasing", paused, resumed, kind.generation)
				}
			})
			err := kind.pass(t, ctx, rig.pool, 1)
			if !hook.fired {
				t.Fatal("the start guard never ran")
			}
			if err != nil || kind.wrote(t, ctx, rig.other) {
				t.Fatalf("re-armed and route moved after the guard: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
			}
		})
	})
}

// TestPostSyncFenceHoldsTheRowFromCheckToCommit: once the commit-time check passed, a wake waits on the row lock
// (lock_timeout makes the wait observable) and lands only after the pass committed.
func TestPostSyncFenceHoldsTheRowFromCheckToCommit(t *testing.T) {
	kind := postSyncFenceKind(postSyncPassOptions{})
	withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
		var held error
		hook := rig.on(isCommitCheck, func(ctx context.Context) {
			tx, err := rig.other.Begin(ctx)
			if err != nil {
				held = err
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '100ms'`); err != nil {
				held = err
				return
			}
			held = rig.rearm(ctx, tx)
		})
		err := kind.pass(t, ctx, rig.pool, 1)
		if !hook.fired {
			t.Fatal("the commit-time check never ran")
		}
		var pgErr *pgconn.PgError
		if !errors.As(held, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("wake between check and commit: %v, want SQLSTATE 55P03 (row held)", held)
		}
		if err != nil || !kind.wrote(t, ctx, rig.other) {
			t.Fatalf("current delivery: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, rig.other))
		}
		if err := rig.rearm(ctx, rig.other); err != nil {
			t.Fatalf("wake after the commit: %v", err)
		}
		if status, _ := rig.outboxState(t, ctx); status != "pending" {
			t.Fatalf("outbox status=%q after the post-commit wake, want pending", status)
		}
	})
}

// TestPostSyncFenceRetriesWhenTheRowIsBusy: the commit-time check never waits for the row. A row held elsewhere makes
// the pass return ErrPostSyncUnavailable (River retries the same delivery) with nothing committed.
func TestPostSyncFenceRetriesWhenTheRowIsBusy(t *testing.T) {
	kind := postSyncFenceKind(postSyncPassOptions{})
	withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
		hook := rig.on(isStartGuard, func(ctx context.Context) { holdOutboxRow(t, ctx, rig) })
		err := kind.pass(t, ctx, rig.pool, 1)
		if !hook.fired {
			t.Fatal("the start guard never ran")
		}
		if !errors.Is(err, ErrPostSyncUnavailable) {
			t.Fatalf("pass with the outbox row held elsewhere: err=%v, want %v", err, ErrPostSyncUnavailable)
		}
		if kind.wrote(t, ctx, rig.other) {
			t.Fatal("pass committed while the outbox row was held elsewhere")
		}
	})
}

// TestPostSyncFenceCommitRefusalIsNotAFanoutError: a delivery superseded during the pass is a no-op that says so in
// one post_sync_fanout.delivery_not_current line; the fan-out outcome counter is not fed.
func TestPostSyncFenceCommitRefusalIsNotAFanoutError(t *testing.T) {
	logs := &lockedLogBuffer{}
	observer := &recordingFanoutObserver{}
	kind := postSyncFenceKind(postSyncPassOptions{logs: logs, observer: observer})
	withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
		hook := rig.on(isStartGuard, func(ctx context.Context) {
			if _, err := rig.other.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = attempts + 1 WHERE id = $1`, kind.outbox); err != nil {
				t.Errorf("redelivery: %v", err)
			}
		})
		err := kind.pass(t, ctx, rig.pool, 1)
		if !hook.fired {
			t.Fatal("the start guard never ran")
		}
		if err != nil || kind.wrote(t, ctx, rig.other) {
			t.Fatalf("superseded during the pass: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
		}
		if len(observer.outcomes) != 0 {
			t.Fatalf("fan-out outcomes %v on a refused delivery, want none", observer.outcomes)
		}
		lines := 0
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=post_sync_fanout.delivery_not_current") {
				lines++
				if !strings.Contains(line, "reason=delivery_attempt_mismatch ") || !strings.Contains(line, "sync_run_id="+postSyncFenceRun) {
					t.Errorf("refusal line without its reason or run: %s", line)
				}
			}
		}
		if lines != 1 {
			t.Fatalf("%d post_sync_fanout.delivery_not_current lines, want 1:\n%s", lines, strings.Join(logs.lines(), "\n"))
		}
	})
}

// TestPostSyncFenceBusyIsOneFailedAttempt: a held row is one failed attempt: one error outcome and one
// post_sync_fanout.failed line naming the commit_check stage and SQLSTATE 55P03.
func TestPostSyncFenceBusyIsOneFailedAttempt(t *testing.T) {
	logs := &lockedLogBuffer{}
	observer := &recordingFanoutObserver{}
	kind := postSyncFenceKind(postSyncPassOptions{logs: logs, observer: observer})
	withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
		rig.on(isStartGuard, func(ctx context.Context) { holdOutboxRow(t, ctx, rig) })
		// The bound only detects a wait: with NOWAIT the check fails at once with 55P03; a waiting check would end in
		// a cancel (57014) when the bound expires, so the line below would not name 55P03.
		passCtx, cancel := context.WithTimeout(ctx, busyPassBound)
		defer cancel()
		if err := kind.pass(t, passCtx, rig.pool, 1); !errors.Is(err, ErrPostSyncUnavailable) {
			t.Fatalf("pass with the outbox row held elsewhere: err=%v, want %v", err, ErrPostSyncUnavailable)
		}
		if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeError {
			t.Fatalf("fan-out outcomes %v, want exactly [error]", observer.outcomes)
		}
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=post_sync_fanout.failed") && strings.Contains(line, "reason=commit_check ") &&
				strings.Contains(line, "error.code=55P03") {
				return
			}
		}
		t.Fatalf("no post_sync_fanout.failed reason=commit_check error.code=55P03 line:\n%s", strings.Join(logs.lines(), "\n"))
	})
}

// TestPostSyncTouchedTake: the touched-day take, which writes the ClickHouse record before the pass transaction, is
// fenced by the same guard: a superseded delivery never reaches the record.
func TestPostSyncTouchedTake(t *testing.T) {
	t.Run("current_delivery_records", func(t *testing.T) {
		store := &countingTouchedDaysStore{}
		kind := postSyncFenceKind(postSyncPassOptions{touched: store})
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			if err := kind.pass(t, ctx, rig.pool, 1); err != nil || store.calls == 0 || !kind.wrote(t, ctx, rig.other) {
				t.Fatalf("current delivery: err=%v store calls=%d wrote=%t, want nil/>0/true", err, store.calls, kind.wrote(t, ctx, rig.other))
			}
		})
	})
	t.Run("stale_delivery_records_nothing", func(t *testing.T) {
		store := &countingTouchedDaysStore{}
		kind := postSyncFenceKind(postSyncPassOptions{touched: store})
		withFenceRig(t, kind, 2, func(ctx context.Context, rig *fenceRig) {
			if err := kind.pass(t, ctx, rig.pool, 1); err != nil || store.calls != 0 || kind.wrote(t, ctx, rig.other) {
				t.Fatalf("delivery attempt 1 on a row at attempt 2: err=%v store calls=%d wrote=%t, want nil/0/false", err, store.calls, kind.wrote(t, ctx, rig.other))
			}
		})
	})
}

// noopPostSyncWriter stages nothing: the domain-role cell proves the fan-out's own statements, not a writer's.
type noopPostSyncWriter struct{}

func (noopPostSyncWriter) StartRunTx(context.Context, pgx.Tx, PostSyncPlan, string) (string, string, error) {
	return "", "", nil
}

type noopPostSyncRemaining struct{}

func (noopPostSyncRemaining) StartRunTx(context.Context, pgx.Tx, string, PostSyncPlan, string) (string, error) {
	return "", nil
}

type noopPostSyncWorkGraph struct{}

func (noopPostSyncWorkGraph) StartRequestTx(context.Context, pgx.Tx, string, PostSyncPlan, string) (string, error) {
	return "", nil
}

type noopPostSyncPublish struct{}

func (noopPostSyncPublish) PublishTx(context.Context, pgx.Tx, PostSyncPlan) error { return nil }

// TestPostSyncFenceRunsAsTheDomainRole: the production pool of the fan-out is the domain role
// (workerservice/sync_dispatch.go NewNativePostSyncService(pools.Domain)). The guard and the commit-time row lock run
// as that role with no privilege denial; a row held elsewhere is a lock conflict (55P03), never a refusal (42501).
func TestPostSyncFenceRunsAsTheDomainRole(t *testing.T) {
	const (
		org, run, outbox = postSyncFenceOrg, postSyncFenceRun, postSyncFenceOutbox
		unit             = "20000000-0000-4000-8000-000000000006"
	)
	setup := func(t *testing.T) (context.Context, *pgxpool.Pool, *NativePostSyncService, *denialRecorder) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		t.Cleanup(cancel)
		admin, domain, denials, _ := startDomainRoleHarness(t, ctx, func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			createFinalizeTablesProbe(t, ctx, pool)
			if _, err := pool.Exec(ctx, `ALTER TABLE sync_runs ADD COLUMN created_at timestamptz NOT NULL DEFAULT now()`); err != nil {
				t.Fatal(err)
			}
		})
		day := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
		for _, seed := range []struct {
			statement string
			args      []any
		}{
			{`INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
			  VALUES ('post_sync','river',$1,false,'celery')`, []any{postSyncRouteGeneration}},
			{`INSERT INTO sync_dispatch_outbox
			    (id,sync_run_id,org_id,kind,status,available_at,attempts,dispatched_transport,dispatched_route_generation,created_at,updated_at)
			  VALUES ($1,$2,$3,'post_sync','dispatched',now(),1,'river',$4,now(),now())`, []any{outbox, run, org, postSyncRouteGeneration}},
			{`INSERT INTO integrations (id, provider) VALUES ($1,'github')`, []any{postSyncFenceIntegration}},
			{`INSERT INTO sync_runs (id,org_id,integration_id,status,total_units,completed_units,failed_units,started_at)
			  VALUES ($1,$2,$3,'success',1,1,0,$4)`, []any{run, org, postSyncFenceIntegration, day}},
			{`INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,since_at,before_at)
			  VALUES ($1,$2,$3,'github','commits',$4,'success',$5,$5)`, []any{unit, org, run, postSyncFenceRepository, day}},
		} {
			if _, err := admin.Exec(ctx, seed.statement, seed.args...); err != nil {
				t.Fatalf("seed %s: %v", collapseStatement(seed.statement), err)
			}
		}
		service, err := NewNativePostSyncService(domain, noopPostSyncWriter{}, noopPostSyncRemaining{}, noopPostSyncWorkGraph{},
			noopPostSyncPublish{}, noopPostSyncPublish{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		service.now = func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) }
		return ctx, admin, service, denials
	}
	args := PostSyncArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: org, RunID: run, DispatchOutbox: outbox,
		RouteGeneration: postSyncRouteGeneration, DeliveryAttempt: 1,
	}}
	t.Run("commits", func(t *testing.T) {
		ctx, _, service, denials := setup(t)
		err := service.Fanout(ctx, args)
		denials.assertNone(t, "NativePostSyncService fenced fan-out")
		if err != nil {
			t.Fatalf("Fanout as the domain role: %v", err)
		}
	})
	t.Run("row_busy_is_55P03", func(t *testing.T) {
		ctx, admin, service, denials := setup(t)
		holder, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback(context.Background()) }()
		var id string
		if err := holder.QueryRow(ctx, `SELECT id::text FROM sync_dispatch_outbox WHERE id = $1 FOR UPDATE`, outbox).Scan(&id); err != nil {
			t.Fatal(err)
		}
		passCtx, cancel := context.WithTimeout(ctx, busyPassBound)
		defer cancel()
		err = service.Fanout(passCtx, args)
		denials.assertNone(t, "NativePostSyncService fenced fan-out, row held")
		var pgErr *pgconn.PgError
		if !errors.Is(err, ErrPostSyncUnavailable) || !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("Fanout as the domain role with the row held: err=%v, want ErrPostSyncUnavailable over SQLSTATE 55P03", err)
		}
	})
}

// TestPostSyncFenceRunMissing: the commit with no plan (the run is not the delivery's, so loadPostSyncPlan finds no
// row) is fenced too. A delivery superseded during that pass is refused with its line and no outcome; the current
// delivery commits and counts no_repositories.
func TestPostSyncFenceRunMissing(t *testing.T) {
	moveRun := func(t *testing.T, ctx context.Context, rig *fenceRig) {
		t.Helper()
		if _, err := rig.other.Exec(ctx, `UPDATE sync_runs SET org_id = $2 WHERE id = $1`, postSyncFenceRun, "20000000-0000-4000-8000-0000000000ff"); err != nil {
			t.Fatalf("move the run to another organization: %v", err)
		}
	}
	refusals := func(logs *lockedLogBuffer) int {
		count := 0
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=post_sync_fanout.delivery_not_current") && strings.Contains(line, "reason=delivery_attempt_mismatch ") {
				count++
			}
		}
		return count
	}
	t.Run("current_commits_no_repositories", func(t *testing.T) {
		logs := &lockedLogBuffer{}
		observer := &recordingFanoutObserver{}
		kind := postSyncFenceKind(postSyncPassOptions{logs: logs, observer: observer})
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			moveRun(t, ctx, rig)
			hook := rig.on(isCommitCheck, func(context.Context) {})
			err := kind.pass(t, ctx, rig.pool, 1)
			if err != nil || len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeNoRepositories || refusals(logs) != 0 {
				t.Fatalf("current delivery, run missing: err=%v outcomes=%v refusals=%d, want nil/[no_repositories]/0", err, observer.outcomes, refusals(logs))
			}
			if !hook.fired {
				t.Fatal("the commit-time check never ran on the run-missing commit")
			}
		})
	})
	t.Run("superseded_after_guard_is_refused", func(t *testing.T) {
		logs := &lockedLogBuffer{}
		observer := &recordingFanoutObserver{}
		kind := postSyncFenceKind(postSyncPassOptions{logs: logs, observer: observer})
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			moveRun(t, ctx, rig)
			hook := rig.on(isStartGuard, func(ctx context.Context) {
				if _, err := rig.other.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = attempts + 1 WHERE id = $1`, kind.outbox); err != nil {
					t.Errorf("redelivery: %v", err)
				}
			})
			err := kind.pass(t, ctx, rig.pool, 1)
			if !hook.fired {
				t.Fatal("the start guard never ran")
			}
			if err != nil || len(observer.outcomes) != 0 || refusals(logs) != 1 {
				t.Fatalf("superseded delivery, run missing: err=%v outcomes=%v refusals=%d, want nil/[]/1", err, observer.outcomes, refusals(logs))
			}
		})
	})
}
