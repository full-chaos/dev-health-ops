//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A pgx tracer on the service's own pool runs a competing writer on a second
// pool at an exact statement boundary of the pass: right after the start guard,
// or right after the commit-time check. No sleeps; the competing write commits
// (or fails on the held row) before the pass sends its next statement.

type fenceSQLKey struct{}

type fenceHook struct {
	match func(sql string) bool
	run   func(ctx context.Context)
	fired bool
}

type fenceTracer struct{ hooks []*fenceHook }

func (tracer *fenceTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, fenceSQLKey{}, data.SQL)
}

func (tracer *fenceTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(fenceSQLKey{}).(string)
	if data.Err != nil {
		return
	}
	for _, hook := range tracer.hooks {
		if !hook.fired && hook.match(sql) {
			hook.fired = true
			hook.run(ctx)
		}
	}
}

func isStartGuard(sql string) bool {
	return strings.Contains(sql, "SELECT EXISTS") && strings.Contains(sql, "public.sync_dispatch_outbox AS outbox")
}

func isCommitCheck(sql string) bool {
	return strings.Contains(sql, "FOR NO KEY UPDATE OF outbox NOWAIT")
}

// rearmOutboxSQL is the canonical wakeup upsert: every wake merges through
// syncrunrollup.OutboxWakeupOnConflictSQL, which moves the row to 'pending'.
const rearmOutboxSQL = `
INSERT INTO public.sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at)
VALUES ($1::uuid, $2, $3::uuid, $4, 'pending', now(), 0, now(), now())
` + syncrunrollup.OutboxWakeupOnConflictSQL

type fenceKind struct {
	name, kind, org, run, outbox string
	seed                         func(t *testing.T, ctx context.Context, pool *pgxpool.Pool)
	// pass runs one delivery of the given attempt through the service built on pool.
	pass  func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error
	wrote func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) bool
}

func fenceKinds() []fenceKind {
	discoveryCalls := 0
	return []fenceKind{
		{
			name: "finalize", kind: outboxKindFinalizeSyncRun, org: finalizeTestOrg, run: finalizeTestRun, outbox: finalizeTestOutbox,
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedFinalizeRoute(t, ctx, pool)
				insertZeroUnitRun(t, ctx, pool, "", "")
			},
			pass: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
				service, err := NewNativeFinalizeSyncRunService(pool, nil)
				if err != nil {
					t.Fatal(err)
				}
				args := newFinalizeArgs()
				args.DeliveryAttempt = attempt
				return service.Finalize(ctx, args)
			},
			wrote: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) bool {
				var status string
				if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1`, finalizeTestRun).Scan(&status); err != nil {
					t.Fatal(err)
				}
				return status != "dispatching"
			},
		},
		{
			name: "dispatch", kind: outboxKindDispatchSyncRun, org: discoveryTestOrg, run: discoveryTestRun, outbox: discoveryTestOutbox,
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				if got := pgseed.SyncTransportRoute(ctx, t, pool, outboxKindDispatchSyncRun, "river", discoveryRouteGeneration, false, "celery"); got != discoveryRouteGeneration {
					t.Fatalf("dispatch_sync_run route generation = %d, want %d", got, discoveryRouteGeneration)
				}
				pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: discoveryTestRun, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration})
				pgseed.SyncDispatchOutbox(ctx, t, pool, discoveryTestOutbox, discoveryTestRun, discoveryTestOrg, outboxKindDispatchSyncRun, "dispatched", "river", discoveryRouteGeneration)
			},
			pass: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
				registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{
					jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
				}}
				producer, err := joboutbox.NewProducer(pool, registry)
				if err != nil {
					t.Fatal(err)
				}
				service, err := NewNativeDispatchSyncRunService(pool, nil, &fakeBudgetEstimator{}, producer, registry)
				if err != nil {
					t.Fatal(err)
				}
				return service.Dispatch(ctx, DispatchSyncRunArgs{TransportArgs: TransportArgs{
					Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
					DispatchOutbox: discoveryTestOutbox, RouteGeneration: discoveryRouteGeneration, DeliveryAttempt: attempt,
				}})
			},
			// With no reference-discovery ledger the pass arms a reference_discovery wakeup and commits.
			wrote: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) bool {
				var rows int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = 'reference_discovery'`,
					discoveryTestRun).Scan(&rows); err != nil {
					t.Fatal(err)
				}
				return rows > 0
			},
		},
		{
			name: "discovery", kind: outboxKindReferenceDiscovery, org: discoveryTestOrg, run: discoveryTestRun, outbox: discoveryTestOutbox,
			seed: seedDiscoveryRoute,
			pass: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
				executor := &fakeDiscoveryExecutor{summary: map[string]any{"reference_team_keys": []string{"ENG"}}}
				service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
				if err != nil {
					t.Fatal(err)
				}
				args := newDiscoveryArgs()
				args.DeliveryAttempt = attempt
				err = service.Discover(ctx, args)
				discoveryCalls = executor.calls
				return err
			},
			wrote: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) bool {
				var rows int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_run_reference_discoveries WHERE sync_run_id = $1`,
					discoveryTestRun).Scan(&rows); err != nil {
					t.Fatal(err)
				}
				return rows > 0 || discoveryCalls > 0
			},
		},
	}
}

type fenceRig struct {
	kind    fenceKind
	pool    *pgxpool.Pool // the service's pool, traced
	other   *pgxpool.Pool // the competing writer and the assertions
	tracer  *fenceTracer
	release []func()
}

func withFenceRig(t *testing.T, kind fenceKind, rowAttempts int64, fn func(ctx context.Context, rig *fenceRig)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	other, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	kind.seed(t, ctx, other)
	if _, err := other.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = $2 WHERE id = $1`, kind.outbox, rowAttempts); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	tracer := &fenceTracer{}
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rig := &fenceRig{kind: kind, pool: pool, other: other, tracer: tracer}
	defer func() {
		for _, release := range rig.release {
			release()
		}
	}()
	fn(ctx, rig)
}

func (rig *fenceRig) on(match func(string) bool, run func(ctx context.Context)) *fenceHook {
	hook := &fenceHook{match: match, run: run}
	rig.tracer.hooks = append(rig.tracer.hooks, hook)
	return hook
}

func (rig *fenceRig) rearm(ctx context.Context, db interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	_, err := db.Exec(ctx, rearmOutboxSQL, uuid.NewString(), rig.kind.org, rig.kind.run, rig.kind.kind)
	return err
}

func (rig *fenceRig) outboxState(t *testing.T, ctx context.Context) (string, int64) {
	t.Helper()
	var status string
	var attempts int64
	if err := rig.other.QueryRow(ctx, `SELECT status, attempts FROM sync_dispatch_outbox WHERE id = $1`, rig.kind.outbox).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	return status, attempts
}

// TestTransportFenceIntegration: P1-2 (a superseded delivery attempt) and P1-3
// (a wake that re-arms the row after the start guard) commit nothing, for
// every native service that checks the transport reference; the current
// delivery still commits.
func TestTransportFenceIntegration(t *testing.T) {
	for _, kind := range fenceKinds() {
		t.Run(kind.name+"/current_first_attempt", func(t *testing.T) {
			withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
				if err := kind.pass(t, ctx, rig.pool, 1); err != nil || !kind.wrote(t, ctx, rig.other) {
					t.Fatalf("current delivery: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, rig.other))
				}
			})
		})
		t.Run(kind.name+"/current_redelivery", func(t *testing.T) {
			withFenceRig(t, kind, 2, func(ctx context.Context, rig *fenceRig) {
				if err := kind.pass(t, ctx, rig.pool, 2); err != nil || !kind.wrote(t, ctx, rig.other) {
					t.Fatalf("current redelivery: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, rig.other))
				}
			})
		})
		t.Run(kind.name+"/redelivery_committed_before_guard", func(t *testing.T) {
			withFenceRig(t, kind, 2, func(ctx context.Context, rig *fenceRig) {
				err := kind.pass(t, ctx, rig.pool, 1)
				if err != nil || kind.wrote(t, ctx, rig.other) {
					t.Fatalf("delivery attempt 1 on a row at attempt 2: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
				}
			})
		})
		t.Run(kind.name+"/redelivery_committed_after_guard", func(t *testing.T) {
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
		t.Run(kind.name+"/rearmed_after_guard", func(t *testing.T) {
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
				if err != nil || kind.wrote(t, ctx, rig.other) {
					t.Fatalf("re-armed after the guard: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
				}
				if status, _ := rig.outboxState(t, ctx); status != "pending" {
					t.Fatalf("outbox status=%q after the wake, want pending (the wake must survive)", status)
				}
			})
		})
	}
}

// TestTransportFenceHoldsTheRowFromCheckToCommit: once the commit-time check
// passed, a wake cannot land before the pass commits (it waits on the row
// lock; lock_timeout makes the wait observable without a sleep), and it lands
// after the commit.
func TestTransportFenceHoldsTheRowFromCheckToCommit(t *testing.T) {
	kind := fenceKinds()[0]
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

// TestTransportFenceRetriesWhenTheRowIsBusy: the commit-time check never waits
// for the row lock. A row held by another transaction makes the pass return
// its Unavailable error (River retries the same delivery) with nothing committed.
func TestTransportFenceRetriesWhenTheRowIsBusy(t *testing.T) {
	kind := fenceKinds()[0]
	withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
		hook := rig.on(isStartGuard, func(ctx context.Context) {
			tx, err := rig.other.Begin(ctx)
			if err != nil {
				t.Errorf("holder begin: %v", err)
				return
			}
			rig.release = append(rig.release, func() { _ = tx.Rollback(context.Background()) })
			var id string
			if err := tx.QueryRow(ctx, `SELECT id::text FROM sync_dispatch_outbox WHERE id = $1 FOR UPDATE`, kind.outbox).Scan(&id); err != nil {
				t.Errorf("holder lock: %v", err)
			}
		})
		err := kind.pass(t, ctx, rig.pool, 1)
		if !hook.fired {
			t.Fatal("the start guard never ran")
		}
		if !errors.Is(err, ErrFinalizeSyncRunUnavailable) {
			t.Fatalf("pass with the outbox row held elsewhere: err=%v, want %v", err, ErrFinalizeSyncRunUnavailable)
		}
		if kind.wrote(t, ctx, rig.other) {
			t.Fatal("pass committed while the outbox row was held elsewhere")
		}
	})
}

// I-OWN: the commit-time check sees the pass's own uncommitted writes. The
// feature-disabled graph parks every PENDING outbox row of the run
// (feature_disabled_termination.go terminalizeFeatureDisabledGraph); a
// legitimate pass holds its own row 'dispatched', so the park touches only
// siblings and the pass must still commit.
func assertOwnRowKeptAndSiblingParked(t *testing.T, ctx context.Context, pool *pgxpool.Pool, ownID, siblingKind string) {
	t.Helper()
	var status string
	var transport *string
	if err := pool.QueryRow(ctx, `SELECT status, dispatched_transport FROM sync_dispatch_outbox WHERE id = $1`, ownID).Scan(&status, &transport); err != nil {
		t.Fatal(err)
	}
	if status != "dispatched" || transport == nil || *transport != "river" {
		t.Fatalf("own outbox row status=%q transport=%v, want dispatched/river", status, transport)
	}
	var siblingStatus string
	var siblingError *string
	if err := pool.QueryRow(ctx, `SELECT status, last_error FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`,
		discoveryTestRun, siblingKind).Scan(&siblingStatus, &siblingError); err != nil {
		t.Fatal(err)
	}
	if siblingStatus != "dispatched" || siblingError == nil || *siblingError != featureDisabledErrorCategory {
		t.Fatalf("sibling %s row status=%q last_error=%v, want dispatched/%s", siblingKind, siblingStatus, siblingError, featureDisabledErrorCategory)
	}
}

func TestTransportFenceOwnRowDispatchFeatureDisabledCommits(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		featureID := "00000000-0000-4000-8000-0000000000e7"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ('00000000-0000-4000-8000-0000000000e8',$1,$2::uuid,'pagerduty','incidents','00000000-0000-4000-8000-0000000000ec','planned',now(),
        (SELECT integration_id FROM sync_runs WHERE id = $2::uuid),'rest_core','incremental',0,now())`,
			discoveryTestOrg, discoveryTestRun); err != nil {
			t.Fatal(err)
		}
		pgseed.SetFeatureFlag(ctx, t, pool, featureID, "canonical_incident_ingestion", "enterprise", true)
		pgseed.OrgOverride(ctx, t, pool, discoveryTestOrg, featureID, false)
		pgseed.SyncDispatchOutbox(ctx, t, pool, uuid.NewString(), discoveryTestRun, discoveryTestOrg, outboxKindReferenceDiscovery, "pending", "", 0)
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, dispatchServiceTestOutbox); err != nil {
			t.Fatal(err)
		}
		args := dispatchTestArgs()
		args.DeliveryAttempt = 1
		if err := newTestDispatchService(t, pool).Dispatch(ctx, args); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		var runStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1`, discoveryTestRun).Scan(&runStatus); err != nil {
			t.Fatal(err)
		}
		if runStatus != syncRunStatusFailed {
			t.Fatalf("run status=%q, want failed (the legitimate feature-disabled pass committed)", runStatus)
		}
		assertOwnRowKeptAndSiblingParked(t, ctx, pool, dispatchServiceTestOutbox, outboxKindReferenceDiscovery)
	})
}

func TestTransportFenceOwnRowDiscoveryFeatureDisabledCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	seedDiscoveryRoute(t, ctx, pool)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: "00000000-0000-4000-8000-0000000000d9", RunID: discoveryTestRun, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration,
		SourceID: "00000000-0000-4000-8000-0000000000da", Provider: "pagerduty", DatasetKey: "incidents", Status: "planned",
	})
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'canonical_incident_ingestion'`); err != nil {
		t.Fatal(err)
	}
	pgseed.SyncDispatchOutbox(ctx, t, pool, uuid.NewString(), discoveryTestRun, discoveryTestOrg, outboxKindDispatchSyncRun, "pending", "", 0)
	if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, discoveryTestOutbox); err != nil {
		t.Fatal(err)
	}
	executor := &fakeDiscoveryExecutor{summary: map[string]any{"ok": true}}
	service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	args := newDiscoveryArgs()
	args.DeliveryAttempt = 1
	if err := service.Discover(ctx, args); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	var runStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1`, discoveryTestRun).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != syncRunStatusFailed {
		t.Fatalf("run status=%q, want failed (the legitimate feature-disabled claim committed)", runStatus)
	}
	assertOwnRowKeptAndSiblingParked(t, ctx, pool, discoveryTestOutbox, outboxKindDispatchSyncRun)
}
