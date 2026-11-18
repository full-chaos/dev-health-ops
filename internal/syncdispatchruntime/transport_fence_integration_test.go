//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// isStartGuard matches the first fence read of a pass: currentTransportReference
// (finalize, discovery) or readTransportReference (Dispatch). A hook fires once,
// so Dispatch's commit-time diagnosis (readTransportReference again) never re-fires it.
func isStartGuard(sql string) bool {
	return strings.Contains(sql, "SELECT EXISTS") && strings.Contains(sql, "public.sync_dispatch_outbox AS outbox") ||
		strings.Contains(sql, "outbox.found, outbox.status")
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
	generation                   int64
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
			generation: finalizeRouteGeneration,
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
			generation: discoveryRouteGeneration,
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
			generation: discoveryRouteGeneration,
			seed:       seedDiscoveryRoute,
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

// TestTransportFenceIntegration: a superseded delivery (P1-2: the attempt
// moved on; or a re-armed row that was claimed again or whose route moved)
// commits nothing, for every native service that checks the transport
// reference. The current delivery commits, also when a same-attempt wake
// re-armed its row after the guard.
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
		// A same-attempt wake does not supersede the delivery: the pass commits
		// and the wake stays armed for the next pass.
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
				if err != nil || !kind.wrote(t, ctx, rig.other) {
					t.Fatalf("re-armed after the guard: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, rig.other))
				}
				if status, attempts := rig.outboxState(t, ctx); status != "pending" || attempts != 1 {
					t.Fatalf("outbox status=%q attempts=%d after the wake, want pending/1 (the wake must survive)", status, attempts)
				}
			})
		})
		t.Run(kind.name+"/rearmed_then_claimed_after_guard", func(t *testing.T) {
			withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
				hook := rig.on(isStartGuard, func(ctx context.Context) {
					if err := rig.rearm(ctx, rig.other); err != nil {
						t.Errorf("wake: %v", err)
						return
					}
					// The relay's claim (syncreconciler claimRiverRoutesSQL): a claim token and attempts + 1.
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
		t.Run(kind.name+"/rearmed_and_route_moved_after_guard", func(t *testing.T) {
			withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
				hook := rig.on(isStartGuard, func(ctx context.Context) {
					if err := rig.rearm(ctx, rig.other); err != nil {
						t.Errorf("wake: %v", err)
						return
					}
					// The route table bumps the generation only on a state change: pause, then resume, so the
					// route is River and unpaused again but at a later generation (only the generation term moves).
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

// relayRedeliver stands in for the relay's claim and River delivery of a
// re-armed row: attempts + 1 and dispatched to the route
// (syncreconciler claimRiverRoutesSQL, markRiverDispatchedSQL).
func relayRedeliver(t *testing.T, ctx context.Context, pool *pgxpool.Pool, outboxID string, generation int64) int64 {
	t.Helper()
	var attempts int64
	if err := pool.QueryRow(ctx, `
UPDATE sync_dispatch_outbox
SET status = 'dispatched', dispatched_at = now(), dispatched_transport = 'river', dispatched_route_generation = $2,
    transport_job_id = NULL, last_error = NULL, claim_token = NULL, claim_expires_at = NULL, claim_transport = NULL,
    claim_route_generation = NULL, attempts = attempts + 1, updated_at = now()
WHERE id = $1 AND status = 'pending'
RETURNING attempts`, outboxID, generation).Scan(&attempts); err != nil {
		t.Fatalf("relay redelivery: %v", err)
	}
	return attempts
}

func runFailed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) bool {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1`, discoveryTestRun).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status == syncRunStatusFailed
}

// rearmedMidPassConverges: on the feature-disabled terminal branch a wake after
// the start guard makes the pass park its own re-armed row, so the commit-time
// check refuses it. The wake's own delivery (attempt + 1) then runs the branch
// to the end: run failed, own row dispatched to the route, sibling parked.
func rearmedMidPassConverges(t *testing.T, kind fenceKind, siblingKind string) {
	withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
		hook := rig.on(isStartGuard, func(ctx context.Context) {
			if err := rig.rearm(ctx, rig.other); err != nil {
				t.Errorf("wake: %v", err)
			}
		})
		if err := kind.pass(t, ctx, rig.pool, 1); err != nil {
			t.Fatalf("first pass: %v", err)
		}
		if !hook.fired {
			t.Fatal("the start guard never ran")
		}
		if kind.wrote(t, ctx, rig.other) {
			t.Fatal("first pass committed although its own park changed its re-armed row")
		}
		if status, attempts := rig.outboxState(t, ctx); status != "pending" || attempts != 1 {
			t.Fatalf("after the refused pass: outbox status=%q attempts=%d, want pending/1 (the wake survives)", status, attempts)
		}
		attempt := relayRedeliver(t, ctx, rig.other, kind.outbox, kind.generation)
		if err := kind.pass(t, ctx, rig.pool, attempt); err != nil {
			t.Fatalf("second pass (attempt %d): %v", attempt, err)
		}
		if !kind.wrote(t, ctx, rig.other) {
			t.Fatalf("second pass (attempt %d) did not run the feature-disabled branch to the end", attempt)
		}
		assertOwnRowKeptAndSiblingParked(t, ctx, rig.other, kind.outbox, siblingKind)
	})
}

func TestTransportFenceOwnRowDispatchFeatureDisabledRearmedMidPass(t *testing.T) {
	rearmedMidPassConverges(t, fenceKind{
		name: "dispatch-feature-disabled", kind: outboxKindDispatchSyncRun, org: discoveryTestOrg, run: discoveryTestRun,
		outbox: dispatchServiceTestOutbox, generation: dispatchRouteGeneration,
		seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			pgseed.Org(ctx, t, pool, discoveryTestOrg, "community")
			seedDispatchRoute(t, ctx, pool)
			if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ('00000000-0000-4000-8000-0000000000e8',$1,$2::uuid,'pagerduty','incidents','00000000-0000-4000-8000-0000000000ec','planned',now(),
        (SELECT integration_id FROM sync_runs WHERE id = $2::uuid),'rest_core','incremental',0,now())`,
				discoveryTestOrg, discoveryTestRun); err != nil {
				t.Fatal(err)
			}
			featureID := "00000000-0000-4000-8000-0000000000e7"
			pgseed.SetFeatureFlag(ctx, t, pool, featureID, "canonical_incident_ingestion", "enterprise", true)
			pgseed.OrgOverride(ctx, t, pool, discoveryTestOrg, featureID, false)
			pgseed.SyncDispatchOutbox(ctx, t, pool, uuid.NewString(), discoveryTestRun, discoveryTestOrg, outboxKindReferenceDiscovery, "pending", "", 0)
		},
		pass: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
			args := dispatchTestArgs()
			args.DeliveryAttempt = attempt
			return newTestDispatchService(t, pool).Dispatch(ctx, args)
		},
		wrote: runFailed,
	}, outboxKindReferenceDiscovery)
}

func TestTransportFenceOwnRowDiscoveryFeatureDisabledRearmedMidPass(t *testing.T) {
	rearmedMidPassConverges(t, fenceKind{
		name: "discovery-feature-disabled", kind: outboxKindReferenceDiscovery, org: discoveryTestOrg, run: discoveryTestRun,
		outbox: discoveryTestOutbox, generation: discoveryRouteGeneration,
		seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
			seedDiscoveryRoute(t, ctx, pool)
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: "00000000-0000-4000-8000-0000000000d9", RunID: discoveryTestRun, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration,
				SourceID: "00000000-0000-4000-8000-0000000000da", Provider: "pagerduty", DatasetKey: "incidents", Status: "planned",
			})
			if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'canonical_incident_ingestion'`); err != nil {
				t.Fatal(err)
			}
			pgseed.SyncDispatchOutbox(ctx, t, pool, uuid.NewString(), discoveryTestRun, discoveryTestOrg, outboxKindDispatchSyncRun, "pending", "", 0)
		},
		pass: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
			service, err := NewNativeReferenceDiscoveryService(pool, nil, &fakeDiscoveryExecutor{summary: map[string]any{"ok": true}})
			if err != nil {
				t.Fatal(err)
			}
			args := newDiscoveryArgs()
			args.DeliveryAttempt = attempt
			return service.Discover(ctx, args)
		},
		wrote: runFailed,
	}, outboxKindDispatchSyncRun)
}

// TestTransportFenceDispatchNamesDeliveryAttemptMismatch: a Dispatch delivery
// whose attempt is superseded is a no-op that says so, at the start guard and
// at the commit-time check.
func TestTransportFenceDispatchNamesDeliveryAttemptMismatch(t *testing.T) {
	logged := func(logs *lockedLogBuffer) fenceKind {
		kind := fenceKinds()[1]
		kind.pass = func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
			registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{
				jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
			}}
			producer, err := joboutbox.NewProducer(pool, registry)
			if err != nil {
				t.Fatal(err)
			}
			logger := synclog.New(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			service, err := NewNativeDispatchSyncRunService(pool, logger, &fakeBudgetEstimator{}, producer, registry)
			if err != nil {
				t.Fatal(err)
			}
			return service.Dispatch(ctx, DispatchSyncRunArgs{TransportArgs: TransportArgs{
				Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
				DispatchOutbox: discoveryTestOutbox, RouteGeneration: discoveryRouteGeneration, DeliveryAttempt: attempt,
			}})
		}
		return kind
	}
	assertLine := func(t *testing.T, logs *lockedLogBuffer) {
		t.Helper()
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=dispatch_sync_run.delivery_not_current") && strings.Contains(line, "reason=delivery_attempt_mismatch ") {
				return
			}
		}
		t.Fatalf("no dispatch_sync_run.delivery_not_current reason=delivery_attempt_mismatch line:\n%s", strings.Join(logs.lines(), "\n"))
	}
	t.Run("before_guard", func(t *testing.T) {
		logs := &lockedLogBuffer{}
		kind := logged(logs)
		withFenceRig(t, kind, 2, func(ctx context.Context, rig *fenceRig) {
			if err := kind.pass(t, ctx, rig.pool, 1); err != nil || kind.wrote(t, ctx, rig.other) {
				t.Fatalf("superseded delivery: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
			}
			assertLine(t, logs)
		})
	})
	t.Run("after_guard", func(t *testing.T) {
		logs := &lockedLogBuffer{}
		kind := logged(logs)
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
			assertLine(t, logs)
		})
	})
}

// TestTransportFenceCommitRefusalNamesTheSupersedingClause: when a wake
// re-armed the row after the start guard and a claim or a route move then
// superseded the delivery, the commit-time refusal names that clause, not the
// row's 'pending' status (which the commit check allows).
func TestTransportFenceCommitRefusalNamesTheSupersedingClause(t *testing.T) {
	claim := func(t *testing.T, ctx context.Context, rig *fenceRig) {
		if _, err := rig.other.Exec(ctx, `
UPDATE sync_dispatch_outbox
SET claim_token = gen_random_uuid()::text, claim_expires_at = now() + interval '1 minute',
    claim_transport = 'river', claim_route_generation = $2, attempts = attempts + 1, updated_at = now()
WHERE id = $1`, rig.kind.outbox, rig.kind.generation); err != nil {
			t.Errorf("claim: %v", err)
		}
	}
	moveRoute := func(t *testing.T, ctx context.Context, rig *fenceRig) {
		paused := pgseed.SyncTransportRoute(ctx, t, rig.other, rig.kind.kind, "river", rig.kind.generation+1, true, "celery")
		pgseed.SyncTransportRoute(ctx, t, rig.other, rig.kind.kind, "river", paused+1, false, "celery")
	}
	supersede := []struct {
		name, reason string
		after        func(t *testing.T, ctx context.Context, rig *fenceRig)
	}{
		{"rearmed_then_claimed", "delivery_attempt_mismatch", claim},
		{"rearmed_and_route_moved", "route_generation_mismatch", moveRoute},
	}
	run := func(t *testing.T, kind fenceKind, logs *lockedLogBuffer, after func(*testing.T, context.Context, *fenceRig)) {
		withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
			hook := rig.on(isStartGuard, func(ctx context.Context) {
				if err := rig.rearm(ctx, rig.other); err != nil {
					t.Errorf("wake: %v", err)
					return
				}
				after(t, ctx, rig)
			})
			err := kind.pass(t, ctx, rig.pool, 1)
			if !hook.fired {
				t.Fatal("the start guard never ran")
			}
			if err != nil || kind.wrote(t, ctx, rig.other) {
				t.Fatalf("superseded after a wake: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
			}
		})
	}
	for _, c := range supersede {
		t.Run("finalize/"+c.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			previous := otel.GetMeterProvider()
			otel.SetMeterProvider(provider)
			t.Cleanup(func() {
				otel.SetMeterProvider(previous)
				_ = provider.Shutdown(context.Background())
			})
			logs := &lockedLogBuffer{}
			kind := fenceKinds()[0]
			kind.pass = func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
				service, err := NewNativeFinalizeSyncRunService(pool, synclog.New(slog.New(slog.NewTextHandler(logs, nil))))
				if err != nil {
					t.Fatal(err)
				}
				args := newFinalizeArgs()
				args.DeliveryAttempt = attempt
				return service.Finalize(ctx, args)
			}
			run(t, kind, logs, c.after)
			var resource metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &resource); err != nil {
				t.Fatal(err)
			}
			counted := map[string]int64{}
			for _, scope := range resource.ScopeMetrics {
				for _, m := range scope.Metrics {
					if m.Name != FinalizeDeliveryNotCurrentCounter {
						continue
					}
					for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
						reason, _ := point.Attributes.Value("reason")
						counted[reason.AsString()] += point.Value
					}
				}
			}
			if counted[c.reason] != 1 || len(counted) != 1 {
				t.Fatalf("%s by reason = %v, want exactly {%s: 1}", FinalizeDeliveryNotCurrentCounter, counted, c.reason)
			}
			if !strings.Contains(strings.Join(logs.lines(), "\n"), "reason="+c.reason+" ") {
				t.Fatalf("finalize refusal line lacks reason=%s:\n%s", c.reason, strings.Join(logs.lines(), "\n"))
			}
		})
		t.Run("dispatch/"+c.name, func(t *testing.T) {
			logs := &lockedLogBuffer{}
			kind := fenceKinds()[1]
			kind.pass = func(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempt int64) error {
				registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{
					jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
				}}
				producer, err := joboutbox.NewProducer(pool, registry)
				if err != nil {
					t.Fatal(err)
				}
				logger := synclog.New(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
				service, err := NewNativeDispatchSyncRunService(pool, logger, &fakeBudgetEstimator{}, producer, registry)
				if err != nil {
					t.Fatal(err)
				}
				return service.Dispatch(ctx, DispatchSyncRunArgs{TransportArgs: TransportArgs{
					Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
					DispatchOutbox: discoveryTestOutbox, RouteGeneration: discoveryRouteGeneration, DeliveryAttempt: attempt,
				}})
			}
			run(t, kind, logs, c.after)
			found := false
			for _, line := range logs.lines() {
				if strings.Contains(line, "msg=dispatch_sync_run.delivery_not_current") && strings.Contains(line, "reason="+c.reason+" ") {
					found = true
				}
			}
			if !found {
				t.Fatalf("no dispatch_sync_run.delivery_not_current reason=%s line:\n%s", c.reason, strings.Join(logs.lines(), "\n"))
			}
		})
	}
}
