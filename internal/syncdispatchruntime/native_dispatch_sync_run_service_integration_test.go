//go:build integration

package syncdispatchruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fakeJobRegistry satisfies joboutbox.PolicyRegistry without needing the
// real jobruntime.Load's checked-in contract/migration artifacts on disk --
// this test only needs ONE kind's descriptor (sync.provider_unit) under a
// caller-controlled Route, to prove Publish's own descriptorAllowsPublish
// gate wires correctly (CHAOS-4175 ruling: this is now the ONLY route check
// Dispatch's write path is subject to -- see NativeDispatchSyncRunService's
// doc comment for why there is no jobroute.Controller here at all), not to
// re-test jobruntime's own artifact loading.
type fakeJobRegistry struct {
	descriptors map[string]jobruntime.Descriptor
}

func (registry *fakeJobRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := registry.descriptors[kind]
	return descriptor, ok
}

func (registry *fakeJobRegistry) Descriptors() []jobruntime.Descriptor {
	list := make([]jobruntime.Descriptor, 0, len(registry.descriptors))
	for _, descriptor := range registry.descriptors {
		list = append(list, descriptor)
	}
	return list
}

func providerUnitDescriptor(route string) jobruntime.Descriptor {
	return jobruntime.Descriptor{
		Kind:              jobcontract.KindSyncProviderUnit,
		CurrentVersion:    jobcontract.ContractVersionV1,
		SupportedVersions: []int{jobcontract.ContractVersionV1},
		Queue:             "sync",
		// River's InsertOpts.Priority defaults 0 to PriorityDefault (1) --
		// leaving this at the Go zero value made a route-hold integration
		// test's relay.Step land on ErrPolicyRejected the first time this
		// descriptor was actually pushed through RiverInserter.Insert
		// (every earlier test using it only checked Producer.Publish's own
		// outbox row, never a real River delivery, so the mismatch between
		// the row's stamped Priority=0 and River's substituted Priority=1
		// was never exercised until then). Set to a real, valid (1-4)
		// value so this descriptor is safe for any future test that DOES
		// push it all the way through delivery.
		Priority:          2,
		MaxAttempts:       5,
		DomainLink:        "sync_run_unit",
		OrganizationScope: "tenant",
		Route:             route,
		RollbackRoute:     "celery",
	}
}

const (
	dispatchServiceTestOutbox = "00000000-0000-4000-8000-0000000000e9"
)

func withDispatchServicePool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createReferenceDiscoveryTables(t, ctx, pool)
	// DispatchGuard.authorize_run's total-cap resolution
	// (scheduledsync.ResolveMaxSyncUnitsCap -> loadPlanLimits) queries
	// organizations/org_licenses -- absent in production only for a
	// malformed org id, never for a real one, so these tables must exist
	// here too: a missing-relation ERROR poisons the whole enclosing
	// Postgres transaction (25P02), which Go-level fallback-to-default
	// error handling cannot undo without a savepoint. Production always has
	// these tables; this is a test-fixture-completeness requirement, not a
	// production behavior this port needs to defend against.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.organizations (id uuid PRIMARY KEY, tier text NULL);
CREATE TABLE public.org_licenses (org_id uuid PRIMARY KEY, tier text NULL, limits_override jsonb NULL);
CREATE TABLE public.tier_limits (tier text NOT NULL, limit_key text NOT NULL, limit_value text NULL, PRIMARY KEY (tier, limit_key));
INSERT INTO public.organizations (id, tier) VALUES ('`+discoveryTestOrg+`', 'community');`); err != nil {
		t.Fatal(err)
	}
	// joboutbox.Producer.Publish's own table. Deliberately no worker_job_routes
	// here (CHAOS-4175 ruling, superseding an earlier one on this branch):
	// Dispatch's write path never reads the live route store -- the domain
	// role has no grant on it in production -- so this fixture doesn't need
	// it either. See NativeDispatchSyncRunService's doc comment.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.worker_job_outbox (
  id uuid PRIMARY KEY, dedupe_key text NOT NULL UNIQUE, job_kind text NOT NULL,
  contract_version int NOT NULL, args json NOT NULL, payload_hash text NOT NULL,
  queue text NOT NULL, priority int NOT NULL, max_attempts int NOT NULL,
  scheduled_at timestamptz NOT NULL, status text NOT NULL, attempt_count int NOT NULL,
  next_attempt_at timestamptz NOT NULL, prerequisite_completion_key text NULL,
  created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);`); err != nil {
		t.Fatal(err)
	}
	// activeCooldowns (inside enforceRun) reads this table and is
	// deliberately fail-open on a query error -- but the underlying
	// Postgres ERROR (missing relation) still poisons the enclosing
	// transaction (25P02) even though the Go-level error is swallowed.
	// Same fixture-completeness class as organizations/tier_limits above.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.provider_rate_limit_observations (
 id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, host text NULL,
 integration_id uuid NOT NULL, sync_run_id uuid NOT NULL, sync_run_unit_id uuid NOT NULL,
 route_family text NULL, route_family_attribution text NULL, dimension text NULL,
 retry_after_seconds double precision NULL, reset_at timestamptz NULL, reason text NULL,
 request_id text NULL, observed_at timestamptz NOT NULL
);`); err != nil {
		t.Fatal(err)
	}
	fn(ctx, pool)
}

// seedDispatchRoute mirrors seedDiscoveryRoute but for kind=dispatch_sync_run
// -- the currentTransportReference guard Dispatch() checks first, before any
// business logic.
func seedDispatchRoute(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
		 VALUES ('dispatch_sync_run','river',1,false,'celery')`,
		`INSERT INTO sync_dispatch_outbox
		    (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
		 VALUES ('` + dispatchServiceTestOutbox + `','` + discoveryTestRun + `','` + discoveryTestOrg + `',
		         'dispatch_sync_run','dispatched',now(),'river',1,now(),now())`,
		`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ('` + discoveryTestRun + `','` +
			discoveryTestOrg + `','` + discoveryTestIntegration + `')`,
		`INSERT INTO integrations (id,org_id,provider) VALUES ('` + discoveryTestIntegration + `','` + discoveryTestOrg + `','github')`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func dispatchTestArgs() DispatchSyncRunArgs {
	return DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
		DispatchOutbox: dispatchServiceTestOutbox, RouteGeneration: 1,
	}}
}

func markReferenceDiscoverySucceeded(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at)
VALUES ('00000000-0000-4000-8000-0000000000ea',$1,$2,$3,1,now())`,
		discoveryTestRun, discoveryTestOrg, discoveryStatusSuccess); err != nil {
		t.Fatal(err)
	}
}

// newTestDispatchService constructs the service with a registry reporting
// route for sync.provider_unit -- callers that need a DIFFERENT capability
// answer per test pass their own registry via newTestDispatchServiceWith.
func newTestDispatchService(t *testing.T, pool *pgxpool.Pool) *NativeDispatchSyncRunService {
	t.Helper()
	return newTestDispatchServiceWith(t, pool, &fakeJobRegistry{
		descriptors: map[string]jobruntime.Descriptor{
			jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
		},
	})
}

func newTestDispatchServiceWith(t *testing.T, pool *pgxpool.Pool, registry *fakeJobRegistry) *NativeDispatchSyncRunService {
	t.Helper()
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatalf("joboutbox.NewProducer: %v", err)
	}
	service, err := NewNativeDispatchSyncRunService(pool, nil, &fakeBudgetEstimator{}, producer, registry)
	if err != nil {
		t.Fatalf("NewNativeDispatchSyncRunService: %v", err)
	}
	return service
}

// TestDispatchReturnsNilWhenTheTransportReferenceIsStale pins the
// outbox-relay fence: a job whose route generation no longer matches the
// current durable route must be a silent no-op, not business logic
// running against a superseded route.
func TestDispatchReturnsNilWhenTheTransportReferenceIsStale(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		// Deliberately do NOT seed the route -- currentTransportReference's
		// EXISTS query then has nothing to match.
		if _, err := pool.Exec(ctx, `INSERT INTO sync_runs (id,org_id,integration_id) VALUES ($1,$2,$3)`,
			discoveryTestRun, discoveryTestOrg, discoveryTestIntegration); err != nil {
			t.Fatal(err)
		}
		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (stale reference is a silent no-op)", err)
		}
	})
}

// TestDispatchCommitsAndReturnsNilForAMissingRun pins the missing-run
// branch: Python returns {"status": "missing", ...} without error; the Go
// port commits a no-op and returns nil.
func TestDispatchCommitsAndReturnsNilForAMissingRun(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		// Seed the route/outbox but NOT the sync_runs row itself.
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
VALUES ('dispatch_sync_run','river',1,false,'celery')`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
VALUES ($1,$2,$3,'dispatch_sync_run','dispatched',now(),'river',1,now(),now())`,
			dispatchServiceTestOutbox, discoveryTestRun, discoveryTestOrg); err != nil {
			t.Fatal(err)
		}
		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}
	})
}

// TestDispatchBlocksOnReferenceDiscoveryAndArmsAWakeup pins the
// reference-discovery gate: with no success ledger row, Dispatch() must
// not proceed to DispatchGuard at all, and must arm a reference-discovery
// wakeup.
func TestDispatchBlocksOnReferenceDiscoveryAndArmsAWakeup(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='reference_discovery'`,
			discoveryTestRun).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("got %d reference_discovery outbox rows, want 1 (a wakeup was armed)", count)
		}
	})
}

// TestDispatchDeniesWithActiveUnitsFailsOnlyStrandedOnes pins the total-cap
// hard-deny branch's FIRST shape: with an active (RUNNING) unit present,
// only the never-going-to-dispatch-again units are failed, the run itself
// stays non-terminal, and a finalize check is armed instead of running the
// whole run to FAILED immediately.
func TestDispatchDeniesWithActiveUnitsFailsOnlyStrandedOnes(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		strandedUnit := "00000000-0000-4000-8000-0000000000eb"
		runningUnit := "00000000-0000-4000-8000-0000000000ec"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$5),
       ($4,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','running',$5)`,
			strandedUnit, discoveryTestOrg, discoveryTestRun, runningUnit, now); err != nil {
			t.Fatal(err)
		}
		// Force DispatchGuard to deny via the total-cap: SYNC_RUN_MAX_UNITS=1
		// with 2 units already present denies the whole run.
		t.Setenv("SYNC_RUN_MAX_UNITS", "1")

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		var strandedStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, strandedUnit).Scan(&strandedStatus); err != nil {
			t.Fatal(err)
		}
		if strandedStatus != syncRunUnitStatusFailed {
			t.Fatalf("strandedUnit status=%q, want failed", strandedStatus)
		}
		var runStatus string
		var runFailedUnits int
		if err := pool.QueryRow(ctx, `SELECT status, failed_units FROM sync_runs WHERE id=$1`, discoveryTestRun).Scan(&runStatus, &runFailedUnits); err != nil {
			t.Fatal(err)
		}
		if runStatus == syncRunStatusFailed {
			t.Fatal("run status must NOT be failed yet -- an active (running) unit means the run isn't terminal")
		}
		// CHAOS-4586: the run stays active (the sibling unit is still
		// RUNNING), so without the rollup fix failed_units stayed 0 here no
		// matter how many units this branch stranded and failed.
		if runFailedUnits != 1 {
			t.Fatalf("sync_runs.failed_units=%d, want 1 (CHAOS-4586: denyRun's active-units branch must recompute the rollup, not leave it stale until finalize)", runFailedUnits)
		}
		var finalizeCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
			discoveryTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeCount); err != nil {
			t.Fatal(err)
		}
		if finalizeCount != 1 {
			t.Fatalf("got %d finalize wakeups, want 1", finalizeCount)
		}
	})
}

// TestDispatchDeniesWithActiveUnitsRecordsTheRollupBumpMetric pins codex
// round 10's P2: the existing Dispatch-path integration tests (this file)
// never construct their service WITH metrics wired
// (newTestDispatchService never calls .WithMetrics), and the pure unit
// tests that DO exercise RecordSyncRunRollupBumped/the path vocabulary
// either compare constants or inject the metric call manually -- none of
// them go through a REAL Dispatch() call end to end. Removing Dispatch's
// post-commit flushRollupBumpTally call would leave every one of those
// tests green while dev_health_sync_run_rollup_bumped_total stayed at
// zero for all five Dispatch-path values in production. This is the SAME
// scenario as TestDispatchDeniesWithActiveUnitsFailsOnlyStrandedOnes, with
// a real *providerfoundation.Metrics wired via WithMetrics (mirroring
// cmd/dev-health-worker/sync_dispatch.go's own
// `dispatchSyncRun.WithMetrics(syncCoordinatorMetrics)` production call),
// asserting the counter via the SAME WritePrometheus a real /metrics
// scrape would render.
func TestDispatchDeniesWithActiveUnitsRecordsTheRollupBumpMetric(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		strandedUnit := "00000000-0000-4000-8000-0000000000fb"
		runningUnit := "00000000-0000-4000-8000-0000000000fc"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$5),
       ($4,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','running',$5)`,
			strandedUnit, discoveryTestOrg, discoveryTestRun, runningUnit, now); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SYNC_RUN_MAX_UNITS", "1")

		service := newTestDispatchService(t, pool)
		metrics := providerfoundation.NewMetrics()
		service.WithMetrics(metrics)

		// dev_health_sync_run_rollup_bumped_total is a process-wide
		// singleton (CHAOS-4586, codex round 1): other integration tests
		// in this package's shared test binary bump the SAME series, so a
		// before/after delta is the only assertion that is not sensitive
		// to test execution order -- same technique as
		// providersync's repository_postgres_metrics_integration_test.go.
		before := rollupBumpedCount(t, "denied")

		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		if got, want := rollupBumpedCount(t, "denied"), before+1; got != want {
			t.Fatalf("dev_health_sync_run_rollup_bumped_total{outcome=\"failed\",path=\"denied\"} = %d after a real "+
				"Dispatch() denial, want %d (before=%d) -- WithMetrics is wired but flushRollupBumpTally never ran "+
				"(codex round 10, CHAOS-4586)", got, want, before)
		}
	})
}

// rollupBumpedCount reads the current value of the process-wide
// dev_health_sync_run_rollup_bumped_total{outcome="failed",path=<path>}
// series directly off the real singleton every Dispatch/reference-discovery
// call in this binary shares (never off the per-instance metrics a test's
// own service.WithMetrics wires, which never renders this metric name at
// all post-CHAOS-4586 -- see providerfoundation.SyncRunRollupBumpedMetricsSource's
// own doc comment). Missing reads as 0, matching Prometheus's own
// "absent == zero" convention.
func rollupBumpedCount(t *testing.T, path string) uint64 {
	t.Helper()
	var rendered bytes.Buffer
	if err := providerfoundation.SyncRunRollupBumpedMetricsSource().WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf(`dev_health_sync_run_rollup_bumped_total{outcome="failed",path=%q} `, path)
	for _, line := range strings.Split(rendered.String(), "\n") {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			var count uint64
			if _, err := fmt.Sscanf(strings.TrimSpace(value), "%d", &count); err != nil {
				t.Fatalf("parse counter value from %q: %v", line, err)
			}
			return count
		}
	}
	return 0
}

// TestDispatchTerminalizesAPermanentlyOversizedUnitRecordsTheRollupBumpMetric
// is TestEnforceRunTerminalizesAnExhaustedPermanentMisfit's fixture
// (budget_enforce_integration_test.go), adapted to go through a REAL
// Dispatch() call end to end (that test calls enforceRun directly, which
// never installs withRollupBumpTally -- only Dispatch/denyRun/handleFailure
// do), with real metrics wired (codex round 10, P2). newTestDispatchService
// hard-codes an empty *fakeBudgetEstimator, so this constructs the service
// manually with one seeded to make this unit's own estimate alone exceed
// its bucket's limit -- a permanent, not transient, misfit.
func TestDispatchTerminalizesAPermanentlyOversizedUnitRecordsTheRollupBumpMetric(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
		// The host shell may have SYNC_BUDGET_BUCKET_LIMITS set for local
		// dev convenience (a real per-bucket override) -- neutralize it so
		// this test's math is deterministic regardless of the ambient
		// environment (same as TestEnforceRunTerminalizesAnExhaustedPermanentMisfit).
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000ff"
		// CHAOS-4556 (landed on main, picked up by this rebase): Dispatch()
		// now validates every dispatch-eligible unit's provider-family claim
		// BEFORE budget capacity is ever computed (invalidClaimsAmongDispatchCandidates,
		// "phase=pre_capacity") -- a github/work-items claim is
		// AtomicCanonical and requires ALL FIVE family_dataset_* flags set,
		// or ValidateClaim rejects it as invalid_claim before this test's own
		// oversized-estimate path is ever reached.
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,processor_flags,source_id,status,updated_at,budget_deferrals,result)
VALUES ($1,$2,$3,'github','work-items',
        '{"family_dataset_work_items":true,"family_dataset_work_item_labels":true,"family_dataset_work_item_projects":true,"family_dataset_work_item_history":true,"family_dataset_work_item_comments":true}'::json,
        '00000000-0000-4000-8000-0000000000ed','planned',$4,$5,'{"error_category":"budget_deferred"}'::json)`,
			unitID, discoveryTestOrg, discoveryTestRun, now, budgetMaxDeferrals()); err != nil {
			t.Fatal(err)
		}

		registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{
			jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
		}}
		producer, err := joboutbox.NewProducer(pool, registry)
		if err != nil {
			t.Fatalf("joboutbox.NewProducer: %v", err)
		}
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitID: estimateFor(500, "github", "rest_core", "work-items"), // alone exceeds the 100 limit -- permanent
		}}
		service, err := NewNativeDispatchSyncRunService(pool, nil, estimator, producer, registry)
		if err != nil {
			t.Fatalf("NewNativeDispatchSyncRunService: %v", err)
		}
		metrics := providerfoundation.NewMetrics()
		service.WithMetrics(metrics)
		before := rollupBumpedCount(t, "budget_exhausted")

		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		var unitStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusFailed {
			t.Fatalf("unit status=%q, want failed (permanently oversized -- exhausted, not deferred again)", unitStatus)
		}
		if got, want := rollupBumpedCount(t, "budget_exhausted"), before+1; got != want {
			t.Fatalf("dev_health_sync_run_rollup_bumped_total{outcome=\"failed\",path=\"budget_exhausted\"} = %d after a "+
				"real Dispatch() budget-exhaustion termination, want %d (before=%d) -- WithMetrics is wired but "+
				"flushRollupBumpTally never ran (codex round 10, CHAOS-4586)", got, want, before)
		}
	})
}

// TestDispatchTerminalizesAFeatureDisabledRunRecordsTheRollupBumpMetric is
// TestNativeReferenceDiscoveryTerminalizesRunWhenFeatureDisabled's fixture
// shape (domain_role_statement_privileges_integration_test.go's
// feature_flags+org_feature_overrides seeding, same technique), adapted to
// reach Dispatch()'s OWN feature-gate-denial branch (terminalizeFeatureDisabled
// in native_dispatch_sync_run_service.go, called before the reference-
// discovery gate even runs) with real metrics wired. This is the codex
// round 10 P1 fix's own dedicated metric test: round 10 fixed
// terminalizeFeatureDisabled's missing flushRollupBumpTally call after its
// own tx.Commit, and fixed the mislabeled "already correct" registry
// exemption on terminalizeFeatureDisabledRun -- neither fix had an
// end-to-end metric assertion of its own until now (only the concurrency
// test TestTerminalizeFeatureDisabledRunWaitsForAConcurrentRollupWriterThenSeesItsResult,
// which proves the rollup WRITE is lock-protected, not that the counter
// increments).
func TestDispatchTerminalizesAFeatureDisabledRunRecordsTheRollupBumpMetric(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		featureID := "00000000-0000-4000-8000-0000000000e7"
		unitID := "00000000-0000-4000-8000-0000000000e8"
		for _, statement := range []string{
			`INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
			 VALUES ('` + unitID + `','` + discoveryTestOrg + `','` + discoveryTestRun + `','pagerduty','incidents',
			         '00000000-0000-4000-8000-0000000000ec','planned',now())`,
			`INSERT INTO feature_flags (id,key,min_tier,is_enabled)
			 VALUES ('` + featureID + `','canonical_incident_ingestion','enterprise',true)`,
			`INSERT INTO org_feature_overrides (id,org_id,feature_id,is_enabled,expires_at)
			 VALUES ('00000000-0000-4000-8000-0000000000ee','` + discoveryTestOrg + `','` + featureID + `',false,NULL)`,
		} {
			if _, err := pool.Exec(ctx, statement); err != nil {
				t.Fatalf("seed %s: %v", statement, err)
			}
		}

		service := newTestDispatchService(t, pool)
		metrics := providerfoundation.NewMetrics()
		service.WithMetrics(metrics)
		before := rollupBumpedCount(t, "feature_disabled")

		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		if got, want := rollupBumpedCount(t, "feature_disabled"), before+1; got != want {
			t.Fatalf("dev_health_sync_run_rollup_bumped_total{outcome=\"failed\",path=\"feature_disabled\"} = %d "+
				"after a real Dispatch() feature-gate denial, want %d (before=%d) -- WithMetrics is wired but "+
				"terminalizeFeatureDisabled's own flushRollupBumpTally never ran (codex round 10, CHAOS-4586)",
				got, want, before)
		}

		var unitStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusFailed {
			t.Fatalf("unit status=%q, want failed -- the feature-disabled path did not terminalize", unitStatus)
		}
		var runStatus string
		var failedUnits, completedUnits int
		if err := pool.QueryRow(ctx, `SELECT status, failed_units, completed_units FROM sync_runs WHERE id=$1`,
			discoveryTestRun).Scan(&runStatus, &failedUnits, &completedUnits); err != nil {
			t.Fatal(err)
		}
		if runStatus != syncRunStatusFailed || failedUnits != 1 || completedUnits != 0 {
			t.Fatalf("run status=%q failed_units=%d completed_units=%d, want failed/1/0",
				runStatus, failedUnits, completedUnits)
		}
	})
}

// TestDispatchDeniesWithNoActiveUnitsFailsTheWholeRun pins the total-cap
// hard-deny branch's SECOND shape: with no active units, the whole run is
// marked FAILED immediately (a redispatch would just be re-denied forever,
// so leaving it PLANNED would strand it invisibly to the reconciler).
func TestDispatchDeniesWithNoActiveUnitsFailsTheWholeRun(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitA := "00000000-0000-4000-8000-0000000000ee"
		unitB := "00000000-0000-4000-8000-0000000000ef"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$5),
       ($4,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$5)`,
			unitA, discoveryTestOrg, discoveryTestRun, unitB, now); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SYNC_RUN_MAX_UNITS", "1")

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		var runStatus string
		var completedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT status, completed_at FROM sync_runs WHERE id=$1`, discoveryTestRun).
			Scan(&runStatus, &completedAt); err != nil {
			t.Fatal(err)
		}
		if runStatus != syncRunStatusFailed || completedAt == nil {
			t.Fatalf("runStatus=%q completedAt=%v, want failed/non-nil", runStatus, completedAt)
		}
		var unitAStatus, unitBStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitA).Scan(&unitAStatus); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitB).Scan(&unitBStatus); err != nil {
			t.Fatal(err)
		}
		if unitAStatus != syncRunUnitStatusFailed || unitBStatus != syncRunUnitStatusFailed {
			t.Fatalf("unitA=%q unitB=%q, want both failed", unitAStatus, unitBStatus)
		}
	})
}

// TestDispatchEnqueuesARoutableUnitAndMarksTheRunDispatching pins the
// whole continuation's happy path end to end: a claimable, routable unit
// (github/commits, confirmed route-ready and plannable) gets published
// into worker_job_outbox, the run moves to DISPATCHING with started_at
// set, and Dispatch() returns nil -- "dispatched", not the not-yet-
// implemented stop, since river_queued > 0.
func TestDispatchEnqueuesARoutableUnitAndMarksTheRunDispatching(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000f1"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (successful dispatch)", err)
		}

		var outboxCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind=$1`, jobcontract.KindSyncProviderUnit).
			Scan(&outboxCount); err != nil {
			t.Fatal(err)
		}
		if outboxCount != 1 {
			t.Fatalf("got %d worker_job_outbox rows, want 1", outboxCount)
		}

		var runStatus string
		var startedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT status, started_at FROM sync_runs WHERE id=$1`, discoveryTestRun).
			Scan(&runStatus, &startedAt); err != nil {
			t.Fatal(err)
		}
		if runStatus != syncRunStatusDispatching || startedAt == nil {
			t.Fatalf("runStatus=%q startedAt=%v, want dispatching/non-nil", runStatus, startedAt)
		}
	})
}

// TestDispatchTerminalizesAnUnroutableUnitAndArmsFinalize pins the
// unroutable-unit path wired into Dispatch() all the way through the
// tail: a claimed unit whose (provider, dataset) pair the capability
// matrix does not route gets terminalized (CHAOS-3990), never enqueued --
// river_queued stays 0, and since terminalizing it leaves NOTHING pending
// (no other unit in the run), Dispatch() reaches the tail's case (d) and
// arms a finalize wakeup, returning nil ("noop_finalize"), not
// "dispatched".
func TestDispatchTerminalizesAnUnroutableUnitAndArmsFinalize(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000f2"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'unknown-provider','unknown-dataset','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (noop_finalize)", err)
		}

		var unitStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusFailed {
			t.Fatalf("unit status=%q, want failed (terminalized as unroutable)", unitStatus)
		}
		// CHAOS-4586: terminalizeUnroutableUnits must recompute sync_runs'
		// rollup, not leave it stale until finalize.
		var runFailedUnits int
		if err := pool.QueryRow(ctx, `SELECT failed_units FROM sync_runs WHERE id=$1`, discoveryTestRun).Scan(&runFailedUnits); err != nil {
			t.Fatal(err)
		}
		if runFailedUnits != 1 {
			t.Fatalf("sync_runs.failed_units=%d, want 1", runFailedUnits)
		}
		var outboxCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox`).Scan(&outboxCount); err != nil {
			t.Fatal(err)
		}
		if outboxCount != 0 {
			t.Fatalf("got %d worker_job_outbox rows, want 0 -- an unroutable unit must never be published", outboxCount)
		}
		var finalizeWakeups int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
			discoveryTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeWakeups); err != nil {
			t.Fatal(err)
		}
		if finalizeWakeups != 1 {
			t.Fatalf("got %d finalize wakeups, want 1 -- nothing left pending, so finalize must be armed", finalizeWakeups)
		}
	})
}

// TestDispatchTerminalizesAnUnroutableUnitRecordsTheRollupBumpMetric is
// TestDispatchTerminalizesAnUnroutableUnitAndArmsFinalize's fixture, with
// real metrics wired (codex round 10, P2: no existing Dispatch-path test
// wires WithMetrics -- see TestDispatchDeniesWithActiveUnitsRecordsTheRollupBumpMetric's
// own doc comment for the full rationale and the process-wide-singleton
// delta-assertion technique).
func TestDispatchTerminalizesAnUnroutableUnitRecordsTheRollupBumpMetric(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000fd"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'unknown-provider','unknown-dataset','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		metrics := providerfoundation.NewMetrics()
		service.WithMetrics(metrics)
		before := rollupBumpedCount(t, "unroutable")

		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		if got, want := rollupBumpedCount(t, "unroutable"), before+1; got != want {
			t.Fatalf("dev_health_sync_run_rollup_bumped_total{outcome=\"failed\",path=\"unroutable\"} = %d after a real "+
				"Dispatch() unroutable termination, want %d (before=%d) -- WithMetrics is wired but flushRollupBumpTally "+
				"never ran (codex round 10, CHAOS-4586)", got, want, before)
		}
	})
}

// TestDispatchTerminalizesANonCanonicalAtomicFamilyAliasAndArmsFinalize pins
// ValidateClaim's own wiring into the per-unit loop, and an important
// empirical finding from trying to build the RouteReady-without-Plannable
// fixture directly: github's work-item-labels dataset IS RouteReady-but-
// not-Plannable in providersync's capability matrix (the whole atomic
// family is marked native/ready so producer/matrix drift cannot re-open
// one partial alias; only the canonical work-items claim is Plannable),
// but that combination is UNREACHABLE at the routability check in
// practice: ValidateClaim (which runs FIRST, matching Python's own
// validate-before-route order) already refuses any non-canonical atomic-
// family alias outright, for every family currently declared.
//
// CHAOS-4550: this used to prove a DIFFERENT, buggy guard -- a malformed
// persisted claim aborting the WHOLE pass (the deferred tx.Rollback undid
// claimUnits' own claim, leaving the unit exactly as it was, not
// half-claimed). That is exactly the discard-storm class CHAOS-3990 exists
// to prevent: aborting Dispatch() here means the SAME unit is reclaimed and
// refused again next redispatch, forever (live: sync_run f02b38f3, 548+
// delivery attempts / 17 days). This test now pins the fixed behavior: the
// offending unit is terminalized (failed, with a durable reason) and
// Dispatch proceeds to arm finalize, same as the sibling unroutable-unit
// case below.
func TestDispatchTerminalizesANonCanonicalAtomicFamilyAliasAndArmsFinalize(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000f4"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','work-item-labels','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (noop_finalize)", err)
		}

		var unitStatus, errorCategory string
		if err := pool.QueryRow(ctx,
			`SELECT status, result->>'error_category' FROM sync_run_units WHERE id=$1`, unitID,
		).Scan(&unitStatus, &errorCategory); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusFailed {
			t.Fatalf("unit status=%q, want failed (terminalized, not rolled back to planned)", unitStatus)
		}
		if errorCategory != invalidProviderFamilyClaimErrorCategory {
			t.Fatalf("error_category=%q, want %q", errorCategory, invalidProviderFamilyClaimErrorCategory)
		}
		// CHAOS-4586: terminalizeInvalidClaimUnits must recompute sync_runs'
		// rollup, not leave it stale until finalize.
		var runFailedUnits int
		if err := pool.QueryRow(ctx, `SELECT failed_units FROM sync_runs WHERE id=$1`, discoveryTestRun).Scan(&runFailedUnits); err != nil {
			t.Fatal(err)
		}
		if runFailedUnits != 1 {
			t.Fatalf("sync_runs.failed_units=%d, want 1", runFailedUnits)
		}
		var finalizeWakeups int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
			discoveryTestRun, outboxKindFinalizeSyncRun).Scan(&finalizeWakeups); err != nil {
			t.Fatal(err)
		}
		if finalizeWakeups != 1 {
			t.Fatalf("got %d finalize wakeups, want 1 -- nothing left pending, so finalize must be armed", finalizeWakeups)
		}
	})
}

// TestDispatchTerminalizesANonCanonicalAtomicFamilyAliasRecordsTheRollupBumpMetric
// is TestDispatchTerminalizesANonCanonicalAtomicFamilyAliasAndArmsFinalize's
// fixture, with real metrics wired (codex round 10, P2).
func TestDispatchTerminalizesANonCanonicalAtomicFamilyAliasRecordsTheRollupBumpMetric(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000fe"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','work-item-labels','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		metrics := providerfoundation.NewMetrics()
		service.WithMetrics(metrics)
		before := rollupBumpedCount(t, "invalid_claim")

		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		if got, want := rollupBumpedCount(t, "invalid_claim"), before+1; got != want {
			t.Fatalf("dev_health_sync_run_rollup_bumped_total{outcome=\"failed\",path=\"invalid_claim\"} = %d after a real "+
				"Dispatch() invalid-claim termination, want %d (before=%d) -- WithMetrics is wired but "+
				"flushRollupBumpTally never ran (codex round 10, CHAOS-4586)", got, want, before)
		}
	})
}

// TestDispatchTerminalizesAnAtomicCanonicalClaimMissingFamilyFlags
// reproduces CHAOS-4550's live discard storm exactly: a persisted work-items
// claim carrying ONLY its own family_dataset_work_items flag, missing the
// four sibling family_dataset_* flags workitemcontract.FamilyDatasets()
// requires for an ATOMIC canonical claim (work-item-labels, work-item-
// projects, work-item-history, work-item-comments). This is the shape found
// live on org 70d529e0 (sync_run f02b38f3-4018-4029-8fed-8aa8f0a0264d,
// created 2026-08-12, predating the current 5-flag contract): a stale unit,
// not a missing route. Before the fix, ValidateClaim's failure aborted the
// WHOLE Dispatch pass and the unit reverted to planned; it must now be
// terminalized like any other malformed claim.
func TestDispatchTerminalizesAnAtomicCanonicalClaimMissingFamilyFlags(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000f5"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,processor_flags,source_id,status,updated_at)
VALUES ($1,$2,$3,'linear','work-items','{"family_dataset_work_items": true}'::json,'00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil (noop_finalize)", err)
		}

		var unitStatus, errorCategory string
		if err := pool.QueryRow(ctx,
			`SELECT status, result->>'error_category' FROM sync_run_units WHERE id=$1`, unitID,
		).Scan(&unitStatus, &errorCategory); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusFailed {
			t.Fatalf("unit status=%q, want failed -- a stale claim missing sibling family flags must be terminalized, not re-planned forever", unitStatus)
		}
		if errorCategory != invalidProviderFamilyClaimErrorCategory {
			t.Fatalf("error_category=%q, want %q", errorCategory, invalidProviderFamilyClaimErrorCategory)
		}
		// CHAOS-4586: terminalizeInvalidClaimUnits must recompute sync_runs'
		// rollup, not leave it stale until finalize.
		var runFailedUnits int
		if err := pool.QueryRow(ctx, `SELECT failed_units FROM sync_runs WHERE id=$1`, discoveryTestRun).Scan(&runFailedUnits); err != nil {
			t.Fatal(err)
		}
		if runFailedUnits != 1 {
			t.Fatalf("sync_runs.failed_units=%d, want 1", runFailedUnits)
		}

		// The discard-storm regression check: a SECOND dispatch pass must
		// find nothing left to claim for this unit (it is terminal), not
		// reclaim and re-refuse it again.
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("second Dispatch: %v, want nil", err)
		}
		var statusAfterSecondPass string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&statusAfterSecondPass); err != nil {
			t.Fatal(err)
		}
		if statusAfterSecondPass != syncRunUnitStatusFailed {
			t.Fatalf("unit status after second Dispatch=%q, want failed (still terminal, never reclaimed)", statusAfterSecondPass)
		}
	})
}

// TestDispatchTerminalizesAnInvalidClaimEvenWhenConcurrencyCapped is the
// CHAOS-4556 reproduction (this ticket was reopened once already: d098ef1f5
// fixed CHAOS-4550's terminal-vs-abort defect but left THIS ordering gap,
// confirmed by reading claim_units.go:71,88,103 before writing this test).
// Two PLANNED units share one dispatch bucket (org, provider=github,
// cost_class=default): a VALID github/commits unit (ValidateClaim always
// passes it -- "commits" is not a family-policy dataset) and, sorted after
// it by id, a MALFORMED github/work-items unit missing its atomic family's
// sibling family_dataset_* flags (same defect shape as the CHAOS-4550 live
// incident). SYNC_UNIT_CONCURRENCY_PER_BUCKET=1 forces authorizeRun to cap
// exactly one of the two -- the second by id, i.e. the malformed one --
// BEFORE claimUnits or the per-claimed-unit ValidateClaim loop ever run.
//
// On origin/main before the CHAOS-4556 fix, cappedUnitIDs (built entirely
// from concurrency/budget bookkeeping) excludes the malformed unit from all
// three of claimUnits' claim statements, so it is never claimed and
// therefore never reaches ValidateClaim: it stays PLANNED, not failed, and
// no invalid_provider_family_claim error_category is ever recorded --
// exactly the defect this ticket names ("delaying termination", "occupying
// a counted candidate slot it can never fill"). The fix validates every
// dispatch-eligible unit BEFORE authorizeRun runs, so the malformed unit is
// terminalized on this very pass regardless of the concurrency cap, and the
// valid unit (which does fit in the one available slot) is still claimed
// and queued.
func TestDispatchTerminalizesAnInvalidClaimEvenWhenConcurrencyCapped(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		validUnit := "00000000-0000-4000-8000-0000000000f6"
		malformedUnit := "00000000-0000-4000-8000-0000000000f7"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			validUnit, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,processor_flags,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','work-items','{"family_dataset_work_items": true}'::json,'00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			malformedUnit, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}
		// Both units share (org, provider=github, cost_class=DEFAULT 'standard')
		// -- the same dispatchBucket -- and this cap admits only one of them.
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		var malformedStatus string
		var errorCategory *string
		if err := pool.QueryRow(ctx,
			`SELECT status, result->>'error_category' FROM sync_run_units WHERE id=$1`, malformedUnit,
		).Scan(&malformedStatus, &errorCategory); err != nil {
			t.Fatal(err)
		}
		if malformedStatus != syncRunUnitStatusFailed {
			t.Fatalf("malformed unit status=%q, want failed -- a malformed claim must terminalize even when the concurrency cap defers it, not sit PLANNED under sustained capacity pressure", malformedStatus)
		}
		if errorCategory == nil || *errorCategory != invalidProviderFamilyClaimErrorCategory {
			t.Fatalf("malformed unit error_category=%v, want %q", errorCategory, invalidProviderFamilyClaimErrorCategory)
		}

		var validStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, validUnit).Scan(&validStatus); err != nil {
			t.Fatal(err)
		}
		if validStatus != syncRunUnitStatusDispatching {
			t.Fatalf("valid unit status=%q, want dispatching -- it should still take the one available concurrency slot and be claimed/queued", validStatus)
		}
		var outboxCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind=$1`, jobcontract.KindSyncProviderUnit).
			Scan(&outboxCount); err != nil {
			t.Fatal(err)
		}
		if outboxCount != 1 {
			t.Fatalf("got %d worker_job_outbox rows, want 1 (only the valid unit)", outboxCount)
		}
	})
}

// TestDispatchFailsClosedWhenTheProviderUnitKindIsNotCheckedInAsExecutable
// pins the ONLY route-shaped fail-closed check left on Dispatch's write path
// after the CHAOS-4175 ruling reversal (see NativeDispatchSyncRunService's
// doc comment): there is no jobroute.Controller check here anymore, but
// service.producer.Publish still refuses via its own descriptorAllowsPublish
// gate (joboutbox.ErrPolicyRejected) when the REGISTRY's checked-in
// descriptor for sync.provider_unit does not declare an executable route
// (river/river_canary/shadow) -- confirmed by reading
// joboutbox.descriptorAllowsPublish before rewriting this test, not assumed.
// A celery-declared descriptor must still fail closed and roll back the
// claim, just via a different error than before.
func TestDispatchFailsClosedWhenTheProviderUnitKindIsNotCheckedInAsExecutable(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000f3"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchServiceWith(t, pool, &fakeJobRegistry{
			descriptors: map[string]jobruntime.Descriptor{
				jobcontract.KindSyncProviderUnit: providerUnitDescriptor("celery"),
			},
		})
		err := service.Dispatch(ctx, dispatchTestArgs())
		if !errors.Is(err, joboutbox.ErrPolicyRejected) {
			t.Fatalf("Dispatch: %v, want joboutbox.ErrPolicyRejected", err)
		}

		var unitStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
			t.Fatal(err)
		}
		if unitStatus != syncRunUnitStatusPlanned {
			t.Fatalf("unit status=%q, want unchanged (planned) -- fail-closed must not claim anything", unitStatus)
		}
	})
}

const (
	// A second run in the SAME org (discoveryTestOrg) as discoveryTestRun,
	// on a different provider (jira instead of github), for
	// TestDispatchIsolatesReferenceDiscoveryPerRunAcrossProviders below.
	isolationTestJiraRun         = "00000000-0000-4000-8000-0000000000c1"
	isolationTestJiraOutbox      = "00000000-0000-4000-8000-0000000000c2"
	isolationTestJiraIntegration = "00000000-0000-4000-8000-0000000000c3"
)

func isolationJiraDispatchArgs() DispatchSyncRunArgs {
	return DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: isolationTestJiraRun,
		DispatchOutbox: isolationTestJiraOutbox, RouteGeneration: 1,
	}}
}

// TestDispatchIsolatesReferenceDiscoveryPerRunAcrossProviders is CHAOS-4357's
// scope-item-1 readback: reference_discovery is gated per sync_run_id
// (referenceDiscoverySucceeded queries WHERE sync_run_id = $1, and
// sync_run_reference_discoveries.sync_run_id is UNIQUE), and
// run_reference_discovery_populate_strict resolves context["provider"] from
// the RUN'S OWN integration -- there is no code path where one provider's
// team-autoimport failure is consulted while dispatching a different
// provider's run. This test proves that isolation holds at the one place
// that actually matters operationally: Dispatch() itself. Two runs share an
// org (discoveryTestOrg): discoveryTestRun (github) has a SUCCEEDED
// discovery and a claimable unit; isolationTestJiraRun (jira) has a
// permanently RETRYING discovery, mirroring the org-70d529e0 incident
// exactly. Dispatching the jira run must block (same as
// TestDispatchBlocksOnReferenceDiscoveryAndArmsAWakeup); dispatching the
// github run in the SAME org, in the SAME test, must proceed to actually
// claim and queue its unit -- completely unaffected by the jira run's
// failing discovery.
func TestDispatchIsolatesReferenceDiscoveryPerRunAcrossProviders(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		// --- github run: seeded to succeed ---
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		unitID := "00000000-0000-4000-8000-0000000000c4"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			unitID, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}

		// --- jira run: same org, own outbox row (dispatch_sync_run route is
		// keyed by kind only, so seedDispatchRoute's route row already covers
		// this run too), own integration, discovery permanently 'retrying'
		// (the exact shape handleFailure leaves a still-retryable, not-yet-
		// exhausted failure in -- e.g. the Jira board 400 from scope item 2,
		// before this run's own retry has had a chance to succeed).
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_dispatch_outbox (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
VALUES ($1,$2,$3,'dispatch_sync_run','dispatched',now(),'river',1,now(),now())`,
			isolationTestJiraOutbox, isolationTestJiraRun, discoveryTestOrg); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO sync_runs (id,org_id,integration_id) VALUES ($1,$2,$3)`,
			isolationTestJiraRun, discoveryTestOrg, isolationTestJiraIntegration); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO integrations (id,org_id,provider) VALUES ($1,$2,'jira')`,
			isolationTestJiraIntegration, discoveryTestOrg); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,error)
VALUES ('00000000-0000-4000-8000-0000000000c5',$1,$2,$3,1,now(),'Reference discovery failed')`,
			isolationTestJiraRun, discoveryTestOrg, discoveryStatusRetrying); err != nil {
			t.Fatal(err)
		}

		service := newTestDispatchService(t, pool)

		// Dispatch the FAILING jira run first -- it must block, not error.
		if err := service.Dispatch(ctx, isolationJiraDispatchArgs()); err != nil {
			t.Fatalf("Dispatch(jira): %v, want nil", err)
		}
		// Dispatch()'s blocked_on_reference_discovery branch never writes
		// sync_runs.status at all (it only arms the wakeup and commits) --
		// this test's fixture schema defaults a freshly inserted run to
		// 'dispatching' (see CREATE TABLE sync_runs above), so "unchanged"
		// here means it must NOT have become 'failed' or 'success'.
		var jiraRunStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id=$1`, isolationTestJiraRun).Scan(&jiraRunStatus); err != nil {
			t.Fatal(err)
		}
		if jiraRunStatus == syncRunStatusFailed {
			t.Fatalf("jira run status=%q, want non-terminal (blocked on its own reference discovery, not failed)", jiraRunStatus)
		}
		var jiraWakeupCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='reference_discovery'`,
			isolationTestJiraRun).Scan(&jiraWakeupCount); err != nil {
			t.Fatal(err)
		}
		if jiraWakeupCount != 1 {
			t.Fatalf("got %d jira reference_discovery wakeups, want 1", jiraWakeupCount)
		}

		// Dispatch the github run in the SAME org -- must proceed and claim
		// its unit, completely unaffected by the jira run above.
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch(github): %v, want nil (successful dispatch)", err)
		}
		var githubRunStatus string
		var startedAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT status, started_at FROM sync_runs WHERE id=$1`, discoveryTestRun).
			Scan(&githubRunStatus, &startedAt); err != nil {
			t.Fatal(err)
		}
		if githubRunStatus != syncRunStatusDispatching || startedAt == nil {
			t.Fatalf("github run status=%q startedAt=%v, want dispatching/non-nil -- a failing jira discovery in the same org must not block it", githubRunStatus, startedAt)
		}
		var githubOutboxCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind=$1`, jobcontract.KindSyncProviderUnit).
			Scan(&githubOutboxCount); err != nil {
			t.Fatal(err)
		}
		if githubOutboxCount != 1 {
			t.Fatalf("got %d worker_job_outbox rows for the github unit, want 1", githubOutboxCount)
		}
	})
}

// TestDispatchTailPreservesAnEarlierDeferralWhenWorkIsStillDispatchable pins
// the riverQueued == 0 tail's branch (a) at the CALL SITE, which is where the
// defect lives -- a unit test on dueNowRearmAt cannot catch a caller that
// passes nil, and passing nil is exactly what this branch used to do.
//
// Found by codex round 2 (P2) reviewing CHAOS-4605's own change. It is NOT a
// regression this ticket introduced: the shape below needs no allow-list at
// all -- a bucket at cap leaves a PLANNED unit unclaimed and dispatchable on
// origin/main too, so this test fails on the parent commit. CHAOS-4605's
// allow-list widens the reach considerably, since leaving a unit dispatchable
// instead of claiming it is precisely what it does.
//
// The shape:
//   - a live RUNNING unit saturates the (org, github, standard) bucket at
//     SYNC_UNIT_CONCURRENCY_PER_BUCKET=1, so allowedSlots is 0;
//   - a PLANNED unit in that bucket is therefore capped -- unclaimed, still
//     dispatchable, and nothing is queued to River, which is what routes the
//     pass into the tail rather than the riverQueued > 0 arms;
//   - a RETRYING unit deferred to a time WELL INSIDE the 60 s countdown
//     supplies counts.nextDeferredAt.
//
// scheduleRedispatch writes ONE wakeup and its second statement overwrites a
// pending row unconditionally, so arming the bare countdown here does not
// merge with the deferral -- it destroys it.
func TestDispatchTailPreservesAnEarlierDeferralWhenWorkIsStillDispatchable(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()

		// Comfortably inside the 60s countdown, and comfortably in the future
		// so it cannot come due while the containerized pass runs.
		deferredUntil := now.Add(20 * time.Second)
		leaseUntil := now.Add(time.Hour)

		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,lease_owner,lease_expires_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','running',$4,'live-worker',$5)`,
			"00000000-0000-4000-8000-0000000000fa", discoveryTestOrg, discoveryTestRun, now, leaseUntil); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','planned',$4)`,
			"00000000-0000-4000-8000-0000000000fb", discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,available_at)
VALUES ($1,$2,$3,'github','commits','00000000-0000-4000-8000-0000000000ed','retrying',$4,$5)`,
			"00000000-0000-4000-8000-0000000000fc", discoveryTestOrg, discoveryTestRun, now, deferredUntil); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")

		service := newTestDispatchService(t, pool)
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v, want nil", err)
		}

		// Precondition: the pass really did take the tail, not a
		// riverQueued > 0 arm. Without this the assertion below could pass
		// for the wrong reason.
		var queued int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind=$1`, jobcontract.KindSyncProviderUnit).
			Scan(&queued); err != nil {
			t.Fatal(err)
		}
		if queued != 0 {
			t.Fatalf("got %d queued provider-unit jobs, want 0 -- the bucket is at cap, so this pass must reach the riverQueued == 0 tail", queued)
		}
		var plannedStatus string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`,
			"00000000-0000-4000-8000-0000000000fb").Scan(&plannedStatus); err != nil {
			t.Fatal(err)
		}
		if plannedStatus != syncRunUnitStatusPlanned {
			t.Fatalf("capped unit status=%q want=planned -- it must stay dispatchable, which is what selects tail branch (a)", plannedStatus)
		}

		got := dispatchOutboxAvailableAt(t, ctx, pool, discoveryTestRun)
		if !got.Equal(deferredUntil) {
			t.Fatalf("redispatch available_at=%s want=%s -- branch (a) armed the bare countdown and destroyed the earlier deferral; scheduleRedispatch overwrites a pending row unconditionally, so it does not merge", got, deferredUntil)
		}
	})
}
