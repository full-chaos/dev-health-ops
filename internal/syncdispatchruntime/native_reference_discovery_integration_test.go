//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createReferenceDiscoveryTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE sync_dispatch_transport_routes (
 kind text PRIMARY KEY, transport text NOT NULL, generation bigint NOT NULL,
 paused boolean NOT NULL, rollback_transport text NOT NULL
);
CREATE TABLE sync_dispatch_outbox (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, org_id text NOT NULL, kind text NOT NULL,
 status text NOT NULL, available_at timestamptz NOT NULL, attempts int NOT NULL DEFAULT 0,
 dispatched_at timestamptz NULL, dispatched_transport text NULL, dispatched_route_generation bigint NULL,
 transport_job_id text NULL, claim_token text NULL, claim_transport text NULL,
 claim_route_generation bigint NULL, claim_expires_at timestamptz NULL, last_error text NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 UNIQUE (sync_run_id, kind)
);
CREATE TABLE sync_runs (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
 status text NOT NULL DEFAULT 'dispatching', total_units int NOT NULL DEFAULT 0,
 completed_units int NOT NULL DEFAULT 0, failed_units int NOT NULL DEFAULT 0,
 completed_at timestamptz NULL, result json NULL, error text NULL
);
CREATE TABLE sync_run_units (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, source_id uuid NOT NULL, status text NOT NULL,
 available_at timestamptz NULL, error text NULL, result json NULL, lease_owner text NULL, lease_expires_at timestamptz NULL,
 last_heartbeat_at timestamptz NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE integrations (
 id uuid PRIMARY KEY, provider text NOT NULL
);
CREATE TABLE integration_datasets (
 id uuid PRIMARY KEY, integration_id uuid NOT NULL, dataset_key text NOT NULL, is_enabled boolean NOT NULL
);
CREATE TABLE feature_flags (
 id uuid PRIMARY KEY, key text NOT NULL UNIQUE, min_tier text NOT NULL, is_enabled boolean NOT NULL
);
CREATE TABLE backfill_jobs (
 id uuid PRIMARY KEY, org_id text NOT NULL, celery_task_id text NULL, status text NOT NULL,
 total_chunks int NOT NULL DEFAULT 0, completed_chunks int NOT NULL DEFAULT 0,
 failed_chunks int NOT NULL DEFAULT 0, error_message text NULL, completed_at timestamptz NULL
);
CREATE TABLE scheduled_jobs (
 id uuid PRIMARY KEY
);
CREATE TABLE job_runs (
 id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES scheduled_jobs(id),
 status int NOT NULL, completed_at timestamptz NULL, result json NULL, error text NULL
);
CREATE TABLE sync_run_reference_discoveries (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL UNIQUE, org_id text NOT NULL,
 status text NOT NULL, attempts int NOT NULL DEFAULT 0, available_at timestamptz NOT NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL,
 completed_at timestamptz NULL, error text NULL, result json NULL,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const (
	discoveryTestOrg         = "00000000-0000-4000-8000-0000000000d1"
	discoveryTestRun         = "00000000-0000-4000-8000-0000000000d2"
	discoveryTestOutbox      = "00000000-0000-4000-8000-0000000000d3"
	discoveryTestIntegration = "00000000-0000-4000-8000-0000000000d4"
)

func seedDiscoveryRoute(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`INSERT INTO sync_dispatch_transport_routes (kind,transport,generation,paused,rollback_transport)
		 VALUES ('reference_discovery','river',1,false,'celery')`,
		`INSERT INTO sync_dispatch_outbox
		    (id,sync_run_id,org_id,kind,status,available_at,dispatched_transport,dispatched_route_generation,created_at,updated_at)
		 VALUES ('` + discoveryTestOutbox + `','` + discoveryTestRun + `','` + discoveryTestOrg + `',
		         'reference_discovery','dispatched',now(),'river',1,now(),now())`,
		`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ('` + discoveryTestRun + `','` +
			discoveryTestOrg + `','` + discoveryTestIntegration + `')`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func newDiscoveryArgs() ReferenceDiscoveryArgs {
	return ReferenceDiscoveryArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
		DispatchOutbox: discoveryTestOutbox, RouteGeneration: 1,
	}}
}

// fakeDiscoveryExecutor is a controllable DiscoveryExecutor test double,
// standing in for the not-yet-landed credential-resolution/ClickHouse-
// readback/populate-bridge chain: this test file's whole point is proving
// the lease/claim/heartbeat/retry state machine independent of that chain.
type fakeDiscoveryExecutor struct {
	summary map[string]any
	err     error
	calls   int
}

func (executor *fakeDiscoveryExecutor) Discover(ctx context.Context, runID string) (map[string]any, error) {
	executor.calls++
	return executor.summary, executor.err
}

type retryableDiscoveryError struct{ message string }

func (e retryableDiscoveryError) Error() string { return e.message }

func TestNativeReferenceDiscoverySucceedsAndArmsDispatch(t *testing.T) {
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
	seedDiscoveryRoute(t, ctx, pool)

	executor := &fakeDiscoveryExecutor{summary: map[string]any{"reference_team_keys": []string{"ENG"}}}
	service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Discover(ctx, newDiscoveryArgs()); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if executor.calls != 1 {
		t.Fatalf("executor called %d times, want 1", executor.calls)
	}

	var status string
	var attempts int
	var leaseOwner *string
	if err := pool.QueryRow(ctx, `SELECT status, attempts, lease_owner FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
		discoveryTestRun).Scan(&status, &attempts, &leaseOwner); err != nil {
		t.Fatal(err)
	}
	if status != discoveryStatusSuccess || attempts != 1 || leaseOwner != nil {
		t.Fatalf("status=%q attempts=%d leaseOwner=%v, want success/1/nil", status, attempts, leaseOwner)
	}

	var dispatchOutboxRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='dispatch_sync_run'`,
		discoveryTestRun).Scan(&dispatchOutboxRows); err != nil {
		t.Fatal(err)
	}
	if dispatchOutboxRows != 1 {
		t.Fatalf("dispatch_sync_run wakeup rows=%d want=1", dispatchOutboxRows)
	}
}

// TestNativeReferenceDiscoverySkipsAnAlreadyClaimedRun pins the claim race
// guard: a ledger row already RUNNING under a live lease must not be
// re-claimed by a second attempt, and the executor must never be called for
// it.
func TestNativeReferenceDiscoverySkipsAnAlreadyClaimedRun(t *testing.T) {
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
	seedDiscoveryRoute(t, ctx, pool)

	ledgerID := "00000000-0000-4000-8000-0000000000d5"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,lease_owner,lease_expires_at)
VALUES ($1,$2,$3,'running',1,now(),'someone-else',now() + interval '1 hour')`,
		ledgerID, discoveryTestRun, discoveryTestOrg); err != nil {
		t.Fatal(err)
	}

	executor := &fakeDiscoveryExecutor{summary: map[string]any{}}
	service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Discover(ctx, newDiscoveryArgs()); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if executor.calls != 0 {
		t.Fatalf("executor called %d times, want 0 (run is already claimed)", executor.calls)
	}
}

// TestNativeReferenceDiscoveryRetriesARetryableFailure pins the retry path:
// a retryable executor error moves the ledger to RETRYING with a future
// available_at, arms its own reference_discovery outbox wakeup, and leaves
// attempts incremented (not reset) so the next attempt still counts toward
// the max.
func TestNativeReferenceDiscoveryRetriesARetryableFailure(t *testing.T) {
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
	seedDiscoveryRoute(t, ctx, pool)

	executor := &fakeDiscoveryExecutor{err: retryableDiscoveryError{message: "rate limited, please retry"}}
	service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Discover(ctx, newDiscoveryArgs()); err != nil {
		t.Fatalf("Discover: %v", err)
	}

	var status string
	var attempts int
	var availableAt time.Time
	if err := pool.QueryRow(ctx, `SELECT status, attempts, available_at FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
		discoveryTestRun).Scan(&status, &attempts, &availableAt); err != nil {
		t.Fatal(err)
	}
	if status != discoveryStatusRetrying || attempts != 1 {
		t.Fatalf("status=%q attempts=%d, want retrying/1", status, attempts)
	}
	if !availableAt.After(time.Now()) {
		t.Fatalf("available_at=%s, want a future backoff time", availableAt)
	}

	var selfWakeupRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='reference_discovery'`,
		discoveryTestRun).Scan(&selfWakeupRows); err != nil {
		t.Fatal(err)
	}
	if selfWakeupRows != 1 {
		t.Fatalf("reference_discovery self-wakeup rows=%d want=1", selfWakeupRows)
	}

	// A finalize/dispatch wakeup must NOT have been armed for a retryable,
	// non-terminal failure.
	var otherWakeupRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind != 'reference_discovery'`,
		discoveryTestRun).Scan(&otherWakeupRows); err != nil {
		t.Fatal(err)
	}
	if otherWakeupRows != 0 {
		t.Fatalf("non-self wakeup rows=%d want=0 for a retryable failure", otherWakeupRows)
	}
}

// TestNativeReferenceDiscoveryTerminatesAfterExhaustingRetries pins the
// terminal path: once attempts reach the cap, a still-retryable error
// terminalizes the ledger FAILED, fails every nonterminal unit, stamps the
// run's error, and arms the finalize_sync_run wakeup instead of retrying
// again.
func TestNativeReferenceDiscoveryTerminatesAfterExhaustingRetries(t *testing.T) {
	t.Setenv("SYNC_REFERENCE_DISCOVERY_MAX_ATTEMPTS", "1")
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
	seedDiscoveryRoute(t, ctx, pool)
	unitID := "00000000-0000-4000-8000-0000000000d6"
	sourceID := "00000000-0000-4000-8000-0000000000d7"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status)
VALUES ($1,$2,$3,'github','commits',$4,'planned')`,
		unitID, discoveryTestOrg, discoveryTestRun, sourceID); err != nil {
		t.Fatal(err)
	}

	executor := &fakeDiscoveryExecutor{err: retryableDiscoveryError{message: "rate limited, please retry"}}
	service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	// Attempt 1: consumes the only allowed attempt, still retryable-looking,
	// but attempts (1) is no longer < max (1), so it must terminalize.
	if err := service.Discover(ctx, newDiscoveryArgs()); err != nil {
		t.Fatalf("Discover: %v", err)
	}

	var status string
	var runError *string
	if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
		discoveryTestRun).Scan(&status, &runError); err != nil {
		t.Fatal(err)
	}
	if status != discoveryStatusFailed {
		t.Fatalf("status=%q want=failed", status)
	}
	if runError == nil || *runError != referenceDiscoveryErrorMessage {
		t.Fatalf("ledger error=%v want=%q", runError, referenceDiscoveryErrorMessage)
	}

	var unitStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, unitID).Scan(&unitStatus); err != nil {
		t.Fatal(err)
	}
	if unitStatus != syncRunUnitStatusFailed {
		t.Fatalf("unit status=%q want=failed (must be terminalized alongside the run)", unitStatus)
	}

	var syncRunError *string
	if err := pool.QueryRow(ctx, `SELECT error FROM sync_runs WHERE id=$1`, discoveryTestRun).Scan(&syncRunError); err != nil {
		t.Fatal(err)
	}
	if syncRunError == nil || *syncRunError != referenceDiscoveryErrorMessage {
		t.Fatalf("sync_runs.error=%v want=%q", syncRunError, referenceDiscoveryErrorMessage)
	}

	var finalizeWakeupRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='finalize_sync_run'`,
		discoveryTestRun).Scan(&finalizeWakeupRows); err != nil {
		t.Fatal(err)
	}
	if finalizeWakeupRows != 1 {
		t.Fatalf("finalize_sync_run wakeup rows=%d want=1", finalizeWakeupRows)
	}
}

// TestNativeReferenceDiscoveryLostLeaseDoesNotOverwriteAWinner pins that a
// failure handler which no longer owns the lease (lost the race to a
// concurrent attempt, or the lease simply expired) makes NO durable change
// -- it must not stamp a failure over whatever the actual current owner
// wrote.
func TestNativeReferenceDiscoveryLostLeaseDoesNotOverwriteAWinner(t *testing.T) {
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
	seedDiscoveryRoute(t, ctx, pool)

	service, err := NewNativeReferenceDiscoveryService(pool, nil, &fakeDiscoveryExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate: this attempt's lease was already stolen/expired by the time
	// its executor call returned -- the ledger now shows a DIFFERENT owner
	// having already succeeded.
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,completed_at,result)
VALUES ($1,$2,$3,'success',1,now(),now(),'{}'::json)`,
		"00000000-0000-4000-8000-0000000000d8", discoveryTestRun, discoveryTestOrg); err != nil {
		t.Fatal(err)
	}

	if err := service.handleFailure(ctx, discoveryTestRun, "a-lease-owner-that-lost-the-race", errors.New("boom")); err != nil {
		t.Fatalf("handleFailure: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
		discoveryTestRun).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != discoveryStatusSuccess {
		t.Fatalf("status=%q want=success (the actual winner's result must survive untouched)", status)
	}
}

// TestNativeReferenceDiscoveryTerminalizesRunWhenFeatureDisabled pins the
// gate-check wiring in claim(): a run whose only unit targets a
// canonical-incident-gated dataset (pagerduty/incidents -> legacy_targets
// "operational", one of the two gated targets) must never be claimed at
// all when the feature is disabled org-wide (no feature_flags row --
// canonicalIncidentAllowed's ErrNoRows branch) -- terminalizeFeatureDisabledPlan
// runs instead, Discover returns nil (a clean terminal outcome, not an
// error, matching Python's `return {"status": "feature_disabled", ...}`),
// and the executor is never invoked.
func TestNativeReferenceDiscoveryTerminalizesRunWhenFeatureDisabled(t *testing.T) {
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
	seedDiscoveryRoute(t, ctx, pool)
	unitID := "00000000-0000-4000-8000-0000000000d9"
	sourceID := "00000000-0000-4000-8000-0000000000da"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status)
VALUES ($1,$2,$3,'pagerduty','incidents',$4,'planned')`,
		unitID, discoveryTestOrg, discoveryTestRun, sourceID); err != nil {
		t.Fatal(err)
	}

	executor := &fakeDiscoveryExecutor{summary: map[string]any{"ok": true}}
	service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Discover(ctx, newDiscoveryArgs()); err != nil {
		t.Fatalf("Discover: %v", err)
	}

	if executor.calls != 0 {
		t.Fatalf("executor.calls=%d want=0 (a feature-disabled run must never reach the executor)", executor.calls)
	}
	var unitStatus, unitError string
	if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_units WHERE id=$1`, unitID).
		Scan(&unitStatus, &unitError); err != nil {
		t.Fatal(err)
	}
	if unitStatus != syncRunUnitStatusFailed {
		t.Fatalf("unit status=%q want=failed", unitStatus)
	}
	if !strings.Contains(unitError, "canonical incident ingestion is disabled") {
		t.Fatalf("unit error=%q want feature-disabled message", unitError)
	}
	// No feature_flags row exists for canonical_incident_ingestion in this
	// fixture, so CanonicalIncidentDecisionForUpdate's real reason must be
	// "feature_not_registered" -- proving the CARRIED-THROUGH reason reaches
	// the persisted error text, not a placeholder.
	if !strings.Contains(unitError, "(feature_not_registered)") {
		t.Fatalf("unit error=%q want the real FeatureDecisionReason embedded, not a placeholder", unitError)
	}
	var runStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id=$1`, discoveryTestRun).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != syncRunStatusFailed {
		t.Fatalf("run status=%q want=failed", runStatus)
	}
	// No ledger row is created for a run denied before the claim step --
	// terminalizeFeatureDisabledGraph's ledger UPDATE is a conditional
	// no-op against a table with no matching row, exactly like Python's.
	var ledgerCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_run_reference_discoveries WHERE sync_run_id=$1`, discoveryTestRun).
		Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 0 {
		t.Fatalf("ledger rows=%d want=0 (denied before the ledger is ever created)", ledgerCount)
	}
}

// TestNativeReferenceDiscoveryProceedsWhenFeatureNotRequired pins the other
// half: a run whose unit targets an UNGATED dataset must sail through the
// gate-check straight into a normal claim, with the executor invoked
// exactly once -- proving the new gate-check call in claim() doesn't
// misfire false positives on ordinary, non-gated sync runs.
func TestNativeReferenceDiscoveryProceedsWhenFeatureNotRequired(t *testing.T) {
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
	seedDiscoveryRoute(t, ctx, pool)
	unitID := "00000000-0000-4000-8000-0000000000db"
	sourceID := "00000000-0000-4000-8000-0000000000dc"
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status)
VALUES ($1,$2,$3,'github','prs',$4,'planned')`,
		unitID, discoveryTestOrg, discoveryTestRun, sourceID); err != nil {
		t.Fatal(err)
	}

	executor := &fakeDiscoveryExecutor{summary: map[string]any{"ok": true}}
	service, err := NewNativeReferenceDiscoveryService(pool, nil, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Discover(ctx, newDiscoveryArgs()); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if executor.calls != 1 {
		t.Fatalf("executor.calls=%d want=1 (an ungated run must proceed through the normal claim path)", executor.calls)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_reference_discoveries WHERE sync_run_id=$1`, discoveryTestRun).
		Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != discoveryStatusSuccess {
		t.Fatalf("ledger status=%q want=success", status)
	}
}
