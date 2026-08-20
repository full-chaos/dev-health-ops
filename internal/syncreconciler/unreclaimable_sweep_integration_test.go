//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sweepOrg = "00000000-0000-4000-8000-000000004000"
	sweepRun = "00000000-0000-4000-8000-000000004001"
)

func sweepUnitID(index int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", 4100+index)
}

func startSweepPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createSweepFixture(t, ctx, pool)
	return pool
}

// createSweepFixture mirrors the columns this sweep reads and writes.
//
// Hand-rolled DDL matching the sibling integration fixtures in this package,
// rather than running the real migrations. The columns that matter here are
// the ones the predicate turns on -- created_at, updated_at,
// last_heartbeat_at, attempts, lease_owner, lease_expires_at -- plus the
// terminal-write targets and the outbox dedupe key.
func createSweepFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			status text NOT NULL
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			provider text NOT NULL,
			dataset_key text NOT NULL,
			cost_class text NOT NULL,
			mode text NOT NULL,
			status text NOT NULL,
			attempts integer NOT NULL DEFAULT 0,
			available_at timestamptz,
			last_retry_reason text,
			error text,
			result jsonb,
			lease_owner text,
			lease_expires_at timestamptz,
			last_heartbeat_at timestamptz,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			dedupe_key text NOT NULL UNIQUE,
			job_kind text NOT NULL,
			contract_version integer NOT NULL,
			args jsonb NOT NULL,
			payload_hash text NOT NULL,
			queue text NOT NULL,
			priority smallint NOT NULL,
			max_attempts smallint NOT NULL,
			scheduled_at timestamptz NOT NULL,
			status text NOT NULL,
			next_attempt_at timestamptz NOT NULL,
			attempt_count integer NOT NULL DEFAULT 0
		)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func seedSweepRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, status string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.sync_runs (id, org_id, status) VALUES ($1, $2, $3)`,
		id, sweepOrg, status); err != nil {
		t.Fatal(err)
	}
}

type sweepUnitSpec struct {
	id           string
	dataset      string
	costClass    string
	status       string
	createdAt    time.Time
	updatedAt    time.Time
	attempts     int64
	leaseOwner   *string
	leaseExpires *time.Time
	heartbeat    *time.Time
}

func seedSweepUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, spec sweepUnitSpec) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_units (
			id, org_id, sync_run_id, provider, dataset_key, cost_class, mode, status,
			attempts, lease_owner, lease_expires_at, last_heartbeat_at,
			created_at, updated_at
		) VALUES ($1, $2, $3, 'github', $4, $5, 'incremental', $6,
			$7, $8, $9, $10, $11, $12)`,
		spec.id, sweepOrg, sweepRun, spec.dataset, spec.costClass, spec.status,
		spec.attempts, spec.leaseOwner, spec.leaseExpires, spec.heartbeat,
		spec.createdAt, spec.updatedAt); err != nil {
		t.Fatal(err)
	}
}

// strandedSpec is the production shape: claimed to dispatching, never
// published, no lease, no heartbeat, no attempt, long-lived and idle.
func strandedSpec(id, dataset, costClass string, now time.Time) sweepUnitSpec {
	return sweepUnitSpec{
		id: id, dataset: dataset, costClass: costClass, status: "dispatching",
		createdAt: now.Add(-16 * time.Hour),
		updatedAt: now.Add(-90 * time.Minute),
	}
}

func newSweepForTest(t *testing.T, pool *pgxpool.Pool, mode SweepMode) *UnreclaimableSweep {
	t.Helper()
	sweep, err := NewUnreclaimableSweep(pool, UnreclaimableSweepConfig{
		Age:  DefaultUnreclaimableAge,
		Idle: DefaultUnreclaimableIdle,
		Mode: mode,
		// tests / pr-reviews / pr-comments left off: the production wedge.
		Switches: providersync.CompleteRouteSwitches{GithubRepoMetadata: true},
		Presence: CeleryAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sweep
}

func sweepUnitState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) (
	status string, errText *string, reason *string, result []byte,
) {
	t.Helper()
	if err := pool.QueryRow(ctx,
		`SELECT status, error, last_retry_reason, result::text
		 FROM public.sync_run_units WHERE id = $1`, id,
	).Scan(&status, &errText, &reason, &result); err != nil {
		t.Fatal(err)
	}
	return status, errText, reason, result
}

func TestUnreclaimableSweepTerminalizesTheProductionShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	seedSweepUnit(t, ctx, pool, strandedSpec(sweepUnitID(1), "tests", "heavy", now))

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Candidates != 1 || result.Terminalized != 1 {
		t.Fatalf("result = %+v, want 1 candidate and 1 terminalized", result)
	}

	status, errText, reason, payload := sweepUnitState(t, ctx, pool, sweepUnitID(1))
	if status != "failed" {
		t.Fatalf("status = %q, want failed", status)
	}
	if errText == nil || *errText != unreclaimableErrorCategory {
		t.Fatalf("error = %v, want %q", errText, unreclaimableErrorCategory)
	}
	// The durable reason is the whole point of layer 2: a bare category is
	// what made a thousand-attempt retry loop unexplainable in production.
	if reason == nil || !strings.Contains(*reason, "github/tests") {
		t.Fatalf("last_retry_reason = %v", reason)
	}
	var decoded map[string]string
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("result payload: %v", err)
	}
	if decoded["error_category"] != unreclaimableErrorCategory ||
		decoded["provider"] != "github" || decoded["dataset_key"] != "tests" {
		t.Fatalf("result payload = %v", decoded)
	}
}

// Every one of these must survive: the sweep destroys work if any is wrong.
func TestUnreclaimableSweepSparesEverythingItMust(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")

	owner := "worker-a"
	expires := now.Add(10 * time.Minute)
	live := now

	spared := map[string]sweepUnitSpec{}
	// An enabled pair: a live runtime can execute it.
	spec := strandedSpec(sweepUnitID(10), "repo-metadata", "light", now)
	spared["enabled pair"] = spec
	// Freshly dispatched: inside the stale window.
	spec = strandedSpec(sweepUnitID(11), "tests", "heavy", now)
	spec.updatedAt = now
	spared["freshly dispatched"] = spec
	// Young: has not existed long enough to be presumed dead.
	spec = strandedSpec(sweepUnitID(12), "tests", "heavy", now)
	spec.createdAt = now.Add(-5 * time.Minute)
	spared["young"] = spec
	// Attempted: a consumer started it.
	spec = strandedSpec(sweepUnitID(13), "tests", "heavy", now)
	spec.attempts = 1
	spared["attempted"] = spec
	// Leased and RUNNING: a worker holds it.
	spec = strandedSpec(sweepUnitID(14), "tests", "heavy", now)
	spec.status = "running"
	spec.leaseOwner = &owner
	spec.leaseExpires = &expires
	spared["running with lease"] = spec
	// Live heartbeat: something is actively working it.
	spec = strandedSpec(sweepUnitID(15), "tests", "heavy", now)
	spec.heartbeat = &live
	spared["live heartbeat"] = spec

	for _, spec := range spared {
		seedSweepUnit(t, ctx, pool, spec)
	}

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Terminalized != 0 || result.Candidates != 0 {
		t.Fatalf("result = %+v, want nothing selected or written", result)
	}
	for name, spec := range spared {
		status, _, _, _ := sweepUnitState(t, ctx, pool, spec.id)
		if status == "failed" {
			t.Fatalf("%s was terminalized", name)
		}
	}
}

// A budget deferral stamps last_heartbeat_at though no worker ran, and the
// later claim never clears it. A strict IS NULL would exempt this forever.
func TestUnreclaimableSweepReachesAOnceBudgetDeferredUnit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	stale := now.Add(-4 * time.Hour)
	spec := strandedSpec(sweepUnitID(20), "tests", "heavy", now)
	spec.heartbeat = &stale
	seedSweepUnit(t, ctx, pool, spec)

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Terminalized != 1 {
		t.Fatalf("result = %+v, want the stale-heartbeat strand terminalized", result)
	}
}

// A unit that entered the River relay belongs to River and CHAOS-3951.
func TestUnreclaimableSweepSparesAUnitWithAnOutboxRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	seedSweepUnit(t, ctx, pool, strandedSpec(sweepUnitID(30), "tests", "heavy", now))
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.worker_job_outbox (
			dedupe_key, job_kind, contract_version, args, payload_hash, queue,
			priority, max_attempts, scheduled_at, status, next_attempt_at, attempt_count
		) VALUES ($1, 'sync.provider_unit', 1, '{}'::jsonb, $2, 'sync',
			1, 5, now(), 'pending', now(), 0)`,
		unreclaimableDedupeKey(sweepUnitID(30)), "sha256:"+strings.Repeat("0", 64),
	); err != nil {
		t.Fatal(err)
	}

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Candidates != 0 || result.Terminalized != 0 {
		t.Fatalf("result = %+v, want the published unit left to River", result)
	}
}

// Filtering after the limit would let ineligible rows mask a strand forever.
func TestUnreclaimableSweepPagesPastIneligibleRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	// Three OLDER enabled-pair rows sort ahead of the strand and are dropped
	// by the routability filter.
	for index := 0; index < 3; index++ {
		spec := strandedSpec(sweepUnitID(40+index), "repo-metadata", "light", now)
		spec.createdAt = now.Add(-20 * time.Hour)
		seedSweepUnit(t, ctx, pool, spec)
	}
	seedSweepUnit(t, ctx, pool, strandedSpec(sweepUnitID(50), "tests", "heavy", now))

	// Limit 1: the first page is entirely ineligible.
	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 1)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Terminalized != 1 || len(result.UnitIDs) != 1 ||
		result.UnitIDs[0] != sweepUnitID(50) {
		t.Fatalf("result = %+v, want only the masked strand", result)
	}
}

// Shadow selects exactly what active would, and writes nothing.
func TestUnreclaimableSweepShadowSelectsWithoutWriting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	seedSweepUnit(t, ctx, pool, strandedSpec(sweepUnitID(60), "tests", "heavy", now))

	result, err := newSweepForTest(t, pool, SweepModeShadow).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("shadow sweep: %v", err)
	}
	if result.Mode != SweepModeShadow || result.Candidates != 1 ||
		result.Terminalized != 0 {
		t.Fatalf("result = %+v, want one candidate reported and nothing written", result)
	}
	if len(result.UnitIDs) != 1 || result.UnitIDs[0] != sweepUnitID(60) ||
		len(result.Pairs) != 1 || result.Pairs[0] != "github/tests" {
		t.Fatalf("shadow report = %+v, want the would-terminalize detail", result)
	}
	status, _, _, _ := sweepUnitState(t, ctx, pool, sweepUnitID(60))
	if status != "dispatching" {
		t.Fatalf("shadow mode wrote: status = %q", status)
	}
}

// The optimistic-concurrency guard: a dispatcher touch between the select and
// the write must invalidate the terminalization rather than race it.
func TestUnreclaimableSweepYieldsToAConcurrentDispatcherTouch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	// TWO strands. One gets touched by a dispatcher between the select and the
	// write; the other does not. Both go through the identical write path, so
	// the pair together proves the CAS discriminates rather than simply never
	// matching -- a CAS broken by, say, a lost timestamptz round trip would
	// report zero rows for BOTH, and a single-row test would call that a pass.
	touched := sweepUnitID(70)
	untouched := sweepUnitID(71)
	seedSweepUnit(t, ctx, pool, strandedSpec(touched, "tests", "heavy", now))
	seedSweepUnit(t, ctx, pool, strandedSpec(untouched, "tests", "heavy", now))

	sweep := newSweepForTest(t, pool, SweepModeActive)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	candidates, err := sweep.selectUnreclaimable(ctx, tx, now, 100)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("selected %d candidates, want 2", len(candidates))
	}

	// The dispatcher reclaims and republishes exactly one of them.
	if _, err := pool.Exec(ctx,
		`UPDATE public.sync_run_units SET updated_at = now() WHERE id = $1`,
		touched); err != nil {
		t.Fatal(err)
	}

	for _, candidate := range candidates {
		affected, err := sweep.terminalize(ctx, tx, candidate, now)
		if err != nil {
			t.Fatalf("terminalize %s: %v", candidate.id, err)
		}
		switch candidate.id {
		case touched:
			if affected != 0 {
				t.Fatalf("touched row affected %d, want 0 -- the CAS must yield", affected)
			}
		case untouched:
			if affected != 1 {
				t.Fatalf("untouched row affected %d, want 1 -- otherwise the "+
					"yield above proves nothing", affected)
			}
		}
	}
}

// THE ACCEPTANCE CRITERION (CHAOS-4005): the production wedge shape must
// terminalize in ONE pass, versus the ~6 dispatch cycles cap rotation needs,
// because the sweep is bounded by its own limit rather than the per-bucket
// concurrency cap.
func TestUnreclaimableSweepDrainsTheProductionWedgeInOnePass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")

	// 72 stranded units across the three disabled aliases and both cost
	// classes, matching the 2026-08-20 production wedge.
	shape := []struct {
		dataset   string
		costClass string
		count     int
	}{
		{"tests", "heavy", 29},
		{"pr-comments", "medium", 23},
		{"pr-reviews", "medium", 20},
	}
	index := 0
	for _, group := range shape {
		for seeded := 0; seeded < group.count; seeded++ {
			index++
			seedSweepUnit(t, ctx, pool,
				strandedSpec(sweepUnitID(100+index), group.dataset, group.costClass, now))
		}
	}
	if index != 72 {
		t.Fatalf("seeded %d units, want 72", index)
	}

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Terminalized != 72 {
		t.Fatalf("terminalized %d of 72 in one pass", result.Terminalized)
	}

	var remaining int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.sync_run_units
		 WHERE sync_run_id = $1 AND status = 'dispatching'`, sweepRun,
	).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("%d units still dispatching after one pass", remaining)
	}
	// Every drained unit carries the durable reason an operator reads.
	var missingReason int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.sync_run_units
		 WHERE sync_run_id = $1 AND last_retry_reason IS NULL`, sweepRun,
	).Scan(&missingReason); err != nil {
		t.Fatal(err)
	}
	if missingReason != 0 {
		t.Fatalf("%d drained units carry no last_retry_reason", missingReason)
	}
}
