//go:build integration

package syncreconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

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
		// Alembic 0055, column for column. The hand-rolled two-column version
		// this replaces omitted `generation`, which the CHAOS-4035 route fence
		// reads: the invented schema turned a real read into a 42703 and would
		// have hidden any predicate that depended on the missing columns.
		`CREATE TABLE public.worker_job_routes (
			job_kind varchar(96) NOT NULL,
			transport varchar(16) NOT NULL,
			paused boolean NOT NULL DEFAULT false,
			generation bigint NOT NULL DEFAULT 1,
			updated_at timestamptz NOT NULL DEFAULT now(),
			CONSTRAINT ck_worker_job_route_transport
				CHECK (transport IN ('celery', 'shadow', 'river_canary', 'river')),
			CONSTRAINT ck_worker_job_route_generation CHECK (generation >= 1),
			CONSTRAINT worker_job_routes_pkey PRIMARY KEY (job_kind)
		)`,
		`INSERT INTO public.worker_job_routes (job_kind, transport)
			VALUES ('sync.provider_unit', 'river_canary')`,
		// river_job_id, delivered_at and ck_worker_job_outbox_delivery_state
		// are copied from the production table (\d public.worker_job_outbox),
		// not invented. The constraint is the load-bearing part: it is what
		// makes "status = 'delivered'" and "river_job_id IS NOT NULL" the same
		// statement, which is why CHAOS-4097's liveness read needs none of the
		// text-cast defensiveness the sibling coordinator-plane repair carries.
		// A fixture without it would let a test seed a shape production cannot
		// hold and prove nothing.
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
			attempt_count integer NOT NULL DEFAULT 0,
			river_job_id bigint,
			delivered_at timestamptz,
			CONSTRAINT uq_worker_job_outbox_river_job_id UNIQUE (river_job_id),
			CONSTRAINT ck_worker_job_outbox_delivery_state CHECK (
				status = 'delivered' AND river_job_id IS NOT NULL AND delivered_at IS NOT NULL
				OR status <> 'delivered' AND river_job_id IS NULL AND delivered_at IS NULL
			)
		)`,
		`CREATE SCHEMA river`,
		// The state column is a real enum, as it is in River's own migration,
		// so `state::text` in the sweep is exercised as the cast it actually
		// is. A text column here would silently accept a value River could
		// never store and make the closed IN list untestable.
		`CREATE TYPE river.river_job_state AS ENUM (
			'available', 'cancelled', 'completed', 'discarded',
			'pending', 'retryable', 'running', 'scheduled'
		)`,
		`CREATE TABLE river.river_job (
			id bigserial PRIMARY KEY,
			state river.river_job_state NOT NULL,
			attempt smallint NOT NULL DEFAULT 0,
			max_attempts smallint NOT NULL DEFAULT 5,
			kind text NOT NULL,
			errors jsonb[] NOT NULL DEFAULT '{}',
			finalized_at timestamptz,
			created_at timestamptz NOT NULL DEFAULT now()
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
	// One superuser pool in all three positions. That is exactly what makes
	// this harness blind to the CHAOS-4035 wiring defect, and why the real
	// role split is proven separately in
	// unreclaimable_sweep_role_split_integration_test.go rather than here.
	sweep, err := NewUnreclaimableSweep(pool, pool, pool, "river", UnreclaimableSweepConfig{
		Age:  DefaultUnreclaimableAge,
		Idle: DefaultUnreclaimableIdle,
		Mode: mode,
		// CHAOS-4054: capability is always on and there is no route switch
		// left. github/repo-metadata is RouteReady && Plannable
		// unconditionally, so it is never swept. tests / pr-reviews /
		// pr-comments are alias identities -- RouteReady but never
		// independently Plannable -- so they stay sweepable regardless of
		// any configuration; that is the production wedge this test proves.
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

// Capability configuration is not route ownership. When the durable route
// still says Celery -- the rollback and coexistence state -- Celery owns every
// provider unit and the sweep must not touch one, whatever the Go switches or
// the worker-group declaration say.
func TestUnreclaimableSweepDefersWhenTheDurableRouteIsCelery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	seedSweepUnit(t, ctx, pool, strandedSpec(sweepUnitID(80), "tests", "heavy", now))
	if _, err := pool.Exec(ctx,
		`UPDATE public.worker_job_routes SET transport = 'celery'
		 WHERE job_kind = 'sync.provider_unit'`); err != nil {
		t.Fatal(err)
	}

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Candidates != 0 || result.Terminalized != 0 {
		t.Fatalf("result = %+v, want nothing touched under a Celery route", result)
	}
	status, _, _, _ := sweepUnitState(t, ctx, pool, sweepUnitID(80))
	if status != "dispatching" {
		t.Fatalf("status = %q, want the unit left alone", status)
	}
}

// A missing or duplicated route row is a control-plane fault, not permission.
func TestUnreclaimableSweepDefersWhenTheRouteRowIsUnusable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	seedSweepUnit(t, ctx, pool, strandedSpec(sweepUnitID(81), "tests", "heavy", now))
	if _, err := pool.Exec(ctx,
		`DELETE FROM public.worker_job_routes WHERE job_kind = 'sync.provider_unit'`,
	); err != nil {
		t.Fatal(err)
	}

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Terminalized != 0 {
		t.Fatalf("result = %+v, want no write with the route row missing", result)
	}
}

// The keyset cursor must resume past a persistent prefix of ineligible rows
// WITHIN the pass. An offset-based scan cap restarts at zero every pass, so
// such a prefix would hide this strand forever (review finding).
func TestUnreclaimableSweepReachesAStrandBehindALongIneligiblePrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	// 250 older enabled-pair rows: eligible by the SQL predicate, dropped by
	// routability, and they sort ahead of the strand.
	for index := 0; index < 250; index++ {
		spec := strandedSpec(sweepUnitID(200+index), "repo-metadata", "light", now)
		spec.createdAt = now.Add(-20 * time.Hour).Add(time.Duration(index) * time.Second)
		seedSweepUnit(t, ctx, pool, spec)
	}
	strand := sweepUnitID(900)
	seedSweepUnit(t, ctx, pool, strandedSpec(strand, "tests", "heavy", now))

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 10)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Terminalized != 1 || len(result.UnitIDs) != 1 || result.UnitIDs[0] != strand {
		t.Fatalf("result = %+v, want the strand behind the prefix", result)
	}
}

// mode=off must not touch a strand it would otherwise terminalize.
func TestUnreclaimableSweepDoesNothingWhenModeIsOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	seedSweepUnit(t, ctx, pool, strandedSpec(sweepUnitID(90), "tests", "heavy", now))

	result, err := newSweepForTest(t, pool, SweepModeOff).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Candidates != 0 || result.Terminalized != 0 {
		t.Fatalf("result = %+v, want a disabled sweep to do nothing", result)
	}
	status, _, _, _ := sweepUnitState(t, ctx, pool, sweepUnitID(90))
	if status != "dispatching" {
		t.Fatalf("status = %q, want untouched", status)
	}
}

// A paused route is the control plane's STOP. Every other reader of
// worker_job_routes honours it -- jobroute.Controller.Resolve returns
// ErrPaused -- and an operator pausing provider units during an incident would
// otherwise halt producers and relays while leaving the one component that
// destroys work still running.
func TestUnreclaimableSweepDefersWhenTheDurableRouteIsPaused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	seedSweepUnit(t, ctx, pool, strandedSpec(sweepUnitID(85), "tests", "heavy", now))
	// Transport stays river_canary. Only the pause flag moves, so a sweep that
	// reads transport alone still sees River ownership -- which is exactly the
	// shape that shipped.
	if _, err := pool.Exec(ctx,
		`UPDATE public.worker_job_routes SET paused = TRUE
		 WHERE job_kind = 'sync.provider_unit'`); err != nil {
		t.Fatal(err)
	}

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Candidates != 0 || result.Terminalized != 0 {
		t.Fatalf("result = %+v, want nothing touched under a paused route", result)
	}
	status, _, _, _ := sweepUnitState(t, ctx, pool, sweepUnitID(85))
	if status != "dispatching" {
		t.Fatalf("status = %q, want the unit left alone while the route is paused", status)
	}
}

// seedSweepDelivery writes the production delivery shape: a 'delivered' outbox
// row bound by value to a real river_job row.
//
// The outbox row is minted through the SAME dedupe key the sweep computes
// (unreclaimableDedupeKey), because that key -- not a foreign key -- is the
// only linkage between a unit and its delivery. Hand-writing the string here
// would let the two drift and quietly make every assertion below vacuous.
func seedSweepDelivery(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	unitID string,
	jobState string,
	jobError string,
) int64 {
	t.Helper()
	// A spent budget by default. For a discarded job that is what makes it
	// unrecoverable and therefore sweepable; for every other state the column
	// is not read at all.
	return seedSweepDeliveryWithBudget(t, ctx, pool, unitID, jobState, jobError, 5, 5)
}

// seedSweepDeliveryWithBudget exists so a test can put a discarded job on
// EITHER side of the disjointness boundary with internal/joboutbox's
// terminal-delivery repair. That boundary is the whole safety argument for
// sweeping discarded jobs at all, so it has to be reachable from a fixture.
func seedSweepDeliveryWithBudget(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	unitID string,
	jobState string,
	jobError string,
	attempt int,
	maxAttempts int,
) int64 {
	t.Helper()
	var jobID int64
	// finalized_at is set exactly when the state is terminal, matching River:
	// a live job has none, and the sweep's liveness read requires it.
	var finalized any
	switch jobState {
	case "cancelled", "discarded", "completed":
		finalized = time.Now().UTC()
	}
	// The errors array is built in SQL rather than as a hand-written literal.
	// A jsonb[] literal needs two levels of escaping and a mistake there
	// produces a row that parses but says something else, which is the class
	// of fixture bug that makes an assertion pass for the wrong reason.
	if err := pool.QueryRow(ctx, `
		INSERT INTO river.river_job (state, attempt, max_attempts, kind, errors, finalized_at)
		VALUES ($1::river.river_job_state, $4, $5, 'sync.provider_unit',
			CASE
				WHEN $2::text = '' THEN '{}'::jsonb[]
				ELSE ARRAY[jsonb_build_object('attempt', 1, 'error', $2::text)]
			END,
			$3)
		RETURNING id`,
		jobState, jobError, finalized, attempt, maxAttempts,
	).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.worker_job_outbox (
			dedupe_key, job_kind, contract_version, args, payload_hash, queue,
			priority, max_attempts, scheduled_at, status, next_attempt_at,
			attempt_count, river_job_id, delivered_at
		) VALUES ($1, 'sync.provider_unit', 1, '{}'::jsonb, $2, 'sync',
			1, 5, now(), 'delivered', now(), 1, $3, now())`,
		unreclaimableDedupeKey(unitID), "sha256:"+strings.Repeat("0", 64), jobID,
	); err != nil {
		t.Fatal(err)
	}
	return jobID
}

// THE CHAOS-4093 SHAPE, seeded exactly (CHAOS-4097).
//
// This is the RED CONTROL for the whole ticket. On the code this replaces, the
// unit is dropped by the published-outbox filter before routability is even
// considered, so the sweep reports zero candidates and the unit sits in
// 'dispatching' forever holding a per-bucket concurrency slot. Production held
// 650 of these across 83 runs for twenty-two hours and every repair path in
// the system was structurally blind to all of them.
//
// The pair is repo-metadata deliberately: it is RouteReady AND Plannable, so
// unroutable() returns false for it. If this test passes only because the pair
// happens to be declinable, it is proving the OLD branch and nothing new --
// which is precisely the mistake the ticket warns about, since every one of
// the 650 production units was routable.
func TestUnreclaimableSweepTerminalizesAUnitWhoseRiverDeliveryWasCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	unit := sweepUnitID(60)
	seedSweepUnit(t, ctx, pool, strandedSpec(unit, "repo-metadata", "light", now))
	// The exact text production logged: the adapter cancelled at validation,
	// before the handler and before the idempotency claim, so nothing was
	// written back to the unit.
	jobID := seedSweepDelivery(t, ctx, pool, unit, "cancelled",
		"JobCancelError: dev-health job failed [validation]")

	// Non-vacuity: prove the pair really is routable, so this cannot be the
	// pre-existing unroutable() branch passing under a new name.
	if sweep := newSweepForTest(t, pool, SweepModeActive); sweep.unroutable(
		unreclaimableCandidate{provider: "github", datasetKey: "repo-metadata"},
	) {
		t.Fatal("github/repo-metadata is declinable, so this test would pass " +
			"on the old code and proves nothing about the liveness proof")
	}

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Candidates != 1 || result.Terminalized != 1 {
		t.Fatalf("result = %+v, want the dead delivery's unit terminalized", result)
	}
	status, errText, reason, payload := sweepUnitState(t, ctx, pool, unit)
	if status != "failed" {
		t.Fatalf("unit status = %q, want failed", status)
	}
	if errText == nil || *errText != unreclaimableTerminalDeliveryCategory {
		t.Fatalf("unit error = %v, want %q", errText, unreclaimableTerminalDeliveryCategory)
	}
	// The acceptance criterion is explicit that the durable reason must NAME
	// the terminal job state, so an operator can tell this apart from a
	// declined capability without leaving the row.
	if reason == nil || !strings.Contains(*reason, "cancelled") {
		t.Fatalf("unit reason = %v, want it to name the terminal River state", reason)
	}
	var decoded map[string]string
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["error_category"] != unreclaimableTerminalDeliveryCategory ||
		decoded["river_job_state"] != "cancelled" ||
		decoded["river_job_id"] != fmt.Sprintf("%d", jobID) {
		t.Fatalf("result payload = %#v, want the delivery evidence carried durably", decoded)
	}
}

// The same shape with a DISCARDED job. Both terminal non-success states are
// swept, and asserting only the CHAOS-4093 one would leave the other branch of
// the closed IN list untested.
func TestUnreclaimableSweepTerminalizesAUnitWhoseRiverDeliveryWasDiscarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	unit := sweepUnitID(61)
	seedSweepUnit(t, ctx, pool, strandedSpec(unit, "repo-metadata", "light", now))
	seedSweepDelivery(t, ctx, pool, unit, "discarded", "dev-health job failed [permanent]")

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Terminalized != 1 {
		t.Fatalf("result = %+v, want the discarded delivery's unit terminalized", result)
	}
	_, _, reason, _ := sweepUnitState(t, ctx, pool, unit)
	if reason == nil || !strings.Contains(*reason, "discarded") {
		t.Fatalf("unit reason = %v, want it to name the discarded state", reason)
	}
}

// PER-DIMENSION NON-VACUITY for the liveness read.
//
// Every state River can hold that is NOT a terminal failure must leave the
// unit alone. Without these the new branch would pass just as happily if the
// IN list had been written as "every state", which is the change that would
// destroy live work -- a running job's unit is about to be claimed, and a
// completed job's unit is finished.
func TestUnreclaimableSweepSparesAUnitWhoseRiverDeliveryIsNotTerminallyFailed(t *testing.T) {
	for index, state := range []string{
		"available", "running", "retryable", "scheduled", "pending", "completed",
	} {
		t.Run(state, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			pool := startSweepPostgres(t, ctx)
			now := time.Now().UTC()
			seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
			unit := sweepUnitID(70 + index)
			seedSweepUnit(t, ctx, pool, strandedSpec(unit, "repo-metadata", "light", now))
			seedSweepDelivery(t, ctx, pool, unit, state, "")

			result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
			if err != nil {
				t.Fatalf("sweep: %v", err)
			}
			if result.Candidates != 0 || result.Terminalized != 0 {
				t.Fatalf("result = %+v, want state %q left to River", result, state)
			}
			if status, _, _, _ := sweepUnitState(t, ctx, pool, unit); status != "dispatching" {
				t.Fatalf("unit status = %q, want it untouched", status)
			}
		})
	}
}

// A published row that is NOT 'delivered' carries no river_job_id, so there is
// nothing to prove death with. It must be dropped, not swept.
//
// This is the live case for a row that internal/joboutbox's TerminalDeliveryRepair
// has just rearmed: a replacement delivery is on its way, and terminalizing
// then would destroy work that is about to run.
func TestUnreclaimableSweepSparesAUnitWhoseDeliveryWasRearmed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	unit := sweepUnitID(80)
	seedSweepUnit(t, ctx, pool, strandedSpec(unit, "repo-metadata", "light", now))
	seedSweepDelivery(t, ctx, pool, unit, "cancelled",
		"JobCancelError: dev-health job failed [validation]")
	// Exactly what the rearm does: status back to pending, delivery cleared.
	if _, err := pool.Exec(ctx, `
		UPDATE public.worker_job_outbox
		SET status = 'pending', river_job_id = NULL, delivered_at = NULL
		WHERE dedupe_key = $1`, unreclaimableDedupeKey(unit)); err != nil {
		t.Fatal(err)
	}

	result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Candidates != 0 || result.Terminalized != 0 {
		t.Fatalf("result = %+v, want the rearmed delivery left alone", result)
	}
}

// THE CAS, executed rather than argued.
//
// The liveness proof is taken on the queue-control pool, outside the domain
// transaction, so a rearm can land between the proof and the commit. The write
// therefore re-asserts the exact (dedupe_key, river_job_id) PAIR it was proven
// on. This drives the statement directly because the race itself is not
// reproducible from the Step() seam -- and a guard whose refusal is never
// observed is indistinguishable from one that does not exist.
func TestTerminalizeTerminalDeliveryRefusesWhenTheDeliveryChanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startSweepPostgres(t, ctx)
	now := time.Now().UTC()
	seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
	unit := sweepUnitID(90)
	spec := strandedSpec(unit, "repo-metadata", "light", now)
	seedSweepUnit(t, ctx, pool, spec)
	jobID := seedSweepDelivery(t, ctx, pool, unit, "cancelled",
		"JobCancelError: dev-health job failed [validation]")
	key := unreclaimableDedupeKey(unit)

	exec := func(pinnedJobID int64) int64 {
		t.Helper()
		tag, err := pool.Exec(ctx, terminalizeTerminalDeliverySQL,
			unit, unreclaimableTerminalDeliveryCategory, "reason", "{}",
			now, spec.updatedAt, key, pinnedJobID)
		if err != nil {
			t.Fatal(err)
		}
		return tag.RowsAffected()
	}

	// POSITIVE CONTROL FIRST. Without it a zero below could mean the CAS
	// works, or that the statement never matches anything at all.
	if affected := exec(jobID); affected != 1 {
		t.Fatalf("the unchanged delivery was refused (%d rows); this test cannot "+
			"distinguish a working CAS from a statement that matches nothing", affected)
	}
	// Put the unit back so the negative case differs ONLY in the pinned pair.
	if _, err := pool.Exec(ctx, `
		UPDATE public.sync_run_units
		SET status = 'dispatching', error = NULL, last_retry_reason = NULL,
			result = NULL, updated_at = $2
		WHERE id = $1`, unit, spec.updatedAt); err != nil {
		t.Fatal(err)
	}
	if affected := exec(jobID + 1); affected != 0 {
		t.Fatalf("the write landed on a delivery it was never proven against "+
			"(%d rows); a replacement delivery would be destroyed", affected)
	}
	if status, _, _, _ := sweepUnitState(t, ctx, pool, unit); status != "dispatching" {
		t.Fatalf("unit status = %q, want it untouched by the refused write", status)
	}
}

// THE DISJOINTNESS BOUNDARY with internal/joboutbox's terminal-delivery
// repair, in both directions (adversarial review finding).
//
// That repair rearms a provider-unit outbox row -- deleting the dead River job
// and minting a replacement delivery -- when the job was DISCARDED by River's
// unhandled-kind rescue with attempts still on the clock, and its recovery
// requires the unit to still be 'dispatching'. If this sweep terminalized such
// a unit first, the repair could never run and a transport failure River was
// about to retry would become permanent domain failure.
//
// The two live in different reconcile loops, so ordering cannot be relied on.
// The predicates are disjoint by construction instead: the repair takes
// `attempt < max_attempts`, this sweep takes `attempt >= max_attempts`. Both
// halves are asserted here, because only the pair proves disjointness -- the
// refusal alone would also pass if the sweep had simply stopped sweeping
// discarded jobs altogether.
func TestUnreclaimableSweepLeavesRecoverableDiscardedDeliveriesToTheOutboxRepair(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		attempt     int
		maxAttempts int
		wantSwept   bool
		why         string
	}{
		{
			name: "attempts remaining is the outbox repair's row", attempt: 2, maxAttempts: 5,
			wantSwept: false,
			why: "internal/joboutbox/terminal_delivery_repair.go can still delete this " +
				"job and mint a replacement delivery, but only while the unit is " +
				"'dispatching' -- terminalizing it here destroys a recoverable retry",
		},
		{
			name: "a spent budget is nobody's row but this sweep's", attempt: 5, maxAttempts: 5,
			wantSwept: true,
			why: "River has no retry left and the outbox repair's " +
				"`attempt < max_attempts` predicate excludes it, so refusing here " +
				"would strand the unit exactly as CHAOS-4093 did",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			pool := startSweepPostgres(t, ctx)
			now := time.Now().UTC()
			seedSweepRun(t, ctx, pool, sweepRun, "dispatching")
			unit := sweepUnitID(95)
			seedSweepUnit(t, ctx, pool, strandedSpec(unit, "repo-metadata", "light", now))
			// The exact error the outbox repair keys on, so the row is a genuine
			// candidate for it rather than being excluded for some other reason.
			seedSweepDeliveryWithBudget(t, ctx, pool, unit, "discarded",
				"Stuck job rescued by JobRescuer", testCase.attempt, testCase.maxAttempts)

			result, err := newSweepForTest(t, pool, SweepModeActive).Step(ctx, now, 100)
			if err != nil {
				t.Fatalf("sweep: %v", err)
			}
			swept := result.Terminalized == 1
			if swept != testCase.wantSwept {
				t.Fatalf("terminalized=%t, want %t: %s", swept, testCase.wantSwept, testCase.why)
			}
			status, _, _, _ := sweepUnitState(t, ctx, pool, unit)
			wantStatus := "dispatching"
			if testCase.wantSwept {
				wantStatus = "failed"
			}
			if status != wantStatus {
				t.Fatalf("unit status = %q, want %q: %s", status, wantStatus, testCase.why)
			}
		})
	}
}
