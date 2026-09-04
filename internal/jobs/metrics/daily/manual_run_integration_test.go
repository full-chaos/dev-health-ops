//go:build integration

package daily

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestStartManualDailyRunWithDeferredDiscoveryMaterializesThroughTheSharedPath
// is the red-on-baseline proof for codex adversarial review round 1, P1
// (CHAOS-5055): `dev-hops metrics daily --org O` without --repo-id -- the
// CLI's own default, documented path ("Omit for every org repository
// (deferred discovery, resolved by the worker)", workerctl_dispatch.py) --
// dispatches a manual-daily run with zero partitions, exactly like the
// nightly fixed schedule and post-sync re-drive already do. Before this fix,
// MaterializeScheduledFanout only accepted a scheduled-fanout or post-sync
// generation, so the worker's Dispatcher.Work treated the resulting
// ErrInvalidState as jobruntime.Permanent (daily.go:325-335) and the run was
// stranded 'running' forever with no partitions, no completion fence, and no
// re-drive path -- despite RepositoryDiscoveryRequired correctly reporting
// true and live ClickHouse discovery succeeding. isManualDailyGeneration
// closes that gap: this test proves StartManualDailyRun -> ClaimDispatch ->
// MaterializeScheduledFanout now succeeds end to end for the exact shape a
// default `metrics daily-start` invocation produces.
func TestStartManualDailyRunWithDeferredDiscoveryMaterializesThroughTheSharedPath(t *testing.T) {
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
	createDailyTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	const org = "00000000-0000-4000-8000-000000000011"
	const day = "2026-08-26"
	generation := ManualDailyRunGeneration(org, day, nil)

	// repositoryIDs=nil is the CLI's default: `dev-hops metrics daily --org
	// O` with no --repo-id flags.
	outcome, err := store.StartManualDailyRun(ctx, org, day, generation, nil, publisher)
	if err != nil {
		t.Fatalf("StartManualDailyRun (deferred discovery) failed: %v", err)
	}

	var partitionsBeforeDiscovery int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, outcome.RunID).Scan(&partitionsBeforeDiscovery); err != nil {
		t.Fatal(err)
	}
	if partitionsBeforeDiscovery != 0 {
		t.Fatalf("manual-daily run created %d partitions before discovery ran; want 0 (deferred discovery)", partitionsBeforeDiscovery)
	}

	claimed, err := store.ClaimDispatch(ctx, outcome.RunID)
	if err != nil || claimed == nil || !claimed.RepositoryDiscoveryRequired {
		t.Fatalf("manual-daily dispatch claim=%#v err=%v, want RepositoryDiscoveryRequired=true", claimed, err)
	}

	// The heavy worker resolves this against ClickHouse repos.id in
	// production (daily.RepositoryDiscoverer); this store-layer test
	// supplies the discovered set directly, exactly like the post-sync
	// deferred-discovery test above -- MaterializeScheduledFanout is where
	// this fix's store-side contract lives.
	discovered := []RepositoryID{
		"00000000-0000-4000-8000-000000000002",
		"00000000-0000-4000-8000-000000000001",
	}
	created, err := store.MaterializeScheduledFanout(ctx, *claimed, discovered)
	if err != nil || !created {
		t.Fatalf("manual-daily deferred-discovery materialize=%t err=%v -- before the CHAOS-5055 fix this returned ErrInvalidState, permanently stranding the run", created, err)
	}

	var partitionCount int
	var ids string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, outcome.RunID).Scan(&partitionCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT repo_ids::text FROM daily_metrics_partitions WHERE run_id = $1::uuid`, outcome.RunID).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if partitionCount != 1 || ids != `["00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"]` {
		t.Fatalf("manual-daily materialized partitions=%d ids=%s", partitionCount, ids)
	}
}

// TestStartManualDailyRunRefusesADayAlreadyCoveredByADifferentGeneration is
// the red-on-baseline proof for codex adversarial review round 2, P1
// (CHAOS-5055): `metrics daily --org O` for a day whose all-repository
// computation the nightly fixed schedule or a post-sync re-drive already
// completed used to dispatch a SECOND, independent all-repository run
// anyway -- StartRunTx's (org_id, target_day, generation) uniqueness only
// makes ONE generation idempotent against its own replays, so two different
// triggers for the identical (org, day) scope both persisted and executed,
// duplicate-writing every native daily family (file_hotspots included).
// This test seeds a succeeded run under the SCHEDULED fan-out's own
// generation format, then proves a deferred-discovery manual trigger for
// the same org/day is refused with ErrDayAlreadyCovered rather than
// dispatching a duplicate.
//
// Round 3 (codex adversarial review, P1) narrowed HasSucceededRunForDay to
// ONLY scheduled-fanout/post-sync generations as valid coverage sources --
// an earlier manual trigger (repository-scoped or not) can no longer serve
// as the "already covered" source; see
// TestStartManualDailyRunDoesNotFalselyBlockAnAllRepositoryRequestAfterARepositoryScopedOne
// below for the regression that finding fixed.
func TestStartManualDailyRunRefusesADayAlreadyCoveredByADifferentGeneration(t *testing.T) {
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
	createDailyTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	const org = "00000000-0000-4000-8000-000000000012"
	const day = "2026-08-26"
	const scheduledRunID = "00000000-0000-4000-8000-000000000013"
	now := time.Now().UTC()

	// Simulate the nightly fixed schedule having already computed and
	// finalized this exact org+day, under ITS OWN generation format --
	// nothing this test does reaches MaterializeScheduledFanout/finalize;
	// only the terminal daily_metrics_runs.status this check reads matters.
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1::uuid,$2::uuid,$3::date,'fixed-schedule:daily_metrics_fanout:2026-08-26T01:00:00Z','succeeded','succeeded',$4,$4)`,
		scheduledRunID, org, day, now); err != nil {
		t.Fatal(err)
	}

	generation := ManualDailyRunGeneration(org, day, nil)
	outcome, err := store.StartManualDailyRun(ctx, org, day, generation, nil, publisher)
	if !errors.Is(err, ErrDayAlreadyCovered) {
		t.Fatalf("expected ErrDayAlreadyCovered for a day the fixed schedule already covered, got err=%v outcome=%+v", err, outcome)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM daily_metrics_runs WHERE org_id = $1::uuid AND target_day = $2::date",
		org, day,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("expected no new run to be inserted (still 1, the seeded scheduled run), found %d", runCount)
	}
}

// TestStartManualDailyRunStillAllowsAnIdempotentRetryOfItsOwnSuccess proves
// HasSucceededRunForDay's excludeGeneration parameter does its job: a
// retried CLI invocation for the SAME logical manual request (deterministic
// generation, ManualDailyRunGeneration) must still reach StartRunTx's own
// ON CONFLICT DO NOTHING idempotency path -- not be refused as
// "already covered" by its own prior success, which would turn a safe retry
// into a hard failure.
func TestStartManualDailyRunStillAllowsAnIdempotentRetryOfItsOwnSuccess(t *testing.T) {
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
	createDailyTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	const org = "00000000-0000-4000-8000-000000000014"
	const day = "2026-08-26"
	generation := ManualDailyRunGeneration(org, day, nil)

	outcome, err := store.StartManualDailyRun(ctx, org, day, generation, nil, publisher)
	if err != nil {
		t.Fatalf("first StartManualDailyRun failed: %v", err)
	}
	// Mark it succeeded directly (matching the other integration tests'
	// convention of writing the terminal state straight to the table rather
	// than driving a full claim/materialize/finalize sequence, which is not
	// what this test is proving).
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, outcome.RunID); err != nil {
		t.Fatal(err)
	}

	retried, err := store.StartManualDailyRun(ctx, org, day, generation, nil, publisher)
	if err != nil {
		t.Fatalf("idempotent retry under the SAME generation was refused: %v", err)
	}
	if retried.RunID != outcome.RunID {
		t.Fatalf("retry returned a different run: first=%s retried=%s", outcome.RunID, retried.RunID)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM daily_metrics_runs WHERE org_id = $1::uuid AND target_day = $2::date",
		org, day,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("expected the retry to reuse the same row (still 1), found %d", runCount)
	}
}

// TestStartManualDailyRunDoesNotFalselyBlockAnAllRepositoryRequestAfterARepositoryScopedOne
// is the red-on-baseline proof for codex adversarial review round 3, P1
// (CHAOS-5055): round 2's own HasSucceededRunForDay check queried only
// (org_id, target_day, status) with no awareness of which repositories a
// prior run actually covered. A successful REPOSITORY-SCOPED manual run
// (e.g. `--repo-id R`) satisfied that EXISTS check for the whole org+day,
// so a LATER, genuinely different all-repository request for the same day
// was refused as "already covered" -- even though every OTHER repository
// in the organization was never computed. That is a SILENT
// UNDER-COMPUTATION, worse than the duplicate-compute the check exists to
// prevent. Round 3 restricted HasSucceededRunForDay to scheduled-fanout/
// post-sync generations only (see its own doc comment); this test proves a
// prior repository-scoped manual success no longer blocks a later
// all-repository manual request for the same org/day.
func TestStartManualDailyRunDoesNotFalselyBlockAnAllRepositoryRequestAfterARepositoryScopedOne(t *testing.T) {
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
	createDailyTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	const org = "00000000-0000-4000-8000-000000000015"
	const day = "2026-08-26"
	const repo = RepositoryID("00000000-0000-4000-8000-000000000016")

	// A successful, repository-SCOPED manual run for the same org/day.
	scopedGeneration := ManualDailyRunGeneration(org, day, []RepositoryID{repo})
	scopedOutcome, err := store.StartManualDailyRun(ctx, org, day, scopedGeneration, []RepositoryID{repo}, publisher)
	if err != nil {
		t.Fatalf("repository-scoped StartManualDailyRun failed: %v", err)
	}
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
UPDATE daily_metrics_runs SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $1
WHERE id = $2::uuid`, now, scopedOutcome.RunID); err != nil {
		t.Fatal(err)
	}

	// A LATER, genuinely different all-repository (deferred-discovery)
	// request for the SAME org/day must NOT be refused as already covered.
	allRepoGeneration := ManualDailyRunGeneration(org, day, nil)
	allRepoOutcome, err := store.StartManualDailyRun(ctx, org, day, allRepoGeneration, nil, publisher)
	if err != nil {
		t.Fatalf("all-repository request was falsely blocked by a prior repository-scoped success: %v", err)
	}
	if allRepoOutcome.RunID == scopedOutcome.RunID {
		t.Fatalf("all-repository request was given the repository-scoped run's id instead of its own: %s", allRepoOutcome.RunID)
	}

	var runCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM daily_metrics_runs WHERE org_id = $1::uuid AND target_day = $2::date",
		org, day,
	).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 2 {
		t.Fatalf("expected 2 distinct runs (the repository-scoped one and the all-repository one), found %d", runCount)
	}
}
