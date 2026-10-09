//go:build integration

package workersctl

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The acceptance case of `metrics daily-start --rerun`, through the verb on
// the migrated Postgres schema.
//
// A day that a manual run already computed must be computed again when its
// teams changed. Without the flag the same command starts nothing: the
// generation of a manual request is a function of the request. The flag names
// one re-run by a token:
//
//   - a new token starts a new run of the day, also for a day that a scheduled
//     run covers;
//   - the same token starts nothing a second time, so a retried command is
//     safe;
//   - a new token is refused with in_progress while a manual run of the day is
//     pending or running, so a loop cannot stack runs of one day.
//
// The file uses no symbol of the flag, so the same file runs on a tree
// without it.

const (
	dailyRerunOrg  = "00000000-0000-4000-8000-00000000e2e1"
	dailyRerunRepo = "00000000-0000-4000-8000-00000000e2e2"
	dailyRerunDay  = "2026-08-24"
)

func dailyRerunRuntime(t *testing.T, ctx context.Context) *operatorRuntime {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: &commandBackend{queues: map[string]joboperator.QueueSummary{}},
		Authorizer: commandAuthorizer{}, DomainGuard: commandDomainGuard{}, Auditor: commandAuditor{},
		RouteController: commandRouteController{}, JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &operatorRuntime{
		service: service, registry: registry,
		pools: &postgresstore.RuntimePools{Domain: pool}, principal: joboperator.OperatorPrincipal,
	}
}

// dailyRerunStart runs the verb for the one day and returns its exit code and
// its error output.
func dailyRerunStart(t *testing.T, ctx context.Context, runtime *operatorRuntime, extra ...string) (int, string) {
	t.Helper()
	args := append([]string{
		"daily-start", "--org", dailyRerunOrg, "--day", dailyRerunDay,
		"--reason", "operator_test", "--correlation-id", "corr-1",
	}, extra...)
	var stdout, stderr bytes.Buffer
	code := dispatchMetrics(ctx, runtime, args, &stdout, &stderr)
	return code, stderr.String()
}

func dailyRerunRuns(t *testing.T, ctx context.Context, runtime *operatorRuntime) int {
	t.Helper()
	var runs int
	if err := runtime.pools.Domain.QueryRow(ctx,
		"SELECT count(*) FROM public.daily_metrics_runs WHERE org_id = $1::uuid AND target_day = $2::date",
		dailyRerunOrg, dailyRerunDay).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	return runs
}

// dailyRerunEndRuns ends every run of the day as the worker does when its
// partitions and its finalization are done.
func dailyRerunEndRuns(t *testing.T, ctx context.Context, runtime *operatorRuntime) {
	t.Helper()
	if _, err := runtime.pools.Domain.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE org_id = $1::uuid AND target_day = $2::date`, dailyRerunOrg, dailyRerunDay); err != nil {
		t.Fatalf("end the runs of the day: %v", err)
	}
}

func TestMetricsDailyStartRerunComputesAStoredDayAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runtime := dailyRerunRuntime(t, ctx)
	scoped := []string{"--repo-id", dailyRerunRepo}
	with := func(extra ...string) []string { return append(append([]string{}, scoped...), extra...) }

	// The day is computed by a manual run. The same command starts nothing.
	if code, stderr := dailyRerunStart(t, ctx, runtime, scoped...); code != 0 {
		t.Fatalf("the first run: code=%d stderr=%q", code, stderr)
	}
	dailyRerunEndRuns(t, ctx, runtime)
	if code, stderr := dailyRerunStart(t, ctx, runtime, scoped...); code != 0 || dailyRerunRuns(t, ctx, runtime) != 1 {
		t.Fatalf("the same command again: code=%d stderr=%q runs=%d, want 0 and still 1 run",
			code, stderr, dailyRerunRuns(t, ctx, runtime))
	}

	// A re-run token starts a new run of the day.
	if code, stderr := dailyRerunStart(t, ctx, runtime, with("--rerun", "carry-1")...); code != 0 || dailyRerunRuns(t, ctx, runtime) != 2 {
		t.Fatalf("--rerun carry-1: code=%d stderr=%q runs=%d, want 0 and 2 runs", code, stderr, dailyRerunRuns(t, ctx, runtime))
	}
	// The same token starts nothing a second time, also while its run is in
	// flight.
	if code, stderr := dailyRerunStart(t, ctx, runtime, with("--rerun", "carry-1")...); code != 0 || dailyRerunRuns(t, ctx, runtime) != 2 {
		t.Errorf("--rerun carry-1 again: code=%d stderr=%q runs=%d, want 0 and still 2 runs", code, stderr, dailyRerunRuns(t, ctx, runtime))
	}
	// A new token is refused while the run of carry-1 is pending.
	if code, stderr := dailyRerunStart(t, ctx, runtime, with("--rerun", "carry-2")...); code == 0 ||
		!strings.Contains(stderr, `"in_progress"`) || dailyRerunRuns(t, ctx, runtime) != 2 {
		t.Errorf("--rerun carry-2 with a run in flight: code=%d stderr=%q runs=%d, want a refusal with in_progress and still 2 runs",
			code, stderr, dailyRerunRuns(t, ctx, runtime))
	}
	// When that run has ended, the new token starts its run.
	dailyRerunEndRuns(t, ctx, runtime)
	if code, stderr := dailyRerunStart(t, ctx, runtime, with("--rerun", "carry-2")...); code != 0 || dailyRerunRuns(t, ctx, runtime) != 3 {
		t.Errorf("--rerun carry-2 after the run ended: code=%d stderr=%q runs=%d, want 0 and 3 runs", code, stderr, dailyRerunRuns(t, ctx, runtime))
	}
	dailyRerunEndRuns(t, ctx, runtime)

	// A request for every repository of a day that a scheduled run covers is
	// refused without the flag and starts with it.
	if _, err := runtime.pools.Domain.Exec(ctx, `
INSERT INTO public.daily_metrics_runs
    (id, org_id, target_day, generation, status, finalization_status, finalized_at, created_at, updated_at, full_org)
VALUES (gen_random_uuid(), $1::uuid, $2::date, 'fixed-schedule:daily_metrics_fanout:nightly', 'succeeded', 'succeeded',
        clock_timestamp(), clock_timestamp(), clock_timestamp(), true)`, dailyRerunOrg, dailyRerunDay); err != nil {
		t.Fatalf("insert the scheduled run: %v", err)
	}
	if code, stderr := dailyRerunStart(t, ctx, runtime); code == 0 || !strings.Contains(stderr, `"already_covered"`) {
		t.Fatalf("every repository, no flag, a covered day: code=%d stderr=%q, want a refusal with already_covered", code, stderr)
	}
	if code, stderr := dailyRerunStart(t, ctx, runtime, "--rerun", "carry-3"); code != 0 || dailyRerunRuns(t, ctx, runtime) != 5 {
		t.Errorf("every repository with --rerun carry-3: code=%d stderr=%q runs=%d, want 0 and 5 runs", code, stderr, dailyRerunRuns(t, ctx, runtime))
	}
}
