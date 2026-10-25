//go:build integration

package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	gosync "sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// The streak-load harness measures how often the go-reconciler materializer
// stage (600 ms) overruns while the scheduler materializes plans whose
// failure-streak read runs against a large terminal-unit history. It compares
// code versions in the same seeded database; it asserts nothing by itself and
// runs only with STREAK_HARNESS=1.

const (
	harnessStepTimeout = 15 * time.Second
	harnessBigOrg      = "00000000-0000-4000-8000-0000000a0001"
	harnessNoiseOrg    = "00000000-0000-4000-8000-0000000a0002"
	harnessFeatureID   = "00000000-0000-4000-8000-0000000a0003"
)

type harnessConfig struct {
	integrations, sources, history, hotHistory, activeRuns, activeUnits int
	stageBudget                                                         time.Duration
	duration, tick, passGap                                             time.Duration
	index, noScheduler                                                  bool
	out, label                                                          string
}

func harnessInt(key string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(key)); err == nil && value > 0 {
		return value
	}
	return fallback
}

func harnessConfigFromEnv() harnessConfig {
	return harnessConfig{
		integrations: harnessInt("H_INTEGRATIONS", 10),
		sources:      harnessInt("H_SOURCES", 50),
		history:      harnessInt("H_HISTORY", 200000),
		hotHistory:   harnessInt("H_HOT_HISTORY", 0),
		stageBudget:  time.Duration(harnessInt("H_STAGE_BUDGET_MS", 600)) * time.Millisecond,
		activeRuns:   harnessInt("H_ACTIVE_RUNS", 300),
		activeUnits:  harnessInt("H_ACTIVE_UNITS", 200),
		duration:     time.Duration(harnessInt("H_DURATION_S", 90)) * time.Second,
		tick:         time.Duration(harnessInt("H_TICK_S", 10)) * time.Second,
		passGap:      time.Duration(harnessInt("H_PASS_GAP_MS", 200)) * time.Millisecond,
		index:        os.Getenv("H_INDEX") == "1",
		noScheduler:  os.Getenv("H_NO_SCHED") == "1",
		label:        os.Getenv("H_LABEL"),
		out:          os.Getenv("H_OUT"),
	}
}

func harnessID(kind, number int) string {
	return fmt.Sprintf("00000000-0000-4000-8%03d-%012d", kind, number)
}

type harnessConfigRow struct{ configID, jobID string }

func harnessExec(ctx context.Context, t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("seed: %v\n%s", err, sql)
	}
}

func seedStreakHarness(ctx context.Context, t *testing.T, pool *pgxpool.Pool, cfg harnessConfig) []harnessConfigRow {
	t.Helper()
	pgseed.Org(ctx, t, pool, harnessBigOrg, "community")
	pgseed.Org(ctx, t, pool, harnessNoiseOrg, "community")
	pgseed.SetFeatureFlag(ctx, t, pool, harnessFeatureID, "canonical_incident_ingestion", "community", true)
	var configs []harnessConfigRow
	for i := 1; i <= cfg.integrations; i++ {
		perIntegration := cfg.history / cfg.integrations
		if i == 1 {
			// The first integration carries the large backfill history.
			perIntegration += cfg.hotHistory
		}
		integrationID, configID, jobID := harnessID(1, i), harnessID(2, i), harnessID(3, i)
		pgseed.Integration(ctx, t, pool, integrationID, harnessBigOrg, "github")
		harnessExec(ctx, t, pool, `
INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, integration_id, is_active, planner_managed, created_at, updated_at)
VALUES ($1::uuid, $2, 'harness-' || $1::text, 'github', '["work-items"]'::json, '{"schedule_cron":"0 * * * *"}'::json, $3::uuid, TRUE, TRUE, now(), now())`,
			configID, harnessBigOrg, integrationID)
		harnessExec(ctx, t, pool, `
INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ($1::uuid, $2, $3::uuid, 'work-items', TRUE, '{}'::json)`, harnessID(4, i), harnessBigOrg, integrationID)
		harnessExec(ctx, t, pool, `
INSERT INTO scheduled_jobs (id, org_id, name, sync_config_id, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
VALUES ($1::uuid, $2, 'harness-' || $1::text, $3::uuid, 'sync', '0 * * * *', 'UTC', 0, FALSE, now(), now())`,
			jobID, harnessBigOrg, configID)
		harnessExec(ctx, t, pool, `
INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
SELECT gen_random_uuid(), $1, $2::uuid, 'github', 'repository', 'org/repo-' || $3 || '-' || g, 'repo-' || g, 'org/repo-' || $3 || '-' || g,
       json_build_object('planner_managed_sync_config_id', $4::text), TRUE, now(), now()
FROM generate_series(1, $5) AS g`, harnessBigOrg, integrationID, strconv.Itoa(i), configID, cfg.sources)
		// Terminal work-items history: 20 finished runs per integration, the
		// units spread over the sources, a third failed with a cause class.
		harnessExec(ctx, t, pool, `
INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
SELECT ('00000000-0000-4000-9' || lpad($3::text, 3, '0') || '-' || lpad(g::text, 12, '0'))::uuid, $1, $2::uuid, 'backfill', 'backfill', 'success', 0, 0, 0, now() - interval '30 days'
FROM generate_series(1, 20) AS g`, harnessBigOrg, integrationID, strconv.Itoa(i))
		harnessExec(ctx, t, pool, `
WITH sources AS (
  SELECT id, row_number() OVER (ORDER BY id) - 1 AS n FROM integration_sources WHERE integration_id = $2::uuid
)
INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, result, created_at, updated_at)
SELECT gen_random_uuid(), $1, ('00000000-0000-4000-9' || lpad($3::text, 3, '0') || '-' || lpad((1 + g % 20)::text, 12, '0'))::uuid,
       $2::uuid, sources.id, 'github', 'work-items', 'medium', 'backfill',
       CASE WHEN g % 3 = 0 THEN 'failed' ELSE 'success' END, 1,
       CASE WHEN g % 3 = 0 THEN ('{"cause_class":"rate_limited","error_category":"provider","detail":"' || repeat('x', 200) || '"}')::json ELSE ('{"rows":' || g || '}')::json END,
       now() - interval '30 days', timestamptz '2026-08-01 00:00:00+00' + g * interval '7 seconds'
FROM generate_series(1, $4) AS g
JOIN sources ON sources.n = g % $5`, harnessBigOrg, integrationID, strconv.Itoa(i), perIntegration, cfg.sources)
		configs = append(configs, harnessConfigRow{configID: configID, jobID: jobID})
	}
	// Backfill load for the reconciler CTEs: active runs of another org with
	// many units, a few of them not finished.
	noiseIntegration := harnessID(5, 1)
	pgseed.Integration(ctx, t, pool, noiseIntegration, harnessNoiseOrg, "github")
	noiseSource := harnessID(6, 1)
	pgseed.IntegrationSource(ctx, t, pool, noiseSource, harnessNoiseOrg, noiseIntegration, "github", "noise/repo")
	harnessExec(ctx, t, pool, `
INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
SELECT ('00000000-0000-4000-a000-' || lpad(g::text, 12, '0'))::uuid, $1, $2::uuid, 'backfill', 'backfill', 'running', $3, 0, 0, now() - g * interval '1 minute'
FROM generate_series(1, $4) AS g`, harnessNoiseOrg, noiseIntegration, cfg.activeUnits, cfg.activeRuns)
	harnessExec(ctx, t, pool, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, result, created_at, updated_at)
SELECT gen_random_uuid(), $1, ('00000000-0000-4000-a000-' || lpad(r::text, 12, '0'))::uuid, $2::uuid, $5::uuid, 'github',
       CASE WHEN u % 2 = 0 THEN 'cicd' ELSE 'work-items' END, 'medium', 'backfill',
       CASE WHEN u = 1 THEN 'running' ELSE 'success' END, 1, '{}'::json, now() - interval '1 hour', now() - interval '1 hour'
FROM generate_series(1, $3) AS r, generate_series(1, $4) AS u`, harnessNoiseOrg, noiseIntegration, cfg.activeRuns, cfg.activeUnits, noiseSource)
	harnessExec(ctx, t, pool, `
INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
SELECT gen_random_uuid(), id, org_id, 'success', 1, now() - interval '1 hour', now(), now() FROM sync_runs WHERE org_id = $1`, harnessNoiseOrg)
	harnessExec(ctx, t, pool, `
INSERT INTO sync_run_post_dispatches (id, org_id, sync_run_id, kind, dispatched_at)
SELECT gen_random_uuid(), org_id, id, 'post_sync', now() - interval '1 hour' FROM sync_runs WHERE status = 'success'`)
	harnessExec(ctx, t, pool, `
INSERT INTO sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at)
SELECT gen_random_uuid(), org_id, sync_run_id, 'post_sync', 'pending', now(), 1, now(), now() FROM sync_run_post_dispatches`)
	return configs
}

// harnessTracer times the four materializer candidate statements by the
// outbox kind each one inserts.
type harnessTracer struct {
	mu    gosync.Mutex
	times map[string][]time.Duration
}

type harnessTraceKey struct{}

type harnessTraceStart struct {
	label string
	at    time.Time
}

func harnessStatementLabel(sql string) string {
	for _, kind := range []string{"dispatch_sync_run", "finalize_sync_run", "reference_discovery", "post_sync"} {
		if strings.Contains(sql, "'"+kind+"', 'pending'") {
			return kind
		}
	}
	if strings.Contains(sql, "FROM public.sync_run_units") && strings.Contains(sql, "cause_class") &&
		(strings.Contains(sql, "CROSS JOIN LATERAL") || strings.Contains(sql, "PARTITION BY source_id")) {
		return "streak_read"
	}
	return ""
}

func (tracer *harnessTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	label := harnessStatementLabel(data.SQL)
	if label == "" {
		return ctx
	}
	return context.WithValue(ctx, harnessTraceKey{}, harnessTraceStart{label: label, at: time.Now()})
}

func (tracer *harnessTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	start, ok := ctx.Value(harnessTraceKey{}).(harnessTraceStart)
	if !ok {
		return
	}
	elapsed := time.Since(start.at)
	tracer.mu.Lock()
	tracer.times[start.label] = append(tracer.times[start.label], elapsed)
	tracer.mu.Unlock()
}

// harnessLogCounter counts log messages (and the failure attribute of the
// streak WARN) while passing nothing on.
type harnessLogCounter struct {
	mu     *gosync.Mutex
	counts map[string]int
}

func (counter harnessLogCounter) Enabled(context.Context, slog.Level) bool { return true }
func (counter harnessLogCounter) WithAttrs([]slog.Attr) slog.Handler       { return counter }
func (counter harnessLogCounter) WithGroup(string) slog.Handler            { return counter }
func (counter harnessLogCounter) Handle(_ context.Context, record slog.Record) error {
	key := record.Message
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "failure" {
			key += "/" + attr.Value.String()
		}
		return true
	})
	counter.mu.Lock()
	counter.counts[key]++
	counter.mu.Unlock()
	return nil
}

func harnessPercentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

func TestStreakReadLoadHarness(t *testing.T) {
	if os.Getenv("STREAK_HARNESS") != "1" {
		t.Skip("measurement harness: set STREAK_HARNESS=1")
	}
	cfg := harnessConfigFromEnv()
	ctx := context.Background()
	setupCtx, cancelSetup := context.WithTimeout(ctx, 10*time.Minute)
	defer cancelSetup()
	instance, err := containers.StartPostgres(setupCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(setupCtx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(setupCtx, t, admin)
	seeded := time.Now()
	configs := seedStreakHarness(setupCtx, t, admin, cfg)
	if cfg.index {
		harnessExec(setupCtx, t, admin, `CREATE INDEX IF NOT EXISTS ix_sync_run_units_failure_streak ON public.sync_run_units (org_id, integration_id, source_id, dataset_key, updated_at DESC, id DESC) WHERE status IN ('success', 'failed')`)
	}
	harnessExec(setupCtx, t, admin, `ANALYZE`)
	seedElapsed := time.Since(seeded)

	tracer := &harnessTracer{times: map[string][]time.Duration{}}
	reconcilerConfig, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	reconcilerConfig.ConnConfig.Tracer = tracer
	reconcilerPool, err := pgxpool.NewWithConfig(ctx, reconcilerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reconcilerPool.Close)
	tracedPool := func() *pgxpool.Pool {
		config, err := pgxpool.ParseConfig(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		config.ConnConfig.Tracer = tracer
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		return pool
	}
	coordinatorPool := tracedPool()
	t.Cleanup(coordinatorPool.Close)
	domainPool := tracedPool()
	t.Cleanup(domainPool.Close)

	stage, err := syncreconciler.NewMaterializer(reconcilerPool)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(domainPool)
	if err != nil {
		t.Fatal(err)
	}
	occurrences, err := NewOccurrenceReconciler(coordinatorPool, materializer)
	if err != nil {
		t.Fatal(err)
	}

	logMu := &gosync.Mutex{}
	logCounts := map[string]int{}
	previous := slog.Default()
	slog.SetDefault(slog.New(harnessLogCounter{mu: logMu, counts: logCounts}))
	defer slog.SetDefault(previous)

	runCtx, stop := context.WithTimeout(ctx, cfg.duration)
	defer stop()
	base := time.Now().UTC().Truncate(time.Hour).Add(-30 * 24 * time.Hour)
	var wait gosync.WaitGroup

	var schedMu gosync.Mutex
	var schedSteps, schedErrors, completed, retried, quarantined int
	var schedDurations []time.Duration
	wait.Add(1)
	go func() {
		defer wait.Done()
		tick := 0
		nextTick := time.Now()
		for runCtx.Err() == nil && !cfg.noScheduler {
			if !time.Now().Before(nextTick) {
				scheduledFor := base.Add(time.Duration(tick) * time.Hour)
				for _, row := range configs {
					occurrence := newOccurrence(row.configID, harnessBigOrg, row.jobID, scheduledFor, scheduledFor, scheduledFor)
					if _, err := admin.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences (occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6, now())`, occurrence.ID, occurrence.IdentityVersion, harnessBigOrg, row.configID, row.jobID, scheduledFor); err != nil {
						t.Errorf("insert occurrence: %v", err)
						return
					}
				}
				tick++
				nextTick = nextTick.Add(cfg.tick)
			}
			stepCtx, cancel := context.WithTimeout(runCtx, harnessStepTimeout)
			started := time.Now()
			result, err := occurrences.Reconcile(stepCtx, time.Now(), 100)
			cancel()
			schedMu.Lock()
			schedSteps++
			schedDurations = append(schedDurations, time.Since(started))
			completed += result.Completed
			retried += result.Retried
			quarantined += result.Quarantined
			if err != nil && runCtx.Err() == nil {
				schedErrors++
			}
			schedMu.Unlock()
			select {
			case <-runCtx.Done():
			case <-time.After(time.Second):
			}
		}
	}()

	var passes, overruns, otherErrors int
	var passDurations []time.Duration
	wait.Add(1)
	go func() {
		defer wait.Done()
		for runCtx.Err() == nil {
			stageCtx, cancel := context.WithTimeout(runCtx, cfg.stageBudget)
			started := time.Now()
			now := time.Now().UTC()
			_, err := stage.Step(stageCtx, now, now.Add(-5*time.Minute), 100)
			elapsed := time.Since(started)
			deadline := stageCtx.Err() != nil && runCtx.Err() == nil
			cancel()
			if runCtx.Err() != nil {
				return
			}
			passes++
			passDurations = append(passDurations, elapsed)
			switch {
			case err != nil && (deadline || errors.Is(err, context.DeadlineExceeded)):
				overruns++
			case err != nil:
				otherErrors++
				t.Logf("reconciler pass error: %v", err)
			}
			select {
			case <-runCtx.Done():
			case <-time.After(cfg.passGap):
			}
		}
	}()
	wait.Wait()

	var report strings.Builder
	fmt.Fprintf(&report, "HARNESS label=%s no_scheduler=%t\n", cfg.label, cfg.noScheduler)
	fmt.Fprintf(&report, "HARNESS index=%t stage_budget_ms=%d integrations=%d sources=%d history=%d hot_history=%d active_runs=%d active_units=%d duration_s=%d tick_s=%d seed_s=%.1f\n",
		cfg.index, cfg.stageBudget.Milliseconds(), cfg.integrations, cfg.sources, cfg.history, cfg.hotHistory, cfg.activeRuns, cfg.activeUnits, int(cfg.duration.Seconds()), int(cfg.tick.Seconds()), seedElapsed.Seconds())
	rate := 0.0
	if passes > 0 {
		rate = float64(overruns) / float64(passes)
	}
	sort.Slice(passDurations, func(i, j int) bool { return passDurations[i] < passDurations[j] })
	fmt.Fprintf(&report, "RECONCILER passes=%d overruns=%d other_errors=%d overrun_rate=%.4f pass_p50_ms=%d pass_p95_ms=%d pass_max_ms=%d\n",
		passes, overruns, otherErrors, rate,
		harnessPercentile(passDurations, 0.5).Milliseconds(), harnessPercentile(passDurations, 0.95).Milliseconds(), harnessPercentile(passDurations, 1).Milliseconds())
	tracer.mu.Lock()
	for _, label := range []string{"dispatch_sync_run", "finalize_sync_run", "reference_discovery", "post_sync", "streak_read"} {
		times := append([]time.Duration(nil), tracer.times[label]...)
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		fmt.Fprintf(&report, "CTE %s n=%d p50_ms=%.1f p95_ms=%.1f max_ms=%.1f\n", label, len(times),
			float64(harnessPercentile(times, 0.5).Microseconds())/1000, float64(harnessPercentile(times, 0.95).Microseconds())/1000, float64(harnessPercentile(times, 1).Microseconds())/1000)
	}
	tracer.mu.Unlock()
	schedMu.Lock()
	sort.Slice(schedDurations, func(i, j int) bool { return schedDurations[i] < schedDurations[j] })
	fmt.Fprintf(&report, "SCHEDULER steps=%d step_errors=%d completed=%d retried=%d quarantined=%d step_p50_ms=%d step_max_ms=%d\n",
		schedSteps, schedErrors, completed, retried, quarantined,
		harnessPercentile(schedDurations, 0.5).Milliseconds(), harnessPercentile(schedDurations, 1).Milliseconds())
	schedMu.Unlock()
	var planned int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM sync_run_units WHERE org_id = $1 AND status = 'planned'`, harnessBigOrg).Scan(&planned); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(&report, "PLANNED big_org_planned_units=%d\n", planned)
	logMu.Lock()
	keys := make([]string, 0, len(logCounts))
	for key := range logCounts {
		if strings.Contains(key, "failure_streak") || strings.Contains(key, "materialize") || strings.Contains(key, "occurrence") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&report, "LOG %s=%d\n", key, logCounts[key])
	}
	logMu.Unlock()
	t.Log("\n" + report.String())
	if cfg.out != "" {
		if err := os.WriteFile(cfg.out, []byte(report.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
