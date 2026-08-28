package jobruntime

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetricsCollectorEmitsDeterministicLowCardinalityPrometheusText(t *testing.T) {
	t.Parallel()
	job := JobLabels{Queue: "retention", Kind: "system.retention_cleanup"}
	stream := StreamLabels{Stream: "external_ingest", ConsumerGroup: "sink_workers"}
	budget := BudgetLabels{Provider: "github", CostClass: "medium"}
	syncLease := SyncLeaseLabels{Provider: "github", DatasetFamily: "work_items"}
	collector, err := NewMetricsCollector(MetricDimensions{
		Jobs:        []JobLabels{job},
		DomainTypes: []string{"maintenance_run"},
		SyncLeases:  []SyncLeaseLabels{syncLease},
		Streams:     []StreamLabels{stream},
		Budgets:     []BudgetLabels{budget},
	})
	if err != nil {
		t.Fatalf("NewMetricsCollector: %v", err)
	}
	ctx := context.Background()
	if err := RegisterRuntime(ctx, collector, RuntimeInfo{Version: "1.2.3", Commit: "abc123"}); err != nil {
		t.Fatalf("RegisterRuntime: %v", err)
	}
	if err := collector.SetJobsAvailable(job, 7); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetJobOldestAge("retention", 12*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveJobWait(job, 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetExecutionSaturation("retention", 0.75); err != nil {
		t.Fatal(err)
	}

	collector.JobStarted(ctx, job)
	collector.JobFinished(ctx, job, ResultSuccess, CategoryNone, 2250*time.Millisecond)
	collector.JobStarted(ctx, job)
	collector.JobPanicked(ctx, job)
	collector.JobFinished(ctx, job, ResultRetry, CategoryPanic, 100*time.Millisecond)
	collector.JobStarted(ctx, job)
	collector.JobCancelled(ctx, job, CategoryPermanent)
	collector.JobFinished(ctx, job, ResultCancel, CategoryPermanent, 50*time.Millisecond)
	collector.DomainMismatch(ctx, "maintenance_run")
	if err := collector.ObserveSyncLeaseExpired(syncLease, SyncLeaseResultRetrying); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveReportRunLeaseExpired(ReportRunLeaseResultRetrying); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveWorkGraphLeaseReleaseLost(); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveIdempotencyRenewalRetired(IdempotencyRenewalTransientExhausted); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveDailyMetricsDiscovery(DailyMetricsRunTriggerPostSync, DailyMetricsDiscoveryOutcomeNoRepositories); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObservePostSyncFanout(PostSyncFanoutOutcomePublished); err != nil {
		t.Fatal(err)
	}
	// no_repositories and error are deliberately left unobserved so their
	// pre-seeded zeros (asserted below) are proven, not merely assumed.
	// scheduled_fanout/materialized is deliberately left unobserved so its
	// pre-seeded zero (asserted below) is proven, not merely assumed.
	// The fenced reason is deliberately left unobserved so its pre-seeded zero
	// is proven. A retirement counter that only appears after the first
	// retirement gives an alert nothing to bind to until the incident is
	// already happening.
	// RemainingMetricsLeaseReleaseLost is deliberately left unobserved so its
	// pre-seeded zero (asserted below) is proven, not merely assumed.

	if err := collector.SetStreamLag(stream, 19); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetStreamPending(stream, 3); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetStreamOldestPending(stream, 8*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveProviderBudgetWait(budget, 250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetDatabasePoolSaturation(poolDomain, 0.5); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveDatabasePoolAcquire(poolQueueControl, poolResultTimeout, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	first := collector.PrometheusText()
	second := collector.PrometheusText()
	if first != second {
		t.Fatal("Prometheus exposition is not deterministic")
	}

	lines := []string{
		`worker_runtime_info{version="1.2.3",commit="abc123"} 1`,
		`worker_jobs_available{queue="retention",kind="system.retention_cleanup"} 7`,
		`worker_job_oldest_age_seconds{queue="retention"} 12`,
		`worker_jobs_running{queue="retention",kind="system.retention_cleanup"} 0`,
		`worker_execution_saturation_ratio{queue="retention"} 0.75`,
		`worker_job_wait_seconds_bucket{queue="retention",kind="system.retention_cleanup",le="1"} 0`,
		`worker_job_wait_seconds_bucket{queue="retention",kind="system.retention_cleanup",le="2.5"} 1`,
		`worker_job_wait_seconds_sum{queue="retention",kind="system.retention_cleanup"} 1.5`,
		`worker_job_wait_seconds_count{queue="retention",kind="system.retention_cleanup"} 1`,
		`worker_job_duration_seconds_count{queue="retention",kind="system.retention_cleanup",result="success"} 1`,
		`worker_job_attempts_total{kind="system.retention_cleanup",result="cancel",error_category="permanent"} 1`,
		`worker_job_attempts_total{kind="system.retention_cleanup",result="retry",error_category="panic"} 1`,
		`worker_job_attempts_total{kind="system.retention_cleanup",result="success",error_category="none"} 1`,
		`worker_job_panics_total{kind="system.retention_cleanup"} 1`,
		`worker_job_cancellations_total{kind="system.retention_cleanup",reason="permanent"} 1`,
		`worker_domain_state_mismatch_total{domain_type="maintenance_run"} 1`,
		`worker_sync_lease_expired_total{provider="github",dataset_family="work_items",result="failed"} 0`,
		`worker_sync_lease_expired_total{provider="github",dataset_family="work_items",result="retrying"} 1`,
		`worker_report_run_lease_expired_total{result="failed"} 0`,
		`worker_report_run_lease_expired_total{result="retrying"} 1`,
		`worker_idempotency_renewal_retired_total{reason="fenced"} 0`,
		`worker_idempotency_renewal_retired_total{reason="transient_exhausted"} 1`,
		`worker_daily_metrics_lease_total{stage="finalize",result="reclaimed"} 0`,
		`worker_daily_metrics_lease_total{stage="finalize",result="release_lost"} 0`,
		`worker_daily_metrics_lease_total{stage="finalize",result="snoozed"} 0`,
		`worker_daily_metrics_lease_total{stage="partition",result="reclaimed"} 0`,
		`worker_daily_metrics_lease_total{stage="partition",result="release_lost"} 0`,
		`worker_daily_metrics_lease_total{stage="partition",result="snoozed"} 0`,
		`worker_daily_metrics_discovery_total{trigger="scheduled_fanout",outcome="materialized"} 0`,
		`worker_daily_metrics_discovery_total{trigger="scheduled_fanout",outcome="no_repositories"} 0`,
		`worker_daily_metrics_discovery_total{trigger="post_sync",outcome="materialized"} 0`,
		`worker_daily_metrics_discovery_total{trigger="post_sync",outcome="no_repositories"} 1`,
		`dev_health_post_sync_fanout_total{outcome="published"} 1`,
		`dev_health_post_sync_fanout_total{outcome="no_repositories"} 0`,
		`dev_health_post_sync_fanout_total{outcome="error"} 0`,
		`worker_workgraph_lease_release_lost_total 1`,
		`worker_remaining_metrics_lease_release_lost_total 0`,
		`worker_stream_lag{stream="external_ingest",consumer_group="sink_workers"} 19`,
		`worker_stream_pending{stream="external_ingest",consumer_group="sink_workers"} 3`,
		`worker_stream_oldest_pending_seconds{stream="external_ingest",consumer_group="sink_workers"} 8`,
		`worker_budget_wait_seconds_sum{provider="github",cost_class="medium"} 0.25`,
		`worker_budget_wait_seconds_count{provider="github",cost_class="medium"} 1`,
		`worker_database_pool_saturation_ratio{pool="domain"} 0.5`,
		`worker_database_pool_acquire_seconds_sum{pool="queue_control",result="timeout"} 0.1`,
		`worker_database_pool_acquire_seconds_count{pool="queue_control",result="timeout"} 1`,
	}
	for _, line := range lines {
		if !strings.Contains(first, line+"\n") {
			t.Errorf("missing exposition line:\n%s", line)
		}
	}

	metricOrder := []string{
		"worker_runtime_info", "worker_jobs_available", "worker_job_oldest_age_seconds",
		"worker_jobs_running", "worker_execution_saturation_ratio", "worker_job_wait_seconds", "worker_job_duration_seconds",
		"worker_job_attempts_total", "worker_job_panics_total", "worker_job_cancellations_total",
		"worker_domain_state_mismatch_total", "worker_sync_lease_expired_total", "worker_report_run_lease_expired_total",
		"worker_idempotency_renewal_retired_total",
		"worker_daily_metrics_lease_total", "worker_daily_metrics_discovery_total",
		"dev_health_post_sync_fanout_total",
		"worker_workgraph_lease_release_lost_total", "worker_remaining_metrics_lease_release_lost_total",
		"worker_stream_lag", "worker_stream_pending",
		"worker_stream_oldest_pending_seconds", "worker_budget_wait_seconds",
		"worker_database_pool_saturation_ratio", "worker_database_pool_acquire_seconds",
	}
	previous := -1
	for _, metric := range metricOrder {
		index := strings.Index(first, "# HELP "+metric+" ")
		if index <= previous {
			t.Fatalf("metric family %s is absent or out of order", metric)
		}
		previous = index
	}
	for _, forbidden := range []string{"organization_id", "job_id", "encoded_args", "payload", "credential-secret"} {
		if strings.Contains(first, forbidden) {
			t.Fatalf("forbidden value or label %q appears in exposition", forbidden)
		}
	}
	for _, line := range strings.Split(first, "\n") {
		if strings.HasPrefix(line, "worker_job_") && strings.Contains(line, `result="failed"`) {
			t.Fatalf("generic job metric emitted forbidden failed result: %s", line)
		}
	}
	assertPrometheusTextShape(t, first)
}

func TestMetricsCollectorRejectsUnregisteredOrUnboundedDimensions(t *testing.T) {
	t.Parallel()
	job := JobLabels{Queue: "heartbeat", Kind: "system.heartbeat"}
	collector, err := NewMetricsCollector(MetricDimensions{
		Jobs:        []JobLabels{job},
		DomainTypes: []string{"schedule_occurrence"},
		SyncLeases:  []SyncLeaseLabels{{Provider: "github", DatasetFamily: "work_items"}},
		Streams:     []StreamLabels{{Stream: "external_ingest", ConsumerGroup: "sink_workers"}},
		Budgets:     []BudgetLabels{{Provider: "github", CostClass: "medium"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	unknownJob := JobLabels{Queue: "tenant-secret", Kind: "system.heartbeat"}
	if err := collector.SetJobsAvailable(unknownJob, 1); err == nil || strings.Contains(err.Error(), "tenant-secret") {
		t.Fatalf("unregistered label error is missing or unsafe: %v", err)
	}
	if err := collector.SetJobOldestAge("tenant-secret", time.Second); err == nil {
		t.Fatal("unregistered queue accepted")
	}
	if err := collector.SetStreamLag(StreamLabels{Stream: "unknown", ConsumerGroup: "sink_workers"}, 1); err == nil {
		t.Fatal("unregistered stream accepted")
	}
	if err := collector.ObserveProviderBudgetWait(BudgetLabels{Provider: "unknown", CostClass: "medium"}, time.Second); err == nil {
		t.Fatal("unregistered budget accepted")
	}
	if err := collector.ObserveSyncLeaseExpired(SyncLeaseLabels{Provider: "unknown", DatasetFamily: "work_items"}, SyncLeaseResultRetrying); err == nil {
		t.Fatal("unregistered sync lease dimensions accepted")
	}
	if err := collector.ObserveSyncLeaseExpired(SyncLeaseLabels{Provider: "github", DatasetFamily: "work_items"}, SyncLeaseResult("cas_conflict")); err == nil {
		t.Fatal("unregistered sync lease result accepted")
	}
	if err := collector.ObserveReportRunLeaseExpired(ReportRunLeaseResult("cas_conflict")); err == nil {
		t.Fatal("unregistered report run lease result accepted")
	}
	if err := collector.SetDatabasePoolSaturation("tenant_pool", 0.5); err == nil {
		t.Fatal("unregistered pool accepted")
	}
	if err := collector.SetExecutionSaturation("unknown", 0.5); err == nil {
		t.Fatal("unregistered execution queue accepted")
	}
	if err := collector.SetExecutionSaturation("ops", math.Inf(1)); err == nil {
		t.Fatal("infinite execution saturation accepted")
	}
	if err := collector.SetDatabasePoolSaturation(poolDomain, math.NaN()); err == nil {
		t.Fatal("NaN pool saturation accepted")
	}
	if err := collector.ObserveDatabasePoolAcquire(poolDomain, "unknown", time.Second); err == nil {
		t.Fatal("unregistered acquisition result accepted")
	}

	collector.JobStarted(context.Background(), unknownJob)
	collector.JobFinished(context.Background(), unknownJob, Result("failed"), ErrorCategory("secret"), time.Second)
	collector.JobCancelled(context.Background(), unknownJob, ErrorCategory("secret"))
	collector.DomainMismatch(context.Background(), "tenant-secret")
	collector.RuntimeRegistered(context.Background(), RuntimeInfo{Version: "1.0.0", Commit: "abc"})
	text := collector.PrometheusText()
	for _, forbidden := range []string{"tenant-secret", `result="cas_conflict"`, `error_category="secret"`, `provider="unknown"`} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("rejected observer label escaped into exposition: %s", forbidden)
		}
	}
}

func TestMetricsCollectorPreRegistersSyncLeaseSeriesInStableOrder(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{
		SyncLeases: []SyncLeaseLabels{
			{Provider: "gitlab", DatasetFamily: "issues"},
			{Provider: "github", DatasetFamily: "work_items"},
			{Provider: "github", DatasetFamily: "commits"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	text := collector.PrometheusText()
	want := []string{
		`worker_sync_lease_expired_total{provider="github",dataset_family="commits",result="failed"} 0`,
		`worker_sync_lease_expired_total{provider="github",dataset_family="commits",result="retrying"} 0`,
		`worker_sync_lease_expired_total{provider="github",dataset_family="work_items",result="failed"} 0`,
		`worker_sync_lease_expired_total{provider="github",dataset_family="work_items",result="retrying"} 0`,
		`worker_sync_lease_expired_total{provider="gitlab",dataset_family="issues",result="failed"} 0`,
		`worker_sync_lease_expired_total{provider="gitlab",dataset_family="issues",result="retrying"} 0`,
	}
	previous := -1
	for _, line := range want {
		index := strings.Index(text, line+"\n")
		if index <= previous {
			t.Fatalf("sync lease series absent or out of order: %s", line)
		}
		previous = index
	}
}

func TestMetricsCollectorConstructorBoundsCardinality(t *testing.T) {
	t.Parallel()
	job := JobLabels{Queue: "heartbeat", Kind: "system.heartbeat"}
	if _, err := NewMetricsCollector(MetricDimensions{Jobs: []JobLabels{job, job}}); err == nil {
		t.Fatal("duplicate job dimensions accepted")
	}
	if _, err := NewMetricsCollector(MetricDimensions{Jobs: []JobLabels{{Queue: "bad/queue", Kind: "system.heartbeat"}}}); err == nil {
		t.Fatal("unsafe job dimension accepted")
	}
	syncLease := SyncLeaseLabels{Provider: "github", DatasetFamily: "work_items"}
	if _, err := NewMetricsCollector(MetricDimensions{SyncLeases: []SyncLeaseLabels{syncLease, syncLease}}); err == nil {
		t.Fatal("duplicate sync lease dimensions accepted")
	}
	if _, err := NewMetricsCollector(MetricDimensions{SyncLeases: []SyncLeaseLabels{{Provider: "github", DatasetFamily: "tenant/work_items"}}}); err == nil {
		t.Fatal("unsafe sync lease dimension accepted")
	}
	jobs := make([]JobLabels, maxMetricJobs+1)
	for index := range jobs {
		jobs[index] = JobLabels{Queue: fmt.Sprintf("queue-%d", index), Kind: fmt.Sprintf("job.kind_%d", index)}
	}
	if _, err := NewMetricsCollector(MetricDimensions{Jobs: jobs}); err == nil {
		t.Fatal("unbounded job dimensions accepted")
	}
	syncLeases := make([]SyncLeaseLabels, maxMetricSyncLeases+1)
	for index := range syncLeases {
		syncLeases[index] = SyncLeaseLabels{Provider: "github", DatasetFamily: fmt.Sprintf("family_%d", index)}
	}
	if _, err := NewMetricsCollector(MetricDimensions{SyncLeases: syncLeases}); err == nil {
		t.Fatal("unbounded sync lease dimensions accepted")
	}
}

func TestMetricsCollectorSupportsStreamOnlyRuntimeAndWriter(t *testing.T) {
	t.Parallel()
	stream := StreamLabels{Stream: "external_ingest", ConsumerGroup: "sink_workers"}
	collector, err := NewMetricsCollector(MetricDimensions{
		Streams: []StreamLabels{stream},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterRuntime(context.Background(), collector, RuntimeInfo{Version: "1.0.0", Commit: "abc123"}); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetStreamLag(stream, 5); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := collector.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if output.String() != collector.PrometheusText() {
		t.Fatal("writer and string exposition differ")
	}
	if !strings.Contains(output.String(), `worker_runtime_info{version="1.0.0",commit="abc123"} 1`) ||
		!strings.Contains(output.String(), `worker_stream_lag{stream="external_ingest",consumer_group="sink_workers"} 5`) {
		t.Fatalf("stream-only exposition missing runtime/lag:\n%s", output.String())
	}
}

func TestMetricsCollectorConcurrentUpdates(t *testing.T) {
	job := JobLabels{Queue: "retention", Kind: "system.retention_cleanup"}
	collector, err := NewMetricsCollector(MetricDimensions{
		Jobs: []JobLabels{job}, DomainTypes: []string{"maintenance_run"},
		SyncLeases: []SyncLeaseLabels{{Provider: "github", DatasetFamily: "work_items"}},
		Streams:    []StreamLabels{{Stream: "external_ingest", ConsumerGroup: "sink_workers"}},
		Budgets:    []BudgetLabels{{Provider: "github", CostClass: "medium"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 32
	const iterations = 100
	var wait sync.WaitGroup
	wait.Add(goroutines)
	for worker := 0; worker < goroutines; worker++ {
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				syncLeaseResult := SyncLeaseResultRetrying
				if iteration%2 == 0 {
					syncLeaseResult = SyncLeaseResultFailed
				}
				collector.JobStarted(context.Background(), job)
				collector.JobFinished(context.Background(), job, ResultSuccess, CategoryNone, time.Millisecond)
				_ = collector.SetJobsAvailable(job, int64((worker+iteration)%10))
				_ = collector.SetJobOldestAge("retention", time.Duration(iteration)*time.Millisecond)
				_ = collector.SetExecutionSaturation("retention", 0.5)
				_ = collector.ObserveJobWait(job, time.Millisecond)
				_ = collector.ObserveProviderBudgetWait(BudgetLabels{Provider: "github", CostClass: "medium"}, time.Millisecond)
				_ = collector.ObserveSyncLeaseExpired(SyncLeaseLabels{Provider: "github", DatasetFamily: "work_items"}, syncLeaseResult)
				_ = collector.SetDatabasePoolSaturation(poolDomain, 0.5)
				_ = collector.ObserveDatabasePoolAcquire(poolDomain, poolResultAcquired, time.Millisecond)
				_ = collector.SetStreamPending(StreamLabels{Stream: "external_ingest", ConsumerGroup: "sink_workers"}, int64(iteration))
				_ = collector.PrometheusText()
			}
		}(worker)
	}
	wait.Wait()
	text := collector.PrometheusText()
	wantAttempts := fmt.Sprintf(`worker_job_attempts_total{kind="system.retention_cleanup",result="success",error_category="none"} %d`, goroutines*iterations)
	if !strings.Contains(text, wantAttempts+"\n") {
		t.Fatalf("attempt counter lost concurrent updates; want %s", wantAttempts)
	}
	if !strings.Contains(text, `worker_jobs_running{queue="retention",kind="system.retention_cleanup"} 0`+"\n") {
		t.Fatal("running gauge did not converge to zero")
	}
	wantLeaseResults := goroutines * iterations / 2
	for _, result := range []SyncLeaseResult{SyncLeaseResultFailed, SyncLeaseResultRetrying} {
		want := fmt.Sprintf(`worker_sync_lease_expired_total{provider="github",dataset_family="work_items",result="%s"} %d`, result, wantLeaseResults)
		if !strings.Contains(text, want+"\n") {
			t.Fatalf("sync lease counter lost concurrent updates; want %s", want)
		}
	}
}

func TestDimensionsForQueuesUsesRegistryPolicy(t *testing.T) {
	t.Parallel()
	registry, err := Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	dimensions, err := DimensionsForQueues(registry, []string{"coverage", "heartbeat", "retention", "webhooks"}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(dimensions.Jobs) != 5 ||
		strings.Join(dimensions.DomainTypes, ",") != "billing_notification,maintenance_run,schedule_occurrence,webhook_delivery" {
		t.Fatalf("queue dimensions drifted: %+v", dimensions)
	}
	if _, err := NewMetricsCollector(dimensions); err != nil {
		t.Fatalf("derived dimensions rejected: %v", err)
	}
}

func assertPrometheusTextShape(t *testing.T, text string) {
	t.Helper()
	for number, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		if strings.HasPrefix(line, "# HELP ") || strings.HasPrefix(line, "# TYPE ") {
			continue
		}
		if !strings.Contains(line, " ") || strings.Count(line, "{") != strings.Count(line, "}") {
			t.Fatalf("invalid Prometheus text line %d: %q", number+1, line)
		}
	}
}

func TestMetricsCollectorRejectsUnregisteredIdempotencyRenewalReason(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveIdempotencyRenewalRetired("database_was_slow"); err == nil {
		t.Fatal("unbounded retirement reason was accepted into a metric label")
	}
}

// TestObserveZeroUnitFinalizationRecordsByProviderAndReason (CHAOS-4175)
// pins the Go counterpart of Python's
// devhealth_sync_run_zero_unit_finalizations_total: it must actually appear
// in the exposition once observed, under the exact metric name Python uses.
func TestObserveZeroUnitFinalizationRecordsByProviderAndReason(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveZeroUnitFinalization("github", "pagerduty_credential_unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveZeroUnitFinalization("github", "pagerduty_credential_unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveZeroUnitFinalization("gitlab", "no_sync_units_planned"); err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	if !strings.Contains(text, "# HELP devhealth_sync_run_zero_unit_finalizations_total ") {
		t.Fatalf("missing HELP line:\n%s", text)
	}
	for _, want := range []string{
		`devhealth_sync_run_zero_unit_finalizations_total{provider="github",reason="pagerduty_credential_unavailable"} 2`,
		`devhealth_sync_run_zero_unit_finalizations_total{provider="gitlab",reason="no_sync_units_planned"} 1`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Fatalf("missing exposition line %q:\n%s", want, text)
		}
	}
}

// TestObserveZeroUnitFinalizationClampsUnknownProvider pins that an
// Integration.provider value outside the known set (e.g. a future provider
// not yet added here, or a genuinely missing one) reads as "unknown" --
// the exact residual _run_provider itself falls back to -- rather than
// growing the label set with an arbitrary string.
func TestObserveZeroUnitFinalizationClampsUnknownProvider(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveZeroUnitFinalization("some-future-provider", "no_sync_units_planned"); err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	if strings.Contains(text, "some-future-provider") {
		t.Fatalf("unknown provider leaked into exposition unclamped:\n%s", text)
	}
	want := `devhealth_sync_run_zero_unit_finalizations_total{provider="unknown",reason="no_sync_units_planned"} 1`
	if !strings.Contains(text, want+"\n") {
		t.Fatalf("missing clamped-provider exposition line %q:\n%s", want, text)
	}
}

// TestObserveZeroUnitFinalizationRejectsInvalidReason pins that a reason
// containing exposition-corrupting characters (a raw label injection
// attempt, or genuinely malformed input) is refused rather than accepted
// into a Prometheus label value verbatim.
func TestObserveZeroUnitFinalizationRejectsInvalidReason(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveZeroUnitFinalization("github", `bad"reason`); err == nil {
		t.Fatal("exposition-corrupting reason was accepted into a metric label")
	}
}

// TestObserveZeroUnitFinalizationCapsCardinality (CHAOS-4175) pins the
// escape valve for reason's open cardinality: past
// maxZeroUnitFinalizationSeries distinct (provider, reason) pairs, a NEW
// combination collapses into the fixed overflow bucket instead of growing
// the label set forever, while combinations already seen keep incrementing
// their own series.
func TestObserveZeroUnitFinalizationCapsCardinality(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < maxZeroUnitFinalizationSeries; index++ {
		reason := fmt.Sprintf("reason_%d", index)
		if err := collector.ObserveZeroUnitFinalization("github", reason); err != nil {
			t.Fatalf("observe %s: %v", reason, err)
		}
	}
	// One more already-seen reason: must NOT be treated as overflow.
	if err := collector.ObserveZeroUnitFinalization("github", "reason_0"); err != nil {
		t.Fatal(err)
	}
	// A genuinely new reason past the cap: must collapse to the overflow
	// bucket rather than becoming series number maxZeroUnitFinalizationSeries+1.
	if err := collector.ObserveZeroUnitFinalization("github", "reason_over_the_cap"); err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	if strings.Contains(text, "reason_over_the_cap") {
		t.Fatalf("overflow reason bypassed the cardinality cap:\n%s", text)
	}
	if !strings.Contains(text, `devhealth_sync_run_zero_unit_finalizations_total{provider="github",reason="reason_0"} 2`+"\n") {
		t.Fatalf("already-seen reason was miscounted as overflow:\n%s", text)
	}
	// The overflow key is provider-independent ("unknown", not "github"):
	// see TestObserveZeroUnitFinalizationOverflowBucketIsGlobalNotPerProvider
	// for why a per-provider overflow bucket would defeat the whole point of
	// the cap.
	if !strings.Contains(text, `devhealth_sync_run_zero_unit_finalizations_total{provider="unknown",reason="cardinality_capped"} 1`+"\n") {
		t.Fatalf("missing overflow-bucket exposition line:\n%s", text)
	}
}

// TestObserveZeroUnitFinalizationOverflowBucketIsGlobalNotPerProvider
// (CHAOS-4175, codex adversarial review) pins that the cardinality cap is a
// GLOBAL bound on total series, not a per-provider one: if the overflow key
// varied by provider, each distinct provider that independently exhausted
// the cap would mint its OWN overflow series, so the total series count
// could grow to maxZeroUnitFinalizationSeries + (number of distinct
// providers seen) -- silently exceeding the stated bound while every
// individual reason string still looked correctly capped.
func TestObserveZeroUnitFinalizationOverflowBucketIsGlobalNotPerProvider(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < maxZeroUnitFinalizationSeries; index++ {
		if err := collector.ObserveZeroUnitFinalization("github", fmt.Sprintf("reason_%d", index)); err != nil {
			t.Fatal(err)
		}
	}
	// Two DIFFERENT providers each overflow independently.
	if err := collector.ObserveZeroUnitFinalization("github", "github_overflow_reason"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveZeroUnitFinalization("gitlab", "gitlab_overflow_reason"); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveZeroUnitFinalization("jira", "jira_overflow_reason"); err != nil {
		t.Fatal(err)
	}
	seriesCount := 0
	for _, line := range strings.Split(collector.PrometheusText(), "\n") {
		if strings.HasPrefix(line, "devhealth_sync_run_zero_unit_finalizations_total{") {
			seriesCount++
		}
	}
	if want := maxZeroUnitFinalizationSeries + 1; seriesCount != want {
		t.Fatalf("total zero-unit-finalization series = %d, want exactly %d (cap + one SHARED overflow bucket, not one per provider that overflowed)",
			seriesCount, want)
	}
}

// TestObserveDailyMetricsFinalizeSweepRecordsDetectedAndFinalizedSeparately
// (CHAOS-4389) pins the two counters an operator/reconciler stranded-finalize
// sweep reports: "detected" (a run found stuck) and "finalized" (a fresh
// metrics.daily_finalize job actually enqueued for it) are DISTINCT metric
// names, not one metric split by a label -- so an alert on "how many did we
// actually move" cannot be silently satisfied by a pass that only detected.
func TestObserveDailyMetricsFinalizeSweepRecordsDetectedAndFinalizedSeparately(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveDailyMetricsFinalizeSweep("detected", 3); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveDailyMetricsFinalizeSweep("finalized", 2); err != nil {
		t.Fatal(err)
	}
	// A second pass accumulates rather than replacing.
	if err := collector.ObserveDailyMetricsFinalizeSweep("detected", 1); err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	for _, want := range []string{
		"# HELP dev_health_daily_metrics_stranded_finalize_runs_detected_total ",
		"# HELP dev_health_daily_metrics_runs_finalized_by_sweep_total ",
		"dev_health_daily_metrics_stranded_finalize_runs_detected_total 4",
		"dev_health_daily_metrics_runs_finalized_by_sweep_total 2",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing exposition content %q:\n%s", want, text)
		}
	}
}

// TestObserveDailyMetricsFinalizeSweepIsPreSeededAtZero ensures both series
// are present (not merely absent-until-observed) before anything ever
// strands, matching every other bounded-vocabulary observer in this package
// -- an alert must have something to bind to before the first incident.
func TestObserveDailyMetricsFinalizeSweepIsPreSeededAtZero(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	for _, want := range []string{
		"dev_health_daily_metrics_stranded_finalize_runs_detected_total 0",
		"dev_health_daily_metrics_runs_finalized_by_sweep_total 0",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing pre-seeded zero %q:\n%s", want, text)
		}
	}
}

func TestObserveDailyMetricsFinalizeSweepRejectsUnregisteredOutcomeAndNegativeCount(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveDailyMetricsFinalizeSweep("skipped", 1); err == nil {
		t.Fatal("unbounded finalize sweep outcome was accepted into a metric")
	}
	if err := collector.ObserveDailyMetricsFinalizeSweep("detected", -1); err == nil {
		t.Fatal("negative finalize sweep count was accepted")
	}
}
