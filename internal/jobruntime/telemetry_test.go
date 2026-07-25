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
	job := JobLabels{Profile: "ops", Queue: "retention", Kind: "system.retention_cleanup"}
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
	if err := RegisterRuntime(ctx, collector, RuntimeInfo{Version: "1.2.3", Commit: "abc123", Profile: "ops"}); err != nil {
		t.Fatalf("RegisterRuntime: %v", err)
	}
	if err := collector.SetJobsAvailable(job, 7); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetJobOldestAge("ops", "retention", 12*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveJobWait(job, 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := collector.SetExecutionSaturation("ops", 0.75); err != nil {
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
		`worker_runtime_info{version="1.2.3",commit="abc123",profile="ops"} 1`,
		`worker_jobs_available{profile="ops",queue="retention",kind="system.retention_cleanup"} 7`,
		`worker_job_oldest_age_seconds{profile="ops",queue="retention"} 12`,
		`worker_jobs_running{profile="ops",queue="retention",kind="system.retention_cleanup"} 0`,
		`worker_execution_saturation_ratio{profile="ops"} 0.75`,
		`worker_job_wait_seconds_bucket{profile="ops",queue="retention",kind="system.retention_cleanup",le="1"} 0`,
		`worker_job_wait_seconds_bucket{profile="ops",queue="retention",kind="system.retention_cleanup",le="2.5"} 1`,
		`worker_job_wait_seconds_sum{profile="ops",queue="retention",kind="system.retention_cleanup"} 1.5`,
		`worker_job_wait_seconds_count{profile="ops",queue="retention",kind="system.retention_cleanup"} 1`,
		`worker_job_duration_seconds_count{profile="ops",queue="retention",kind="system.retention_cleanup",result="success"} 1`,
		`worker_job_attempts_total{kind="system.retention_cleanup",result="cancel",error_category="permanent"} 1`,
		`worker_job_attempts_total{kind="system.retention_cleanup",result="retry",error_category="panic"} 1`,
		`worker_job_attempts_total{kind="system.retention_cleanup",result="success",error_category="none"} 1`,
		`worker_job_panics_total{kind="system.retention_cleanup"} 1`,
		`worker_job_cancellations_total{kind="system.retention_cleanup",reason="permanent"} 1`,
		`worker_domain_state_mismatch_total{domain_type="maintenance_run"} 1`,
		`worker_sync_lease_expired_total{provider="github",dataset_family="work_items",result="failed"} 0`,
		`worker_sync_lease_expired_total{provider="github",dataset_family="work_items",result="retrying"} 1`,
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
		"worker_domain_state_mismatch_total", "worker_sync_lease_expired_total", "worker_stream_lag", "worker_stream_pending",
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
	job := JobLabels{Profile: "ops", Queue: "heartbeat", Kind: "system.heartbeat"}
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
	unknownJob := JobLabels{Profile: "ops", Queue: "tenant-secret", Kind: "system.heartbeat"}
	if err := collector.SetJobsAvailable(unknownJob, 1); err == nil || strings.Contains(err.Error(), "tenant-secret") {
		t.Fatalf("unregistered label error is missing or unsafe: %v", err)
	}
	if err := collector.SetJobOldestAge("ops", "tenant-secret", time.Second); err == nil {
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
	if err := collector.SetDatabasePoolSaturation("tenant_pool", 0.5); err == nil {
		t.Fatal("unregistered pool accepted")
	}
	if err := collector.SetExecutionSaturation("unknown", 0.5); err == nil {
		t.Fatal("unregistered execution profile accepted")
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
	collector.RuntimeRegistered(context.Background(), RuntimeInfo{Version: "1.0.0", Commit: "abc", Profile: "unknown"})
	text := collector.PrometheusText()
	for _, forbidden := range []string{"tenant-secret", `result="cas_conflict"`, `error_category="secret"`, `profile="unknown"`, `provider="unknown"`} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("rejected observer label escaped into exposition: %s", forbidden)
		}
	}
}

func TestMetricsCollectorPreRegistersSyncLeaseSeriesInStableOrder(t *testing.T) {
	t.Parallel()
	collector, err := NewMetricsCollector(MetricDimensions{
		Profiles: []string{"sync"},
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
	job := JobLabels{Profile: "ops", Queue: "heartbeat", Kind: "system.heartbeat"}
	if _, err := NewMetricsCollector(MetricDimensions{Jobs: []JobLabels{job, job}}); err == nil {
		t.Fatal("duplicate job dimensions accepted")
	}
	if _, err := NewMetricsCollector(MetricDimensions{Jobs: []JobLabels{{Profile: "ops", Queue: "bad/queue", Kind: "system.heartbeat"}}}); err == nil {
		t.Fatal("unsafe job dimension accepted")
	}
	syncLease := SyncLeaseLabels{Provider: "github", DatasetFamily: "work_items"}
	if _, err := NewMetricsCollector(MetricDimensions{Profiles: []string{"ops"}, SyncLeases: []SyncLeaseLabels{syncLease, syncLease}}); err == nil {
		t.Fatal("duplicate sync lease dimensions accepted")
	}
	if _, err := NewMetricsCollector(MetricDimensions{Profiles: []string{"ops"}, SyncLeases: []SyncLeaseLabels{{Provider: "github", DatasetFamily: "tenant/work_items"}}}); err == nil {
		t.Fatal("unsafe sync lease dimension accepted")
	}
	jobs := make([]JobLabels, maxMetricJobs+1)
	for index := range jobs {
		jobs[index] = JobLabels{Profile: "ops", Queue: fmt.Sprintf("queue-%d", index), Kind: fmt.Sprintf("job.kind_%d", index)}
	}
	if _, err := NewMetricsCollector(MetricDimensions{Jobs: jobs}); err == nil {
		t.Fatal("unbounded job dimensions accepted")
	}
	syncLeases := make([]SyncLeaseLabels, maxMetricSyncLeases+1)
	for index := range syncLeases {
		syncLeases[index] = SyncLeaseLabels{Provider: "github", DatasetFamily: fmt.Sprintf("family_%d", index)}
	}
	if _, err := NewMetricsCollector(MetricDimensions{Profiles: []string{"ops"}, SyncLeases: syncLeases}); err == nil {
		t.Fatal("unbounded sync lease dimensions accepted")
	}
}

func TestMetricsCollectorSupportsStreamOnlyRuntimeAndWriter(t *testing.T) {
	t.Parallel()
	stream := StreamLabels{Stream: "external_ingest", ConsumerGroup: "sink_workers"}
	collector, err := NewMetricsCollector(MetricDimensions{
		Profiles: []string{"stream"},
		Streams:  []StreamLabels{stream},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterRuntime(context.Background(), collector, RuntimeInfo{Version: "1.0.0", Commit: "abc123", Profile: "stream"}); err != nil {
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
	if !strings.Contains(output.String(), `worker_runtime_info{version="1.0.0",commit="abc123",profile="stream"} 1`) ||
		!strings.Contains(output.String(), `worker_stream_lag{stream="external_ingest",consumer_group="sink_workers"} 5`) {
		t.Fatalf("stream-only exposition missing runtime/lag:\n%s", output.String())
	}
}

func TestMetricsCollectorConcurrentUpdates(t *testing.T) {
	job := JobLabels{Profile: "ops", Queue: "retention", Kind: "system.retention_cleanup"}
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
				_ = collector.SetJobOldestAge("ops", "retention", time.Duration(iteration)*time.Millisecond)
				_ = collector.SetExecutionSaturation("ops", 0.5)
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
	if !strings.Contains(text, `worker_jobs_running{profile="ops",queue="retention",kind="system.retention_cleanup"} 0`+"\n") {
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

func TestDimensionsForProfileUsesRegistryPolicy(t *testing.T) {
	t.Parallel()
	registry, err := Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	dimensions, err := DimensionsForProfile(registry, "ops", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(dimensions.Jobs) != 4 ||
		strings.Join(dimensions.DomainTypes, ",") != "billing_notification,maintenance_run,schedule_occurrence,webhook_delivery" {
		t.Fatalf("profile dimensions drifted: %+v", dimensions)
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
