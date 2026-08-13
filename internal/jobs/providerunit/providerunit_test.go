package providerunit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/google/uuid"
)

// leaseSyncDimensions is the bounded SyncLeaseLabels pair providerUnit()
// exercises (Provider: "launchdarkly", Dataset: "feature-flags").
func leaseSyncDimensions() jobruntime.MetricDimensions {
	return jobruntime.MetricDimensions{
		Profiles: []string{"sync"},
		SyncLeases: []jobruntime.SyncLeaseLabels{
			{Provider: "launchdarkly", DatasetFamily: "feature-flags"},
		},
	}
}

func TestEnabledProviderUnitExecutesCompleteRouteAndTerminalizes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(providerUnit())
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: successfulExecutor(t, now),
	}
	execution := providerExecution(repository.unit, now, 1)

	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	if repository.status != "success" || repository.attempt != 1 {
		t.Fatalf("repository status=%s attempt=%d", repository.status, repository.attempt)
	}
	if _, ok := repository.result["go_provider_route"]; !ok {
		t.Fatalf("terminal result=%#v", repository.result)
	}
	observations, ok := repository.result["go_worklog_observations"].([]providersync.JiraWorklogFetchObservation)
	if !ok || len(observations) != 1 || !observations[0].RESTFallbackUsed {
		t.Fatalf("persisted worklog observations=%#v", repository.result["go_worklog_observations"])
	}
}

// TestProviderUnitLifecycleLogsIdentifyTheClaimedScope proves the operator log
// seam uses the authoritative claim, not River's ID-only arguments. A mixed
// sync_provider queue otherwise exposes only the shared kind and queue, which
// cannot tell an operator which provider/dataset is draining or retrying.
func TestProviderUnitLifecycleLogsIdentifyTheClaimedScope(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	unit.Mode = "backfill"
	repository := newMemoryUnitRepository(unit)
	var output bytes.Buffer
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: successfulExecutor(t, now),
	}
	execution := providerExecution(unit, now, 1)
	execution.JobID = 42
	execution.Definition.Kind = jobcontract.KindSyncProviderUnit
	execution.Definition.Queue = "sync_provider"
	execution.Logger = slog.New(slog.NewJSONHandler(&output, nil))

	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatalf("Work() error = %v", err)
	}

	records := providerUnitLifecycleRecords(t, output.Bytes())
	if len(records) != 2 {
		t.Fatalf("lifecycle logs=%d want start and terminal records: %s", len(records), output.String())
	}
	for _, record := range records {
		for key, want := range map[string]any{
			"provider":     unit.Provider,
			"dataset":      unit.Dataset,
			"mode":         unit.Mode,
			"kind":         jobcontract.KindSyncProviderUnit,
			"queue":        "sync_provider",
			"job_id":       float64(42),
			"sync_run_id":  unit.SyncRunID,
			"sync_unit_id": unit.ID,
		} {
			if record[key] != want {
				t.Fatalf("record[%q]=%#v want %#v; record=%#v", key, record[key], want, record)
			}
		}
	}
	if records[0]["msg"] != "sync_provider_unit_started" || records[1]["msg"] != "sync_provider_unit_finished" || records[1]["result"] != "succeeded" {
		t.Fatalf("lifecycle records=%#v", records)
	}
}

func TestProviderUnitLifecycleLogsRetryAndFailureResults(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name, wantStatus, wantResult string
		attempt, maxAttempts         int
	}{
		{name: "retryable attempt", attempt: 1, maxAttempts: 5, wantStatus: "dispatching", wantResult: "retrying"},
		{name: "exhausted attempt", attempt: 5, maxAttempts: 5, wantStatus: "failed", wantResult: "failed"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			unit := providerUnit()
			unit.Mode = "backfill"
			repository := newMemoryUnitRepository(unit)
			var output bytes.Buffer
			errDetail := fmt.Errorf(
				"github files traversal failed for octo/hello: %w",
				errors.New("GET https://alice:secret@github.example.test/repos/octo/hello"),
			)
			handler := &Handler{
				Repository: repository,
				Switches: providersync.CompleteRouteSwitches{
					LaunchDarklyFeatureFlags: true,
				},
				LeaseDuration: time.Minute,
				Heartbeat:     10 * time.Second,
				Now:           func() time.Time { return now },
				BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
					return providersync.CompleteRouteExecutor{}, errDetail
				},
			}
			execution := providerExecution(unit, now, test.attempt)
			execution.JobID = 42
			execution.Definition.Kind = jobcontract.KindSyncProviderUnit
			execution.Definition.Queue = "sync_provider"
			execution.Definition.MaxAttempts = test.maxAttempts
			execution.Logger = slog.New(slog.NewJSONHandler(&output, nil))

			if err := handler.Work(context.Background(), execution); err == nil {
				t.Fatal("Work() error = nil, want retryable transport result")
			}
			if repository.status != test.wantStatus {
				t.Fatalf("unit status=%q want %q", repository.status, test.wantStatus)
			}
			records := providerUnitLifecycleRecords(t, output.Bytes())
			if len(records) != 2 || records[1]["msg"] != "sync_provider_unit_finished" || records[1]["result"] != test.wantResult {
				t.Fatalf("lifecycle records=%#v", records)
			}
			detail, ok := records[1]["error_detail"].(string)
			if !ok {
				t.Fatalf("missing error_detail in lifecycle records=%#v", records[1])
			}
			if !strings.Contains(detail, "github files traversal failed for octo/hello") {
				t.Fatalf("error_detail=%q want traversal context", detail)
			}
			if !strings.Contains(detail, "[REDACTED]") {
				t.Fatalf("error_detail=%q want redacted credential URL", detail)
			}
			if strings.Contains(detail, "alice:secret") {
				t.Fatalf("error_detail=%q leaked credentials", detail)
			}
		})
	}
}

func TestProviderBudgetContentionDefersWithoutConsumingTheRiverAttempt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, providerfoundation.ErrBudgetContended
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	delay, snoozed := jobruntime.SnoozeDelay(err)
	if !snoozed || delay < time.Second || delay >= 2*time.Second {
		t.Fatalf("Work() = %v, delay=%v; want typed 1s <= snooze < 2s", err, delay)
	}
	if repository.status != "dispatching" || repository.failures != 0 {
		t.Fatalf("contention status=%q failures=%d; must defer, not exhaust", repository.status, repository.failures)
	}
	if repository.releaseCalls != 0 || repository.contentionDeferrals != 1 {
		t.Fatalf("ordinary releases=%d contention deferrals=%d; want 0/1",
			repository.releaseCalls, repository.contentionDeferrals)
	}
	if !repository.availableAt.Equal(now.Add(delay)) {
		t.Fatalf("durable available_at=%v want %v", repository.availableAt, now.Add(delay))
	}
}

func TestProviderBudgetStoreUnavailableRemainsAnAttemptFailure(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, providerfoundation.ErrBudgetUnavailable
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	if _, snoozed := jobruntime.SnoozeDelay(err); snoozed {
		t.Fatalf("budget-store outage was incorrectly treated as contention: %v", err)
	}
	if repository.status != "failed" || repository.lastFailCategory != "provider_unit_exhausted" {
		t.Fatalf("store outage status=%q category=%q; want bounded failure",
			repository.status, repository.lastFailCategory)
	}
	if repository.contentionDeferrals != 0 {
		t.Fatalf("store outage wrote %d contention deferrals", repository.contentionDeferrals)
	}
}

func TestProviderDatasetUnavailableTerminalizesOnFirstAttempt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	unit.Provider = "gitlab"
	unit.Dataset = "feature-flags"
	unit.SourceExternalID = "group/project"
	unit.SourceName = "group/project"
	repository := newMemoryUnitRepository(unit)
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			GitlabFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, errors.Join(
				providersync.ErrProviderDatasetUnavailable,
				&providerfoundation.ProviderError{
					Class: providerfoundation.ErrorAuthentication, StatusCode: http.StatusForbidden,
				},
			)
		},
	}
	execution := providerExecution(unit, now, 1)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	if !errors.Is(err, providersync.ErrProviderDatasetUnavailable) {
		t.Fatalf("Work()=%v want unavailable dataset", err)
	}
	if repository.status != "failed" || repository.attempt != 1 ||
		repository.failures != 1 ||
		repository.lastFailCategory != ProviderDatasetUnavailableCategory {
		t.Fatalf(
			"status=%q attempt=%d failures=%d category=%q",
			repository.status, repository.attempt, repository.failures,
			repository.lastFailCategory,
		)
	}
	if repository.releaseCalls != 0 {
		t.Fatalf("unavailable dataset was released for retry %d times", repository.releaseCalls)
	}
}

func TestProviderBudgetContentionDelayIsDeterministicAndBounded(t *testing.T) {
	t.Parallel()
	first := providerBudgetContentionDelay("11111111-1111-4111-8111-111111111111")
	repeated := providerBudgetContentionDelay("11111111-1111-4111-8111-111111111111")
	sibling := providerBudgetContentionDelay("22222222-2222-4222-8222-222222222222")
	if first != repeated {
		t.Fatalf("same unit jitter changed: %v != %v", first, repeated)
	}
	for name, delay := range map[string]time.Duration{"first": first, "sibling": sibling} {
		if delay < time.Second || delay >= 2*time.Second {
			t.Fatalf("%s delay=%v; want 1s <= delay < 2s", name, delay)
		}
	}
	if first == sibling {
		t.Fatalf("known sibling units collided at %v; the regression fixture no longer proves spreading", first)
	}
}

func TestProviderUnitLifecycleLogsCaptureTraversalFailureDetail(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	var output bytes.Buffer
	wrappedErr := fmt.Errorf(
		"github files traversal failed for acme/api: %w",
		errors.New("GET https://alice:secret@github.example.test/repos/acme/api"),
	)
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, wrappedErr
		},
	}
	execution := providerExecution(unit, now, 1)
	execution.JobID = 42
	execution.Definition.Kind = jobcontract.KindSyncProviderUnit
	execution.Definition.Queue = "sync_provider"
	execution.Logger = slog.New(slog.NewJSONHandler(&output, nil))

	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work() error = nil, want retryable traversal failure")
	}

	records := providerUnitLifecycleRecords(t, output.Bytes())
	if len(records) != 2 {
		t.Fatalf("lifecycle logs=%d want start and terminal records: %s", len(records), output.String())
	}
	detail, ok := records[1]["error_detail"].(string)
	if !ok || !strings.Contains(detail, "github files traversal failed for acme/api") {
		t.Fatalf("lifecycle detail=%#v records=%#v", records[1]["error_detail"], records[1])
	}
}

func providerUnitLifecycleRecords(t *testing.T, output []byte) []map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(output), []byte{'\n'})
	records := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode lifecycle log %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func TestProviderUnitPersistsCanonicalProviderUsageObservations(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(providerUnit())
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: successfulExecutor(t, now),
	}
	if err := handler.Work(context.Background(), providerExecution(repository.unit, now, 1)); err != nil {
		t.Fatalf("Work() error = %v", err)
	}
	observations, ok := repository.result["observations"].(map[string]any)
	if !ok {
		t.Fatalf("persisted observations=%#v", repository.result["observations"])
	}
	usage, ok := observations["provider_usage"].([]any)
	if !ok || len(usage) != 1 {
		t.Fatalf("persisted provider usage=%#v", observations["provider_usage"])
	}
	observation, ok := usage[0].(map[string]any)
	if !ok || observation["transport"] != "rest" ||
		observation["route_family"] != "flags" ||
		observation["dimension"] != "rest_core" || observation["request_count"] != 4 {
		t.Fatalf("persisted provider usage observation=%#v", observation)
	}
}

func TestProviderUnitKeepsWatermarkUnadvancedWhenGitHubFilesTraversalFails(t *testing.T) {
	// Given
	t.Setenv("REPO_UUID", "")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(githubFilesUnit())
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			GithubFiles: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: githubFilesTraversalExecutor(now),
	}

	// When
	err := handler.Work(context.Background(), providerExecution(repository.unit, now, 1))

	// Then
	if err == nil {
		t.Fatal("Work() error = nil, want retryable traversal failure")
	}
	if repository.status != "dispatching" || repository.result != nil || repository.watermark != nil {
		t.Fatalf(
			"status=%s result=%#v watermark=%v, want retrying state without completed result or watermark",
			repository.status, repository.result, repository.watermark,
		)
	}
}

func TestProviderUnitRecordsGitHubFilesTraversalFailureAfterRetriesExhaust(t *testing.T) {
	// Given
	t.Setenv("REPO_UUID", "")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(githubFilesUnit())
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			GithubFiles: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: githubFilesTraversalExecutor(now),
	}
	execution := providerExecution(repository.unit, now, 5)
	execution.Definition.MaxAttempts = 5

	// When
	err := handler.Work(context.Background(), execution)

	// Then
	if err == nil {
		t.Fatal("Work() error = nil, want exhausted traversal failure")
	}
	if repository.status != "failed" || repository.lastFailCategory != GitHubFilesInventoryFailureCategory {
		t.Fatalf(
			"status=%s category=%s, want failed/%s",
			repository.status, repository.lastFailCategory, GitHubFilesInventoryFailureCategory,
		)
	}
}

func TestFreshHandlerRecoversExpiredProcessClaimAndReleasesForRiverRetry(
	t *testing.T,
) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(providerUnit())
	first, err := repository.Claim(context.Background(), providersync.ClaimRequest{
		UnitID: repository.unit.ID, OrgID: repository.unit.OrgID,
		Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
		AllowExpiredRecovery: true,
	})
	if err != nil || first.Recovered {
		t.Fatalf("first claim=%+v error=%v", first, err)
	}
	recoveryNow := now.Add(time.Minute + time.Second)
	// CHAOS-3118 evidence for worker_sync_lease_expired_total: a real
	// *jobruntime.MetricsCollector observes the durable resolution of this
	// recovered claim through the actual Handler.Work() path below, not a
	// direct setter call.
	collector, err := jobruntime.NewMetricsCollector(leaseSyncDimensions())
	if err != nil {
		t.Fatalf("NewMetricsCollector: %v", err)
	}
	fresh := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return recoveryNow },
		LeaseMetrics:  collector,
		BuildExecutor: func(
			*providersync.LeaseSession,
		) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, errors.New("transient")
		},
	}
	err = fresh.Work(
		context.Background(),
		providerExecution(repository.unit, recoveryNow, 2),
	)
	if err == nil || repository.attempt != 2 || !repository.lastClaim.Recovered ||
		repository.status != "dispatching" {
		t.Fatalf(
			"error=%v attempt=%d recovered=%v status=%s",
			err, repository.attempt, repository.lastClaim.Recovered, repository.status,
		)
	}
	text := collector.PrometheusText()
	if !strings.Contains(text, `worker_sync_lease_expired_total{provider="launchdarkly",dataset_family="feature-flags",result="retrying"} 1`) {
		t.Fatalf("expected a non-zero retrying series, got:\n%s", text)
	}
	if !strings.Contains(text, `worker_sync_lease_expired_total{provider="launchdarkly",dataset_family="feature-flags",result="failed"} 0`) {
		t.Fatalf("expected the failed series to stay at zero, got:\n%s", text)
	}
}

// TestRecoveredExpiredLeaseAttemptExhaustionRecordsFailedResolution is
// CHAOS-3118 evidence for the "failed" branch of worker_sync_lease_expired_total:
// a claim that itself recovered an expired lease, then exhausts its last
// River attempt, must durably resolve to "failed" and the collector's own
// ObserveSyncLeaseExpired contract (never record a failed CAS) must still
// hold — Repository.Fail here genuinely succeeds, so the series is observed.
func TestRecoveredExpiredLeaseAttemptExhaustionRecordsFailedResolution(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(providerUnit())
	if _, err := repository.Claim(context.Background(), providersync.ClaimRequest{
		UnitID: repository.unit.ID, OrgID: repository.unit.OrgID,
		Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
		AllowExpiredRecovery: true,
	}); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	recoveryNow := now.Add(time.Minute + time.Second)
	collector, err := jobruntime.NewMetricsCollector(leaseSyncDimensions())
	if err != nil {
		t.Fatalf("NewMetricsCollector: %v", err)
	}
	const maxAttempts = 2
	fresh := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return recoveryNow },
		LeaseMetrics:  collector,
		BuildExecutor: func(
			*providersync.LeaseSession,
		) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, errors.New("transient")
		},
	}
	execution := providerExecution(repository.unit, recoveryNow, maxAttempts)
	execution.Definition.MaxAttempts = maxAttempts
	err = fresh.Work(context.Background(), execution)
	if err == nil || repository.attempt != 2 || !repository.lastClaim.Recovered ||
		repository.status != "failed" || repository.lastFailCategory != "provider_unit_exhausted" {
		t.Fatalf(
			"error=%v attempt=%d recovered=%v status=%s category=%s",
			err, repository.attempt, repository.lastClaim.Recovered,
			repository.status, repository.lastFailCategory,
		)
	}
	text := collector.PrometheusText()
	if !strings.Contains(text, `worker_sync_lease_expired_total{provider="launchdarkly",dataset_family="feature-flags",result="failed"} 1`) {
		t.Fatalf("expected a non-zero failed series, got:\n%s", text)
	}
	if !strings.Contains(text, `worker_sync_lease_expired_total{provider="launchdarkly",dataset_family="feature-flags",result="retrying"} 0`) {
		t.Fatalf("expected the retrying series to stay at zero, got:\n%s", text)
	}
}

func providerExecution(
	unit providersync.Unit,
	now time.Time,
	attempt int,
) *jobruntime.Execution[jobruntime.ProviderUnitArgs] {
	organizationID := unit.OrgID
	payload := jobcontract.ProviderUnitPayload{UnitID: unit.ID}
	args := jobruntime.ProviderUnitArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.ProviderUnitPayload]{
			ContractVersion: 1,
			OrganizationID:  &organizationID,
			CorrelationID:   "sync-run:" + unit.SyncRunID,
			IdempotencyKey:  "sync.provider_unit:" + unit.ID,
			Domain: jobcontract.DomainLink{
				Type: "sync_run_unit", ID: unit.ID,
			},
			Payload: payload,
		},
	}
	return &jobruntime.Execution[jobruntime.ProviderUnitArgs]{
		Attempt: attempt, Args: args, Envelope: args.ContractEnvelope(),
		OrganizationID: &organizationID, Deadline: now.Add(10 * time.Minute),
		Definition: jobruntime.Descriptor{MaxAttempts: 5},
	}
}

func providerUnit() providersync.Unit {
	return providersync.Unit{
		ID: uuid.NewString(), SyncRunID: uuid.NewString(),
		OrgID: uuid.NewString(), IntegrationID: uuid.NewString(),
		SourceID: uuid.NewString(), SourceExternalID: "launchdarkly-project",
		SourceName: "LaunchDarkly project", Provider: "launchdarkly",
		Dataset: "feature-flags", CostClass: providersync.CostMedium,
		Mode: "incremental", ProcessorFlags: map[string]bool{},
		DatasetOptions: map[string]any{"project_key": "project"},
		Result:         map[string]any{}, SourceMetadata: map[string]any{},
		IntegrationConfig: map[string]any{},
		CredentialID:      uuid.NewString(), CredentialFingerprint: "fingerprint",
		AuthSource: "integration_credential",
	}
}

func githubFilesUnit() providersync.Unit {
	unit := providerUnit()
	capability, ok := providersync.Capability("github", "files")
	if !ok {
		panic("github/files capability missing")
	}
	unit.SourceExternalID = "acme/api"
	unit.SourceName = "acme/api"
	unit.Provider = "github"
	unit.Dataset = "files"
	unit.CostClass = capability.CostClass
	return unit
}

func successfulExecutor(
	t *testing.T,
	now time.Time,
) ExecutorFactory {
	t.Helper()
	return func(
		session *providersync.LeaseSession,
	) (providersync.CompleteRouteExecutor, error) {
		return providersync.CompleteRouteExecutor{
			Credentials: providerfoundation.CredentialResolver{
				Repository: testCredentialRepository{unit: session.Claim.Unit},
				Decryptor:  testCredentialDecryptor{},
			},
			Doer: testDoer{},
			Retry: providerfoundation.RetryPolicy{
				MaxAttempts: 1, InitialWait: time.Nanosecond,
				MaxWait: time.Nanosecond,
			},
			Budget: testBudgetStore{},
			BudgetLimits: map[providersync.CostClass]int{
				providersync.CostMedium: 1,
			},
			BudgetTTL: time.Minute,
			Gate: func(
				providersync.Claim,
				*providerfoundation.HTTPClient,
			) providerfoundation.BackoffGate {
				return testBackoffGate{}
			},
			Handler:           testCompleteRouteHandler{t: t, now: now, WorklogObservations: []providersync.JiraWorklogFetchObservation{{IssueKey: "FLAGS-1", RESTFallbackUsed: true}}},
			Comparator:        providersync.ProductionContractComparator{},
			Committer:         providersync.EffectCommitter{Ledger: &testEffectLedger{}, Sink: testEffectSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now }},
			HeartbeatInterval: 10 * time.Second,
			Now:               func() time.Time { return now },
		}, nil
	}
}

func githubFilesTraversalExecutor(now time.Time) ExecutorFactory {
	return func(
		session *providersync.LeaseSession,
	) (providersync.CompleteRouteExecutor, error) {
		return providersync.CompleteRouteExecutor{
			Credentials: providerfoundation.CredentialResolver{
				Repository: githubCredentialRepository{unit: session.Claim.Unit},
				Decryptor:  githubCredentialDecryptor{},
			},
			Doer: githubFilesTraversalDoer{},
			Retry: providerfoundation.RetryPolicy{
				MaxAttempts: 1, InitialWait: time.Nanosecond,
				MaxWait: time.Nanosecond,
			},
			Budget: testBudgetStore{},
			BudgetLimits: map[providersync.CostClass]int{
				providersync.CostMedium: 1,
			},
			BudgetTTL: time.Minute,
			Gate: func(
				providersync.Claim,
				*providerfoundation.HTTPClient,
			) providerfoundation.BackoffGate {
				return testBackoffGate{}
			},
			Handler:           providersync.GitHubFilesRouteHandler{},
			Comparator:        providersync.ProductionContractComparator{},
			Committer:         providersync.EffectCommitter{Ledger: &testEffectLedger{}, Sink: testEffectSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now }},
			HeartbeatInterval: 10 * time.Second,
			Now:               func() time.Time { return now },
		}, nil
	}
}

type memoryUnitRepository struct {
	mu                  sync.Mutex
	unit                providersync.Unit
	status              string
	attempt             int
	lastClaim           providersync.Claim
	result              map[string]any
	watermark           *time.Time
	failures            int
	lastFailCategory    string
	releaseErr          error
	releaseCalls        int
	contentionDeferrals int
	availableAt         time.Time
}

func newMemoryUnitRepository(unit providersync.Unit) *memoryUnitRepository {
	return &memoryUnitRepository{unit: unit, status: "dispatching"}
}

func (repository *memoryUnitRepository) Claim(
	_ context.Context,
	request providersync.ClaimRequest,
) (providersync.Claim, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if request.UnitID != repository.unit.ID || request.OrgID != repository.unit.OrgID {
		return providersync.Claim{}, providersync.ErrUnitNotClaimable
	}
	recovered := repository.status == "running" &&
		!repository.lastClaim.LeaseExpiresAt.After(request.Now)
	if repository.status != "dispatching" && !recovered {
		return providersync.Claim{}, providersync.ErrUnitNotClaimable
	}
	repository.attempt++
	repository.status = "running"
	repository.lastClaim = providersync.Claim{
		Unit: repository.unit, Owner: request.Owner,
		Attempt: repository.attempt, Recovered: recovered,
		LeaseExpiresAt: request.Now.Add(request.LeaseDuration),
	}
	return repository.lastClaim, nil
}

func (repository *memoryUnitRepository) Assert(
	_ context.Context,
	claim providersync.Claim,
	now time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" ||
		repository.lastClaim.Owner != claim.Owner ||
		!repository.lastClaim.LeaseExpiresAt.After(now) {
		return providersync.ErrLeaseLost
	}
	return nil
}

func (repository *memoryUnitRepository) Renew(
	_ context.Context,
	claim providersync.Claim,
	_ time.Time,
	expiresAt time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
	repository.lastClaim.LeaseExpiresAt = expiresAt
	return nil
}

func (repository *memoryUnitRepository) Complete(
	_ context.Context,
	claim providersync.Claim,
	result map[string]any,
	watermark *time.Time,
	_ time.Time,
	_ time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
	repository.status, repository.result, repository.watermark = "success", result, watermark
	return nil
}

func (repository *memoryUnitRepository) ReleaseForRetry(
	_ context.Context,
	claim providersync.Claim,
	_ time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	repository.releaseCalls++
	if repository.releaseErr != nil {
		return repository.releaseErr
	}
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
	repository.status = "dispatching"
	return nil
}

func (repository *memoryUnitRepository) DeferForBudgetContention(
	_ context.Context,
	claim providersync.Claim,
	availableAt time.Time,
	_ time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
	repository.contentionDeferrals++
	repository.availableAt = availableAt
	repository.status = "dispatching"
	return nil
}

func (repository *memoryUnitRepository) Fail(
	_ context.Context,
	claim providersync.Claim,
	category string,
	_ time.Time,
	_ time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
	repository.failures++
	repository.lastFailCategory = category
	repository.status = "failed"
	return nil
}

type testCredentialRepository struct{ unit providersync.Unit }

func (repository testCredentialRepository) ResolveEncrypted(
	_ context.Context,
	_ providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{
		ID: repository.unit.CredentialID, Provider: "launchdarkly",
		Name: "default", Active: true,
		Ciphertext: secrets.NewValue("ciphertext"),
	}, nil
}

type testCredentialDecryptor struct{}

func (testCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"api_key":"test-token"}`), nil
}

type githubCredentialRepository struct{ unit providersync.Unit }

func (repository githubCredentialRepository) ResolveEncrypted(
	_ context.Context,
	_ providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{
		ID: repository.unit.CredentialID, Provider: "github",
		Name: "default", Active: true,
		Ciphertext: secrets.NewValue("ciphertext"),
	}, nil
}

type githubCredentialDecryptor struct{}

func (githubCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"token":"test-token"}`), nil
}

type testDoer struct{}

func (testDoer) Do(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected request")
}

type githubFilesTraversalDoer struct{}

func (githubFilesTraversalDoer) Do(request *http.Request) (*http.Response, error) {
	switch request.URL.Path {
	case "/repos/acme/api":
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"full_name":"acme/api","default_branch":"main"}`)),
			Request:    request,
		}, nil
	case "/repos/acme/api/branches/main":
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"message":"tree traversal unavailable"}`)),
			Request:    request,
		}, nil
	default:
		return nil, errors.New("unexpected request")
	}
}

type testReservation struct{}

func (testReservation) Release(context.Context) error { return nil }

type testBudgetStore struct{}

func (testBudgetStore) Acquire(
	context.Context,
	providerfoundation.BudgetKey,
) (providerfoundation.Reservation, error) {
	return testReservation{}, nil
}

type testBackoffGate struct{}

func (testBackoffGate) Wait(context.Context) (time.Duration, error) { return 0, nil }
func (testBackoffGate) Penalize(context.Context, time.Duration) error {
	return nil
}

type testCompleteRouteHandler struct {
	t                   *testing.T
	now                 time.Time
	WorklogObservations []providersync.JiraWorklogFetchObservation
}

func (handler testCompleteRouteHandler) Collect(
	_ context.Context,
	claim providersync.Claim,
	_ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient,
	_ time.Time,
) (providersync.CompleteRouteBatch, error) {
	handler.t.Helper()
	effects := make([]providersync.EffectBatch, 0, 4)
	for _, destination := range []string{
		"feature_flag", "feature_flag_event",
		"feature_flag_link", "work_graph_edges",
	} {
		recovery := providersync.EffectReplaySafe
		if destination == "feature_flag_event" {
			recovery = providersync.EffectReadbackRequired
		}
		effect, err := providersync.BuildEffectBatch(
			destination, recovery,
			[]json.RawMessage{json.RawMessage(`{"org_id":"` + claim.OrgID + `"}`)},
		)
		if err != nil {
			handler.t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	watermark := handler.now
	return providersync.CompleteRouteBatch{
		Effects: effects, Result: map[string]any{
			"records": 4,
			"observations": map[string]any{
				"provider_usage": []any{map[string]any{
					"transport": "rest", "route_family": "flags",
					"dimension": "rest_core", "request_count": 4,
				}},
			},
		},
		Watermark:           &watermark,
		WorklogObservations: handler.WorklogObservations,
		Evidence: providersync.FetchEvidence{
			Provider: "launchdarkly", Dataset: "feature-flags", Records: 4,
		},
	}, nil
}

type testEffectLedger struct {
	state providersync.EffectLedgerState
}

func (*testEffectLedger) LoadEffects(
	context.Context,
	providersync.Claim,
	time.Time,
) (providersync.EffectLedgerState, error) {
	return providersync.EffectLedgerState{}, providersync.ErrEffectLedgerNotFound
}

func (ledger *testEffectLedger) PrepareEffects(
	_ context.Context,
	_ providersync.Claim,
	state providersync.EffectLedgerState,
	_ time.Time,
) (providersync.EffectLedgerState, error) {
	ledger.state = state
	return state, nil
}

func (*testEffectLedger) BeginEffect(
	context.Context, providersync.Claim, int, string, time.Time,
) error {
	return nil
}

func (*testEffectLedger) CommitEffect(
	context.Context, providersync.Claim, int, string, time.Time,
) error {
	return nil
}

func (*testEffectLedger) ResolveEffect(
	context.Context,
	providersync.Claim,
	int,
	string,
	providersync.GenerationBlockResolution,
	time.Time,
) error {
	return nil
}

type testEffectSink struct{}

func (testEffectSink) WriteEffect(
	context.Context,
	providersync.Claim,
	providersync.EffectBatch,
) error {
	return nil
}

type testEffectReadback struct{}

func (testEffectReadback) InspectEffect(
	context.Context,
	providersync.Claim,
	providersync.EffectBatch,
) (providersync.EffectInspection, error) {
	return providersync.EffectAbsent, nil
}
