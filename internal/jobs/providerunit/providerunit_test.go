package providerunit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
			Handler:           testCompleteRouteHandler{t: t, now: now},
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
	mu               sync.Mutex
	unit             providersync.Unit
	status           string
	attempt          int
	lastClaim        providersync.Claim
	result           map[string]any
	watermark        *time.Time
	failures         int
	lastFailCategory string
	releaseErr       error
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
	if repository.releaseErr != nil {
		return repository.releaseErr
	}
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
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
	t   *testing.T
	now time.Time
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
		Effects: effects, Result: map[string]any{"records": 4},
		Watermark: &watermark,
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
