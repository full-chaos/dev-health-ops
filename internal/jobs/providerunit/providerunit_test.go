package providerunit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
	fresh := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return recoveryNow },
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
			Committer:         providersync.EffectCommitter{Ledger: &testEffectLedger{}, Sink: testEffectSink{}, Now: func() time.Time { return now }},
			HeartbeatInterval: 10 * time.Second,
			Now:               func() time.Time { return now },
		}, nil
	}
}

type memoryUnitRepository struct {
	mu        sync.Mutex
	unit      providersync.Unit
	status    string
	attempt   int
	lastClaim providersync.Claim
	result    map[string]any
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
	_ *time.Time,
	_ time.Time,
	_ time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
	repository.status, repository.result = "success", result
	return nil
}

func (repository *memoryUnitRepository) ReleaseForRetry(
	_ context.Context,
	claim providersync.Claim,
	_ time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
	repository.status = "dispatching"
	return nil
}

func (repository *memoryUnitRepository) Fail(
	_ context.Context,
	claim providersync.Claim,
	_ string,
	_ time.Time,
	_ time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
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

type testDoer struct{}

func (testDoer) Do(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected request")
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
