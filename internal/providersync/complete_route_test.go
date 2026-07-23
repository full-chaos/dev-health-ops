package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

func TestCompleteRouteExecutorRunsEnabledMultiEffectUnit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	descriptor, ok := (CompleteRouteSwitches{
		LaunchDarklyFeatureFlags: true,
	}).Descriptor("launchdarkly", "feature-flags")
	if !ok || !descriptor.RouteEnabled {
		t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
	}
	handler := &staticCompleteRouteHandler{
		batch: completeRouteFixture(t, claim),
	}
	ledger := &memoryEffectLedger{}
	sink := &memoryEffectSink{}
	result, err := completeRouteExecutor(
		now, handler, ledger, sink,
	).Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if result.ShadowOnly || !result.Comparison.Match ||
		result.Effects.Written != 4 || len(sink.destinations) != 4 {
		t.Fatalf("result=%+v writes=%v", result, sink.destinations)
	}
}

func TestCompleteRouteExecutorReusesPersistedNormalizationTimeOnRecovery(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, true)
	createdAt := now.Add(-5 * time.Minute)
	batch := completeRouteFixture(t, claim)
	state, err := NewEffectLedgerState(claim, batch.Effects, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &memoryEffectLedger{state: state}
	handler := &staticCompleteRouteHandler{batch: batch}
	descriptor, _ := (CompleteRouteSwitches{
		LaunchDarklyFeatureFlags: true,
	}).Descriptor("launchdarkly", "feature-flags")
	_, err = completeRouteExecutor(
		now, handler, ledger, &memoryEffectSink{},
	).Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if !handler.normalizedAt.Equal(createdAt) {
		t.Fatalf("normalization time=%s want=%s", handler.normalizedAt, createdAt)
	}
}

func TestCompleteRouteExecutorRejectsAliasActivation(t *testing.T) {
	t.Parallel()
	descriptor, ok := (CompleteRouteSwitches{
		LinearWorkItems: true,
	}).Descriptor("linear", "work-item-comments")
	if !ok || descriptor.RouteReady || descriptor.RouteEnabled {
		t.Fatalf("alias descriptor=%+v ok=%v", descriptor, ok)
	}
}

func completeRouteSession(
	t *testing.T,
	now time.Time,
	recovered bool,
) (Claim, *LeaseSession) {
	t.Helper()
	unit := nativeTestClaim("launchdarkly", "feature-flags").Unit
	status := "dispatching"
	if recovered {
		status = "running"
	}
	leases := newMemoryLeaseRepository(unit, status)
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: recovered,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim, &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
}

func completeRouteExecutor(
	now time.Time,
	handler CompleteRouteHandler,
	ledger EffectLedger,
	sink EffectSink,
) CompleteRouteExecutor {
	return CompleteRouteExecutor{
		Credentials: providerfoundation.CredentialResolver{
			Repository: completeRouteCredentialRepository{},
			Decryptor:  completeRouteCredentialDecryptor{},
		},
		Doer: noRequestDoer{},
		Retry: providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		Budget:       executorBudgetStore{},
		BudgetLimits: map[CostClass]int{CostMedium: 1},
		BudgetTTL:    time.Minute,
		Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
			return executorBackoffGate{}
		},
		Handler: handler, Comparator: matchingCompleteRouteComparator{},
		Committer: EffectCommitter{
			Ledger: ledger, Sink: sink, Now: func() time.Time { return now },
		},
		HeartbeatInterval: 30 * time.Second,
		Now:               func() time.Time { return now },
	}
}

func completeRouteFixture(t *testing.T, claim Claim) CompleteRouteBatch {
	t.Helper()
	destinations := []struct {
		name   string
		policy EffectRecoveryPolicy
	}{
		{"feature_flag", EffectReplaySafe},
		{"feature_flag_event", EffectReadbackRequired},
		{"feature_flag_link", EffectReplaySafe},
		{"work_graph_edges", EffectReplaySafe},
	}
	effects := make([]EffectBatch, 0, len(destinations))
	for _, destination := range destinations {
		effect, err := BuildEffectBatch(
			destination.name,
			destination.policy,
			[]json.RawMessage{json.RawMessage(
				`{"org_id":"` + claim.OrgID + `","destination":"` +
					destination.name + `"}`,
			)},
		)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	return CompleteRouteBatch{
		Effects: effects,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Records: 4,
		},
	}
}

type staticCompleteRouteHandler struct {
	batch        CompleteRouteBatch
	normalizedAt time.Time
}

func (handler *staticCompleteRouteHandler) Collect(
	_ context.Context,
	_ Claim,
	_ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	handler.normalizedAt = normalizedAt
	return handler.batch, nil
}

type matchingCompleteRouteComparator struct{}

func (matchingCompleteRouteComparator) CompareCompleteRoute(
	context.Context,
	Claim,
	CompleteRouteBatch,
) (ShadowComparison, error) {
	return ShadowComparison{Match: true}, nil
}

type completeRouteCredentialRepository struct{}

func (completeRouteCredentialRepository) ResolveEncrypted(
	context.Context,
	providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{
		ID: firstCredentialID, Provider: "launchdarkly", Name: "fixture",
		Active: true, Ciphertext: secrets.NewValue("opaque"),
		Config: map[string]string{"base_url": "https://fixture.test"},
	}, nil
}

type completeRouteCredentialDecryptor struct{}

func (completeRouteCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"api_key":"fixture-token","project_key":"payments"}`), nil
}

var _ CompleteRouteHandler = (*staticCompleteRouteHandler)(nil)
var _ CompleteRouteComparator = matchingCompleteRouteComparator{}
