package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
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
	descriptor, ok := Descriptor("launchdarkly", "feature-flags")
	if !ok || !descriptor.Plannable {
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
	if !result.Comparison.Match ||
		result.Effects.Written != 4 || len(sink.destinations) != 4 {
		t.Fatalf("result=%+v writes=%v", result, sink.destinations)
	}
}

func TestCompleteRouteExecutorBindsCredentialScopedEffectsBeforeCollection(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	bound := false
	handler := &effectsFactoryObservingHandler{
		bound: &bound, batch: completeRouteFixture(t, claim),
	}
	ledger := &memoryEffectLedger{}
	sink := &memoryEffectSink{}
	executor := completeRouteExecutor(now, handler, ledger, nil)
	executor.EffectsFactory = func(
		credential providerfoundation.Credential,
	) (EffectSink, EffectReadback, error) {
		if credential.Provider != "launchdarkly" || credential.ID != firstCredentialID {
			return nil, nil, ErrInvalidConfiguration
		}
		bound = true
		return sink, nil, nil
	}
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if !bound || result.Effects.Written != 4 || len(sink.destinations) != 4 {
		t.Fatalf("bound=%t result=%+v writes=%v", bound, result, sink.destinations)
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
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
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
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	descriptor, ok := Descriptor("linear", "work-item-comments")
	if !ok || !descriptor.RouteReady || descriptor.Plannable ||
		descriptor.RouteDataset != "work-items" {
		t.Fatalf("alias descriptor=%+v ok=%v", descriptor, ok)
	}
	claim, session := completeRouteSessionFor(
		t, now, false, "linear", "work-item-comments",
	)
	credentials := &trackingCompleteRouteCredentialRepository{provider: "linear"}
	budget := &trackingCompleteRouteBudget{}
	gate := &trackingCompleteRouteGate{}
	doer := &trackingCompleteRouteDoer{}
	executor := completeRouteExecutor(
		now, &requestingCompleteRouteHandler{batch: completeRouteFixture(t, claim)},
		nil, nil,
	)
	executor.Credentials.Repository = credentials
	executor.Budget = budget
	executor.Gate = func(
		Claim, *providerfoundation.HTTPClient,
	) providerfoundation.BackoffGate {
		return gate
	}
	executor.Doer = doer
	_, err := executor.Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("alias execution error=%v", err)
	}
	if credentials.resolves != 0 || budget.acquires != 0 || gate.waits != 0 ||
		doer.requests != 0 {
		t.Fatalf(
			"alias crossed preflight: credentials=%d budget=%d gate=%d requests=%d",
			credentials.resolves, budget.acquires, gate.waits, doer.requests,
		)
	}
}

// TestCompleteRouteExecutorRejectsNonPlannableDescriptorBeforeAnyIO replaces
// the pre-CHAOS-4054 "switch off -> shadow only" coverage. There is no route
// enablement plane left (CompleteRouteExecutionResult.ShadowOnly was
// deleted): a RouteReady-but-not-Plannable descriptor -- here github/tests,
// the alias of the canonical github/cicd writer -- must be rejected outright
// by Execute's precondition check, before any credential resolution, budget
// acquisition, gate wait, or provider request.
func TestCompleteRouteExecutorRejectsNonPlannableDescriptorBeforeAnyIO(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "tests")
	descriptor, ok := Descriptor("github", "tests")
	if !ok || !descriptor.RouteReady || descriptor.Plannable {
		t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
	}
	budget := &trackingCompleteRouteBudget{}
	gate := &trackingCompleteRouteGate{}
	doer := &trackingCompleteRouteDoer{}
	handler := &requestingCompleteRouteHandler{
		batch: completeRouteFixture(t, claim),
	}
	executor := completeRouteExecutor(now, handler, nil, nil)
	executor.Budget = budget
	executor.Gate = func(
		Claim,
		*providerfoundation.HTTPClient,
	) providerfoundation.BackoffGate {
		return gate
	}
	executor.Doer = doer
	_, err := executor.Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("non-plannable execution error=%v", err)
	}
	if doer.requests != 0 || budget.acquires != 0 || gate.waits != 0 {
		t.Fatalf(
			"non-plannable descriptor crossed preflight: requests=%d acquires=%d waits=%d",
			doer.requests, budget.acquires, gate.waits,
		)
	}
}

func TestCompleteRouteExecutorRejectsMissingOutboundDependencies(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	for _, test := range []struct {
		name   string
		mutate func(*CompleteRouteExecutor)
	}{
		{
			name: "credentials",
			mutate: func(executor *CompleteRouteExecutor) {
				executor.Credentials = providerfoundation.CredentialResolver{}
			},
		},
		{
			name: "budget",
			mutate: func(executor *CompleteRouteExecutor) {
				executor.Budget = nil
			},
		},
		{
			name: "gate",
			mutate: func(executor *CompleteRouteExecutor) {
				executor.Gate = nil
			},
		},
	} {
		executor := completeRouteExecutor(
			now,
			&staticCompleteRouteHandler{batch: completeRouteFixture(t, claim)},
			nil,
			nil,
		)
		test.mutate(&executor)
		_, err := executor.Execute(context.Background(), session, descriptor)
		if !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("%s error=%v", test.name, err)
		}
	}
}

func completeRouteSession(
	t *testing.T,
	now time.Time,
	recovered bool,
) (Claim, *LeaseSession) {
	t.Helper()
	return completeRouteSessionFor(
		t, now, recovered, "launchdarkly", "feature-flags",
	)
}

func completeRouteSessionFor(
	t *testing.T,
	now time.Time,
	recovered bool,
	provider string,
	dataset string,
) (Claim, *LeaseSession) {
	t.Helper()
	unit := nativeTestClaim(provider, dataset).Unit
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
	// Deliberately skewed: the committer must never stamp the persisted
	// ledger CreatedAt from its own clock, so pinning both to one fake value
	// would hide a clock split between collection and commit.
	return completeRouteExecutorWithCommitClock(
		now, now.Add(90*time.Second), handler, ledger, sink,
	)
}

func completeRouteExecutorWithCommitClock(
	now time.Time,
	commitNow time.Time,
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
			Ledger: ledger, Sink: sink,
			Now: func() time.Time { return commitNow },
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

type requestingCompleteRouteHandler struct {
	batch CompleteRouteBatch
}

type effectsFactoryObservingHandler struct {
	bound *bool
	batch CompleteRouteBatch
}

func (handler *effectsFactoryObservingHandler) Collect(
	_ context.Context,
	_ Claim,
	_ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient,
	_ time.Time,
) (CompleteRouteBatch, error) {
	if handler.bound == nil || !*handler.bound {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	return handler.batch, nil
}

func (handler *requestingCompleteRouteHandler) Collect(
	ctx context.Context,
	_ Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	_ time.Time,
) (CompleteRouteBatch, error) {
	response, err := client.Do(ctx, http.MethodGet, "/probe", nil)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	_ = response.Body.Close()
	return handler.batch, nil
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

type trackingCompleteRouteCredentialRepository struct {
	provider string
	resolves int
}

func (repository *trackingCompleteRouteCredentialRepository) ResolveEncrypted(
	context.Context,
	providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	repository.resolves++
	return providerfoundation.EncryptedCredential{
		ID: firstCredentialID, Provider: repository.provider, Name: "fixture",
		Active: true, Ciphertext: secrets.NewValue("opaque"),
		Config: map[string]string{"base_url": "https://fixture.test"},
	}, nil
}

type completeRouteCredentialDecryptor struct{}

func (completeRouteCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"api_key":"fixture-token","project_key":"payments"}`), nil
}

type trackingCompleteRouteBudget struct {
	acquires int
	releases int
}

func (budget *trackingCompleteRouteBudget) Acquire(
	_ context.Context,
	key providerfoundation.BudgetKey,
) (providerfoundation.Reservation, error) {
	if key.Validate() != nil {
		return nil, providerfoundation.ErrBudgetUnavailable
	}
	budget.acquires++
	return completeRouteTrackingReservation{budget: budget}, nil
}

type completeRouteTrackingReservation struct {
	budget *trackingCompleteRouteBudget
}

func (reservation completeRouteTrackingReservation) Release(context.Context) error {
	reservation.budget.releases++
	return nil
}

type trackingCompleteRouteGate struct {
	waits int
}

func (gate *trackingCompleteRouteGate) Wait(context.Context) (time.Duration, error) {
	gate.waits++
	return 0, nil
}

func (*trackingCompleteRouteGate) Penalize(context.Context, time.Duration) error {
	return nil
}

type trackingCompleteRouteDoer struct {
	requests int
}

func (doer *trackingCompleteRouteDoer) Do(
	*http.Request,
) (*http.Response, error) {
	doer.requests++
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(`{}`)),
	}, nil
}

var _ CompleteRouteHandler = (*staticCompleteRouteHandler)(nil)
var _ CompleteRouteHandler = (*requestingCompleteRouteHandler)(nil)
var _ CompleteRouteComparator = matchingCompleteRouteComparator{}

// TestCompleteRouteExecutorReusesPersistedNormalizationTimeOnOrdinaryRetry is
// the wedge regression. Effect digests cover the serialized rows, so a
// wall-clock timestamp regenerated on an ordinary River retry changes the
// digest and PrepareEffects rejects the manifest with ErrEffectLedgerConflict
// before any readback can run — permanently wedging the unit.
//
// ReleaseForRetry returns the unit to `dispatching`, so the next claim is NOT
// Recovered. Stabilization must therefore key off the persisted ledger, not
// the recovery flag.
func TestCompleteRouteExecutorReusesPersistedNormalizationTimeOnOrdinaryRetry(
	t *testing.T,
) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	if claim.Recovered {
		t.Fatal("this regression requires a non-recovered claim")
	}
	firstAttemptAt := now.Add(-3 * time.Minute)
	batch := completeRouteFixture(t, claim)
	state, err := NewEffectLedgerState(claim, batch.Effects, firstAttemptAt)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &memoryEffectLedger{state: state}
	handler := &staticCompleteRouteHandler{batch: batch}
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")

	if _, err := completeRouteExecutor(
		now, handler, ledger, &memoryEffectSink{},
	).Execute(context.Background(), session, descriptor); err != nil {
		t.Fatalf("ordinary retry error=%v", err)
	}

	if !handler.normalizedAt.Equal(firstAttemptAt) {
		t.Fatalf(
			"normalization time=%s want=%s (retry regenerated the digest)",
			handler.normalizedAt, firstAttemptAt,
		)
	}
}

// TestCompleteRouteExecutorStartsFreshWhenNoLedgerExists keeps the
// stabilization from swallowing a genuinely absent ledger.
func TestCompleteRouteExecutorStartsFreshWhenNoLedgerExists(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	handler := &staticCompleteRouteHandler{batch: completeRouteFixture(t, claim)}
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")

	if _, err := completeRouteExecutor(
		now, handler, &memoryEffectLedger{}, &memoryEffectSink{},
	).Execute(context.Background(), session, descriptor); err != nil {
		t.Fatal(err)
	}
	if !handler.normalizedAt.Equal(now) {
		t.Fatalf("normalization time=%s want=%s", handler.normalizedAt, now)
	}
}

// TestCompleteRouteExecutorPersistsTheCollectionInstantNotTheCommitClock is
// the clock-split regression. The executor picks the normalization instant
// before collection; the committer runs later. If the committer stamps the
// persisted ledger CreatedAt from its own clock, the ledger records an instant
// the rows were never built with — so the next attempt reloads that later
// instant, rebuilds different rows, and PrepareEffects rejects the digest.
// The wedge returns, one layer down from where round 2 fixed it.
func TestCompleteRouteExecutorPersistsTheCollectionInstantNotTheCommitClock(
	t *testing.T,
) {
	t.Parallel()
	collectedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	committedAt := collectedAt.Add(90 * time.Second)
	claim, session := completeRouteSession(t, collectedAt, false)
	ledger := &memoryEffectLedger{}
	handler := &staticCompleteRouteHandler{batch: completeRouteFixture(t, claim)}
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")

	if _, err := completeRouteExecutorWithCommitClock(
		collectedAt, committedAt, handler, ledger, &memoryEffectSink{},
	).Execute(context.Background(), session, descriptor); err != nil {
		t.Fatal(err)
	}

	if !handler.normalizedAt.Equal(collectedAt) {
		t.Fatalf("collection instant=%s want=%s", handler.normalizedAt, collectedAt)
	}
	if !ledger.state.CreatedAt.Equal(collectedAt) {
		t.Fatalf(
			"persisted ledger CreatedAt=%s want=%s (committer clock leaked in)",
			ledger.state.CreatedAt, collectedAt,
		)
	}
}

// TestCompleteRouteExecutorRetryReproducesTheDigestAcrossSkewedClocks closes
// the loop end to end: attempt one persists the ledger, attempt two runs with
// a completely different wall clock and must still rebuild the identical
// manifest. A regenerated instant would surface as ErrEffectLedgerConflict.
func TestCompleteRouteExecutorRetryReproducesTheDigestAcrossSkewedClocks(
	t *testing.T,
) {
	t.Parallel()
	firstAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, firstAt, false)
	ledger := &memoryEffectLedger{}
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")

	first := &staticCompleteRouteHandler{batch: completeRouteFixture(t, claim)}
	if _, err := completeRouteExecutorWithCommitClock(
		firstAt, firstAt.Add(30*time.Second), first, ledger, &memoryEffectSink{},
	).Execute(context.Background(), session, descriptor); err != nil {
		t.Fatal(err)
	}

	// A later River attempt: different executor clock, different committer
	// clock, same unit occurrence.
	retryAt := firstAt.Add(11 * time.Minute)
	_, retrySession := completeRouteSession(t, retryAt, false)
	second := &staticCompleteRouteHandler{batch: completeRouteFixture(t, claim)}
	if _, err := completeRouteExecutorWithCommitClock(
		retryAt, retryAt.Add(45*time.Second), second, ledger, &memoryEffectSink{},
	).Execute(context.Background(), retrySession, descriptor); err != nil {
		t.Fatalf("retry error=%v", err)
	}

	if !second.normalizedAt.Equal(firstAt) {
		t.Fatalf(
			"retry collection instant=%s want=%s", second.normalizedAt, firstAt,
		)
	}
	if !ledger.state.CreatedAt.Equal(firstAt) {
		t.Fatalf("ledger CreatedAt drifted to %s", ledger.state.CreatedAt)
	}
}
