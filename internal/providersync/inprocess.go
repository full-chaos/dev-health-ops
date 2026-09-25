package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// This file runs ONE provider-unit route in the caller's own process, with no
// Postgres sync_run/unit rows and no worker: the shape `dho sync <target>`
// needs (the Python `dev-hops sync <target>` never touched a scheduler either).
// It is the worker's executor with its durable collaborators replaced by
// in-process ones:
//
//   - the lease is a LeaseRepository that grants for the life of the process;
//   - the effect ledger keeps one attempt's manifest in memory, so an attempt
//     never resumes (a crash re-runs the command, which is idempotent through
//     the same ReplacingMergeTree generation keys the worker relies on);
//   - the provider budget is a per-key semaphore and the backoff gate a local
//     timer, so two concurrent CLI runs do not share a budget (the worker's is
//     Valkey);
//   - the credential is the caller's, delivered through the same encrypted-
//     credential resolver seam with an identity decryptor.
//
// Everything else -- the route handler, the effect sink, the readback, the
// comparator, the committer and Execute itself -- is the worker's, selected
// through SelectGitFamilyRoute.

// InProcessLeaseRepository grants every claim, assert and renewal: the process
// is the only claimant, so there is no one to lose the lease to.
type InProcessLeaseRepository struct{}

func (InProcessLeaseRepository) Claim(_ context.Context, request ClaimRequest) (Claim, error) {
	return Claim{}, fmt.Errorf("%w: an in-process run builds its claim itself", ErrInvalidConfiguration)
}

func (InProcessLeaseRepository) Assert(context.Context, Claim, time.Time) error { return nil }

func (InProcessLeaseRepository) Renew(context.Context, Claim, time.Time, time.Time) error {
	return nil
}

// InProcessEffectLedger is the effect manifest of a single in-process attempt.
// It implements EffectLedger, PreparedEffectLedger and EffectLedgerReplanner
// with the same transitions and conflict rules the Postgres ledger enforces, so
// the committer cannot tell it from the durable one; it just forgets when the
// process exits. One ledger belongs to one claim.
type InProcessEffectLedger struct {
	mu       sync.Mutex
	state    EffectLedgerState
	snapshot []byte
}

var (
	_ EffectLedger          = (*InProcessEffectLedger)(nil)
	_ PreparedEffectLedger  = (*InProcessEffectLedger)(nil)
	_ EffectLedgerReplanner = (*InProcessEffectLedger)(nil)
)

func (ledger *InProcessEffectLedger) LoadEffects(context.Context, Claim, time.Time) (EffectLedgerState, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.state.SchemaVersion == "" {
		return EffectLedgerState{}, ErrEffectLedgerNotFound
	}
	return cloneEffectLedgerState(ledger.state), nil
}

func (ledger *InProcessEffectLedger) PrepareEffects(
	_ context.Context, _ Claim, desired EffectLedgerState, _ time.Time,
) (EffectLedgerState, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.state.SchemaVersion == "" {
		ledger.state = cloneEffectLedgerState(desired)
		return cloneEffectLedgerState(ledger.state), nil
	}
	if !sameEffectManifest(ledger.state, desired) {
		return EffectLedgerState{}, ErrEffectLedgerConflict
	}
	return cloneEffectLedgerState(ledger.state), nil
}

func (ledger *InProcessEffectLedger) PrepareRouteSnapshot(
	_ context.Context, claim Claim, batch CompleteRouteBatch, comparison ShadowComparison, normalizedAt time.Time,
) (EffectLedgerState, error) {
	payload, reference, err := encodePreparedRouteManifest(claim, batch, comparison, normalizedAt)
	if err != nil {
		return EffectLedgerState{}, err
	}
	desired, err := NewEffectLedgerState(claim, batch.Effects, normalizedAt)
	if err != nil {
		return EffectLedgerState{}, err
	}
	desired.SchemaVersion = "v2"
	desired.PreparedSnapshot = &reference
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.state.SchemaVersion != "" {
		if !sameEffectManifest(ledger.state, desired) || !bytes.Equal(ledger.snapshot, payload) {
			return EffectLedgerState{}, ErrEffectLedgerConflict
		}
		return cloneEffectLedgerState(ledger.state), nil
	}
	ledger.state = desired
	ledger.snapshot = append([]byte(nil), payload...)
	return cloneEffectLedgerState(ledger.state), nil
}

func (ledger *InProcessEffectLedger) LoadRouteSnapshot(
	_ context.Context, claim Claim, state EffectLedgerState, _ time.Time,
) (PreparedRouteManifest, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.snapshot) == 0 {
		return PreparedRouteManifest{}, ErrPreparedRouteSnapshotNotFound
	}
	return decodePreparedRouteManifest(ledger.snapshot, claim, state)
}

func (ledger *InProcessEffectLedger) transition(index int, digest string, from GenerationBlockStatus) (*EffectLedgerEntry, error) {
	if index < 0 || index >= len(ledger.state.Effects) {
		return nil, ErrEffectLedgerConflict
	}
	effect := &ledger.state.Effects[index]
	if effect.ContentDigest != digest || effect.Status != from {
		return nil, ErrEffectLedgerConflict
	}
	return effect, nil
}

func (ledger *InProcessEffectLedger) BeginEffect(_ context.Context, _ Claim, index int, digest string, now time.Time) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	effect, err := ledger.transition(index, digest, GenerationBlockPending)
	if err != nil {
		return err
	}
	effect.Status = GenerationBlockWriting
	at := now.UTC()
	effect.StartedAt = &at
	return nil
}

func (ledger *InProcessEffectLedger) CommitEffect(_ context.Context, _ Claim, index int, digest string, now time.Time) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	effect, err := ledger.transition(index, digest, GenerationBlockWriting)
	if err != nil {
		return err
	}
	effect.Status = GenerationBlockCommitted
	at := now.UTC()
	effect.CommittedAt = &at
	return nil
}

func (ledger *InProcessEffectLedger) ResolveEffect(
	_ context.Context, _ Claim, index int, digest string, resolution GenerationBlockResolution, now time.Time,
) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	effect, err := ledger.transition(index, digest, GenerationBlockWriting)
	if err != nil {
		return err
	}
	at := now.UTC()
	switch resolution {
	case GenerationBlockMarkCommitted:
		effect.Status = GenerationBlockCommitted
		effect.CommittedAt = &at
	case GenerationBlockRetryPending:
		effect.Status = GenerationBlockPending
		effect.StartedAt = nil
	default:
		return ErrInvalidConfiguration
	}
	return nil
}

// ResetPreparedEffectsForReplan forgets the manifest so the route replays from
// the claim, under the SAME safety rules as the durable ledger: only a manifest
// that isSafeReplanState admits (nothing in it committed) and that equals the
// caller's expected state exactly. An in-process attempt starts empty and never
// resumes, so the executor does not reach this in practice; it is implemented
// (not stubbed) so the type satisfies the interface the executor asserts and
// cannot discard a committed write if a future caller does reach it.
func (ledger *InProcessEffectLedger) ResetPreparedEffectsForReplan(
	_ context.Context, claim Claim, expected EffectLedgerState, now time.Time,
) error {
	if !isSafeReplanState(claim, expected) || now.IsZero() {
		return ErrInvalidConfiguration
	}
	expectedEncoded := encodeEffectLedgerState(expected)
	if len(expectedEncoded) == 0 {
		return ErrEffectLedgerConflict
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.state.SchemaVersion == "" ||
		!bytes.Equal(encodeEffectLedgerState(ledger.state), expectedEncoded) ||
		!isSafeReplanState(claim, ledger.state) {
		return ErrEffectLedgerConflict
	}
	ledger.state = EffectLedgerState{}
	ledger.snapshot = nil
	return nil
}

func cloneEffectLedgerState(state EffectLedgerState) EffectLedgerState {
	state.Effects = append([]EffectLedgerEntry(nil), state.Effects...)
	return state
}

// InProcessBudgetStore bounds concurrent provider requests per budget key with
// a semaphore. The worker's store is Valkey and spans processes; this one spans
// the goroutines of one command.
type InProcessBudgetStore struct {
	mu    sync.Mutex
	slots map[string]chan struct{}
}

type inProcessReservation struct {
	once    sync.Once
	release func()
}

func (reservation *inProcessReservation) Release(context.Context) error {
	reservation.once.Do(reservation.release)
	return nil
}

func (store *InProcessBudgetStore) Acquire(ctx context.Context, key providerfoundation.BudgetKey) (providerfoundation.Reservation, error) {
	if key.Limit < 1 {
		return nil, ErrInvalidConfiguration
	}
	name := fmt.Sprintf("%s|%s|%s|%s|%d", key.Provider, key.OrgID, key.Host, key.CostClass, key.Limit)
	store.mu.Lock()
	if store.slots == nil {
		store.slots = map[string]chan struct{}{}
	}
	slots, ok := store.slots[name]
	if !ok {
		slots = make(chan struct{}, key.Limit)
		store.slots[name] = slots
	}
	store.mu.Unlock()
	select {
	case slots <- struct{}{}:
		return &inProcessReservation{release: func() { <-slots }}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// InProcessBackoffGate delays requests after a provider rate-limit signal.
type InProcessBackoffGate struct {
	mu         sync.Mutex
	until      time.Time
	MaxBackoff time.Duration
	Now        func() time.Time
}

func (gate *InProcessBackoffGate) now() time.Time {
	if gate.Now != nil {
		return gate.Now()
	}
	return time.Now()
}

func (gate *InProcessBackoffGate) Wait(ctx context.Context) (time.Duration, error) {
	gate.mu.Lock()
	wait := gate.until.Sub(gate.now())
	gate.mu.Unlock()
	if wait <= 0 {
		return 0, nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return wait, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (gate *InProcessBackoffGate) Penalize(_ context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if gate.MaxBackoff > 0 && delay > gate.MaxBackoff {
		delay = gate.MaxBackoff
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if until := gate.now().Add(delay); until.After(gate.until) {
		gate.until = until
	}
	return nil
}

// staticCredentialRepository and identityDecryptor deliver one caller-supplied
// credential through the worker's credential-resolver seam unchanged.
type staticCredentialRepository struct {
	record providerfoundation.EncryptedCredential
}

func (repository staticCredentialRepository) ResolveEncrypted(context.Context, providerfoundation.TenantScope) (providerfoundation.EncryptedCredential, error) {
	return repository.record, nil
}

type identityDecryptor struct{}

func (identityDecryptor) Decrypt(value secrets.Value) ([]byte, error) {
	return []byte(value.Reveal()), nil
}

// InProcessRun is one provider-unit route to run in this process.
type InProcessRun struct {
	OrgID    string
	Provider string
	Dataset  string
	// SourceExternalID is "owner/repo" for GitHub and the numeric project id
	// for GitLab; SourceName is the display name.
	SourceExternalID string
	SourceName       string
	SinceAt          *time.Time
	BeforeAt         time.Time
	// Credential is the provider credential's fields (for GitHub `token`, or
	// `app_id` / `private_key` / `installation_id`; for GitLab `token`) and
	// Config its non-secret settings (`base_url`).
	Credential map[string]string
	Config     map[string]string
	Conn       driver.Conn
	// Doer is the HTTP client the route uses; nil builds the worker's own
	// (45 s timeout, redirects refused).
	Doer providerfoundation.HTTPDoer
	// Retry is the request retry policy; the zero value is the worker's default.
	Retry providerfoundation.RetryPolicy
	// GitHubTestsMaxArtifactBytes mirrors GitFamilyDeps.
	GitHubTestsMaxArtifactBytes int64
	Now                         func() time.Time
}

// ErrNotAGitFamilyRoute is returned for a (provider, dataset) pair that has no
// in-process route.
var ErrNotAGitFamilyRoute = errors.New("providersync: not a git-family route")

// inProcessBudgetLimits are the worker's per-cost-class concurrency limits.
var inProcessBudgetLimits = map[CostClass]int{CostLight: 4, CostMedium: 2, CostHeavy: 1}

// RunInProcess runs one git-family route through the worker's executor with
// in-process collaborators, and returns what the executor returns.
func RunInProcess(ctx context.Context, run InProcessRun) (CompleteRouteExecutionResult, error) {
	if run.Conn == nil || run.OrgID == "" || run.SourceExternalID == "" || len(run.Credential) == 0 {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	descriptor, known := Descriptor(run.Provider, run.Dataset)
	capability, capable := Capability(run.Provider, run.Dataset)
	if !known || !capable || !descriptor.RouteReady {
		return CompleteRouteExecutionResult{}, ErrNotAGitFamilyRoute
	}
	now := run.Now
	if now == nil {
		now = time.Now
	}
	started := now().UTC()
	claim := Claim{
		Unit: Unit{
			ID: uuid.NewString(), SyncRunID: uuid.NewString(), OrgID: run.OrgID,
			IntegrationID: uuid.NewString(), SourceID: uuid.NewString(),
			SourceExternalID: run.SourceExternalID, SourceName: run.SourceName,
			Provider: run.Provider, Dataset: run.Dataset, CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: run.SinceAt, BeforeAt: &run.BeforeAt,
			ProcessorFlags: capability.ProcessorFlags,
			CredentialID:   uuid.NewString(), AuthSource: "cli",
		},
		Owner: uuid.NewString(), Attempt: 1, LeaseExpiresAt: started.Add(10 * time.Minute),
	}
	if err := claim.Validate(); err != nil {
		return CompleteRouteExecutionResult{}, err
	}
	session := &LeaseSession{
		Repository: InProcessLeaseRepository{}, Claim: claim,
		LeaseDuration: 10 * time.Minute, Deadline: started.Add(24 * time.Hour), Now: now,
	}
	route, ok := SelectGitFamilyRoute(run.Provider, run.Dataset, GitFamilyDeps{
		Conn: run.Conn, Lease: session, GitHubTestsMaxArtifactBytes: run.GitHubTestsMaxArtifactBytes,
	})
	if !ok {
		return CompleteRouteExecutionResult{}, ErrNotAGitFamilyRoute
	}
	plaintext, err := json.Marshal(run.Credential)
	if err != nil {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	doer := inProcessHTTPDoer(run.Doer)
	retry := run.Retry
	if retry.MaxAttempts == 0 {
		retry = providerfoundation.DefaultRetryPolicy()
	}
	budget := &InProcessBudgetStore{}
	executor := CompleteRouteExecutor{
		Credentials: providerfoundation.CredentialResolver{
			Repository: staticCredentialRepository{record: providerfoundation.EncryptedCredential{
				ID: claim.CredentialID, Provider: run.Provider, Name: "cli", Active: true,
				Ciphertext: secrets.NewValue(string(plaintext)), Config: run.Config,
			}},
			Decryptor: identityDecryptor{},
		},
		Doer: doer, Retry: retry, Budget: budget, BudgetLimits: inProcessBudgetLimits,
		BudgetTTL: 15 * time.Minute,
		Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
			return &InProcessBackoffGate{MaxBackoff: 5 * time.Minute, Now: now}
		},
		Metrics: providerfoundation.NewMetrics(),
		Handler: route.Handler, Comparator: ProductionContractComparator{},
		Committer:         EffectCommitter{Ledger: &InProcessEffectLedger{}, Sink: route.Sink, Readback: route.Readback, Now: now},
		HeartbeatInterval: 30 * time.Second, Now: now,
	}
	return executor.Execute(ctx, session, descriptor)
}

// inProcessHTTPDoer is the caller's HTTP client, or the worker's own (45 s timeout,
// redirects refused).
func inProcessHTTPDoer(doer providerfoundation.HTTPDoer) providerfoundation.HTTPDoer {
	if doer != nil {
		return doer
	}
	return &http.Client{
		Timeout: 45 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
