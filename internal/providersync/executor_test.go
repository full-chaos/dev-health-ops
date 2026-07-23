package providersync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

func TestExecutorUsesCredentialLeaseShadowBudgetJournalAndGenerationSink(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	unit := nativeTestClaim("github", "repo-metadata").Unit
	leases := newMemoryLeaseRepository(unit, "dispatching")
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
	doer := &fixtureDoer{t: t, provider: "github", authorization: "token fixture-token"}
	journal := &memoryGenerationJournal{}
	sink := &memoryGenerationSink{}
	executor := Executor{
		Credentials: providerfoundation.CredentialResolver{
			Repository: executorCredentialRepository{},
			Decryptor:  executorCredentialDecryptor{},
		},
		Doer: doer,
		Retry: providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		Budget:       executorBudgetStore{},
		BudgetLimits: map[CostClass]int{CostLight: 2},
		BudgetTTL:    time.Minute,
		Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
			return executorBackoffGate{}
		},
		Handler:    NativeRESTHandler{Now: func() time.Time { return now }},
		Comparator: matchingComparator{},
		Journal:    journal,
		Sink: func(providerfoundation.LeaseGuard) providerfoundation.GenerationSink {
			return sink
		},
		Destination: "provider_records", HeartbeatInterval: 30 * time.Second,
		Now: func() time.Time { return now },
	}
	descriptor, ok := (RouteSwitches{GitHub: true}).Descriptor("github", "repo-metadata")
	if !ok || !descriptor.RouteEnabled {
		t.Fatal("GitHub native route was not explicitly enabled")
	}
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if result.ShadowOnly || !result.Comparison.Match || result.Fetch.Records != 1 ||
		result.BlocksWritten != 1 || result.BlocksSkipped != 0 ||
		len(sink.blocks) != 1 || journal.state.Blocks[0].Status != GenerationBlockCommitted {
		t.Fatalf("result=%+v sink_blocks=%d journal=%+v", result, len(sink.blocks), journal.state)
	}
	if doer.requests != 1 {
		t.Fatalf("provider requests=%d", doer.requests)
	}
}

func TestExecutorDormantRouteRunsShadowWithoutSinkSideEffects(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	unit := nativeTestClaim("github", "repo-metadata").Unit
	leases := newMemoryLeaseRepository(unit, "dispatching")
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
	executor := Executor{
		Credentials: providerfoundation.CredentialResolver{
			Repository: executorCredentialRepository{},
			Decryptor:  executorCredentialDecryptor{},
		},
		Doer: &fixtureDoer{t: t, provider: "github"},
		Retry: providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		Handler:           NativeRESTHandler{Now: func() time.Time { return now }},
		Comparator:        matchingComparator{},
		HeartbeatInterval: 30 * time.Second,
	}
	descriptor, _ := (RouteSwitches{}).Descriptor("github", "repo-metadata")
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil || !result.ShadowOnly || result.BlocksWritten != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestExecutorRefusesAmbiguousWritingBlockOutsideFiniteDedupeWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	unit := nativeTestClaim("github", "repo-metadata").Unit
	leases := newMemoryLeaseRepository(unit, "dispatching")
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
	sink := &memoryGenerationSink{}
	executor := Executor{
		Credentials: providerfoundation.CredentialResolver{
			Repository: executorCredentialRepository{},
			Decryptor:  executorCredentialDecryptor{},
		},
		Doer: &fixtureDoer{t: t, provider: "github"},
		Retry: providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		Budget:       executorBudgetStore{},
		BudgetLimits: map[CostClass]int{CostLight: 2},
		BudgetTTL:    time.Minute,
		Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
			return executorBackoffGate{}
		},
		Handler:    NativeRESTHandler{Now: func() time.Time { return now }},
		Comparator: matchingComparator{},
		Journal:    ambiguousGenerationJournal{},
		Sink: func(providerfoundation.LeaseGuard) providerfoundation.GenerationSink {
			return sink
		},
		Destination: "provider_records", HeartbeatInterval: 30 * time.Second,
		Now: func() time.Time { return now },
	}
	descriptor, _ := (RouteSwitches{GitHub: true}).Descriptor("github", "repo-metadata")
	if _, err := executor.Execute(context.Background(), session, descriptor); !errors.Is(err, ErrGenerationBlockAmbiguous) {
		t.Fatalf("error=%v", err)
	}
	if len(sink.blocks) != 0 {
		t.Fatalf("ambiguous block was replayed: %d writes", len(sink.blocks))
	}
}

func TestNormalizedShadowComparatorReportsMissingAndChangedSinkRecords(t *testing.T) {
	t.Parallel()
	first := providerfoundation.NormalizedEnvelope{
		SchemaVersion: "v1", Provider: "github", OrgID: "org",
		IntegrationID: "integration", EntityType: "repository",
		SourceID: "one", DedupeKey: "github:repository:one",
		ObservedAt: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		Provenance: providerfoundation.Provenance{Source: "github_rest", Confidence: "1.0"},
		Attributes: map[string]string{"name": "one"},
	}
	changed := first
	changed.Attributes = map[string]string{"name": "changed"}
	missing := first
	missing.SourceID = "two"
	missing.DedupeKey = "github:repository:two"
	comparator := NormalizedShadowComparator{
		Python: staticShadowSource{records: []providerfoundation.NormalizedEnvelope{changed, missing}},
	}
	comparison, err := comparator.Compare(context.Background(), nativeTestClaim("github", "repo-metadata"), []providerfoundation.NormalizedEnvelope{first})
	if err != nil {
		t.Fatal(err)
	}
	if comparison.Match || comparison.NativeRecords != 1 || comparison.PythonRecords != 2 ||
		comparison.MissingNative != 1 || comparison.MissingPython != 0 ||
		comparison.ContentMismatch != 1 {
		t.Fatalf("comparison=%+v", comparison)
	}
}

func TestNormalizedShadowComparatorIgnoresVolatileObservationTime(t *testing.T) {
	t.Parallel()
	native := providerfoundation.NormalizedEnvelope{
		SchemaVersion: "v1", Provider: "github", OrgID: "org",
		IntegrationID: "integration", EntityType: "repository",
		SourceID: "one", DedupeKey: "github:repository:one",
		ObservedAt: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		Provenance: providerfoundation.Provenance{Source: "github_rest", Confidence: "1.0"},
		Attributes: map[string]string{"name": "one"},
	}
	python := native
	python.ObservedAt = native.ObservedAt.Add(37 * time.Second)
	comparator := NormalizedShadowComparator{
		Python: staticShadowSource{records: []providerfoundation.NormalizedEnvelope{python}},
	}
	comparison, err := comparator.Compare(
		context.Background(),
		nativeTestClaim("github", "repo-metadata"),
		[]providerfoundation.NormalizedEnvelope{native},
	)
	if err != nil || !comparison.Match || comparison.ContentMismatch != 0 {
		t.Fatalf("comparison=%+v error=%v", comparison, err)
	}
}

type executorCredentialRepository struct{}

func (executorCredentialRepository) ResolveEncrypted(
	context.Context,
	providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{
		ID: firstCredentialID, Provider: "github", Name: "fixture", Active: true,
		Ciphertext: secrets.NewValue("opaque"),
		Config:     map[string]string{"base_url": "https://fixture.test"},
	}, nil
}

type executorCredentialDecryptor struct{}

func (executorCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"token":"fixture-token"}`), nil
}

type executorReservation struct{}

func (executorReservation) Release(context.Context) error { return nil }

type executorBudgetStore struct{}

func (executorBudgetStore) Acquire(
	context.Context,
	providerfoundation.BudgetKey,
) (providerfoundation.Reservation, error) {
	return executorReservation{}, nil
}

type executorBackoffGate struct{}

func (executorBackoffGate) Wait(context.Context) (time.Duration, error) { return 0, nil }
func (executorBackoffGate) Penalize(context.Context, time.Duration) error {
	return nil
}

type matchingComparator struct{}

func (matchingComparator) Compare(
	context.Context,
	Claim,
	[]providerfoundation.NormalizedEnvelope,
) (ShadowComparison, error) {
	return ShadowComparison{Match: true}, nil
}

type staticShadowSource struct {
	records []providerfoundation.NormalizedEnvelope
	err     error
}

func (source staticShadowSource) Load(
	context.Context,
	Claim,
) ([]providerfoundation.NormalizedEnvelope, error) {
	return source.records, source.err
}

type memoryGenerationSink struct {
	mu     sync.Mutex
	blocks []providerfoundation.GenerationBlock
}

func (sink *memoryGenerationSink) WriteGenerationBlock(
	_ context.Context,
	block providerfoundation.GenerationBlock,
) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.blocks = append(sink.blocks, block)
	return nil
}

type memoryGenerationJournal struct {
	mu    sync.Mutex
	state GenerationJournalState
}

type ambiguousGenerationJournal struct{}

func (ambiguousGenerationJournal) Load(
	context.Context,
	Claim,
	time.Time,
) (GenerationJournalState, error) {
	panic("executor must not load recovery state")
}

func (ambiguousGenerationJournal) Prepare(
	_ context.Context,
	_ Claim,
	desired GenerationJournalState,
	now time.Time,
) (GenerationJournalState, error) {
	now = now.UTC()
	desired.Blocks[0].Status = GenerationBlockWriting
	desired.Blocks[0].StartedAt = &now
	return desired, nil
}

func (ambiguousGenerationJournal) BeginBlock(
	context.Context,
	Claim,
	int,
	string,
	time.Time,
) error {
	panic("ambiguous block must not begin")
}

func (ambiguousGenerationJournal) CommitBlock(
	context.Context,
	Claim,
	int,
	string,
	time.Time,
) error {
	panic("ambiguous block must not commit")
}

func (ambiguousGenerationJournal) ResolveBlock(
	context.Context,
	Claim,
	int,
	string,
	GenerationBlockResolution,
	time.Time,
) error {
	panic("executor must not reconcile implicitly")
}

func (journal *memoryGenerationJournal) Load(
	_ context.Context,
	_ Claim,
	_ time.Time,
) (GenerationJournalState, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.state.validate() != nil {
		return GenerationJournalState{}, ErrGenerationJournalConflict
	}
	return journal.state, nil
}

func (journal *memoryGenerationJournal) Prepare(
	_ context.Context,
	_ Claim,
	desired GenerationJournalState,
	_ time.Time,
) (GenerationJournalState, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.state.Generation == "" {
		journal.state = desired
	} else if !sameGenerationManifest(journal.state, desired) {
		return GenerationJournalState{}, ErrGenerationJournalConflict
	}
	return journal.state, nil
}

func (journal *memoryGenerationJournal) BeginBlock(
	_ context.Context,
	_ Claim,
	index int,
	digest string,
	now time.Time,
) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	block := &journal.state.Blocks[index]
	if block.ContentDigest != digest || block.Status != GenerationBlockPending {
		return ErrGenerationBlockAmbiguous
	}
	now = now.UTC()
	block.StartedAt = &now
	block.Status = GenerationBlockWriting
	return nil
}

func (journal *memoryGenerationJournal) CommitBlock(
	_ context.Context,
	_ Claim,
	index int,
	digest string,
	now time.Time,
) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	block := &journal.state.Blocks[index]
	if block.ContentDigest != digest || block.Status != GenerationBlockWriting {
		return ErrGenerationJournalConflict
	}
	now = now.UTC()
	block.CommittedAt = &now
	block.Status = GenerationBlockCommitted
	return nil
}

func (journal *memoryGenerationJournal) ResolveBlock(
	_ context.Context,
	_ Claim,
	index int,
	digest string,
	resolution GenerationBlockResolution,
	now time.Time,
) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	block := &journal.state.Blocks[index]
	if block.ContentDigest != digest {
		return ErrGenerationJournalConflict
	}
	switch resolution {
	case GenerationBlockMarkCommitted:
		if block.Status != GenerationBlockWriting {
			return ErrGenerationJournalConflict
		}
		now = now.UTC()
		block.CommittedAt = &now
		block.Status = GenerationBlockCommitted
	case GenerationBlockRetryPending:
		if block.Status != GenerationBlockWriting {
			return ErrGenerationJournalConflict
		}
		block.StartedAt = nil
		block.CommittedAt = nil
		block.Status = GenerationBlockPending
	default:
		return ErrInvalidConfiguration
	}
	return nil
}

var _ providerfoundation.CredentialRepository = executorCredentialRepository{}
var _ providerfoundation.CredentialDecryptor = executorCredentialDecryptor{}
var _ providerfoundation.BudgetStore = executorBudgetStore{}
var _ providerfoundation.BackoffGate = executorBackoffGate{}
var _ ShadowComparator = matchingComparator{}
var _ ShadowSource = staticShadowSource{}
var _ providerfoundation.GenerationSink = (*memoryGenerationSink)(nil)
var _ GenerationJournal = (*memoryGenerationJournal)(nil)
var _ GenerationJournal = ambiguousGenerationJournal{}
