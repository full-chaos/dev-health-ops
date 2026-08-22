package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

func TestPreparedRouteSnapshotRoundTripBindsExactGitHubWorkItemsManifest(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	payload, reference, err := encodePreparedRouteManifest(
		claim, batch, ShadowComparison{Match: true, NativeRecords: 16, PythonRecords: 16}, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewEffectLedgerState(claim, batch.Effects, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	state.SchemaVersion = "v2"
	state.PreparedSnapshot = &reference
	manifest, err := decodePreparedRouteManifest(payload, claim, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Batch.Effects) != len(workItemRouteDestinations()) ||
		!manifest.NormalizedAt.Equal(normalizedAt) || !manifest.Comparison.Match ||
		manifest.Batch.Evidence.Records != 16 {
		t.Fatalf("manifest=%+v", manifest)
	}
	for index := range manifest.Batch.Effects {
		if manifest.Batch.Effects[index].ContentDigest != state.Effects[index].ContentDigest {
			t.Fatalf("effect[%d] digest changed", index)
		}
	}
}

func TestPreparedRouteSnapshotRejectsTamperTenantGenerationAndSensitiveResult(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	payload, reference, err := encodePreparedRouteManifest(
		claim, batch, ShadowComparison{Match: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewEffectLedgerState(claim, batch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	state.SchemaVersion, state.PreparedSnapshot = "v2", &reference

	var tamperedSnapshot storedPreparedRouteSnapshot
	if err := json.Unmarshal(payload, &tamperedSnapshot); err != nil {
		t.Fatal(err)
	}
	var tamperedResult map[string]any
	if err := json.Unmarshal(tamperedSnapshot.Result, &tamperedResult); err != nil {
		t.Fatal(err)
	}
	tamperedResult["records"] = 17
	tamperedSnapshot.Result, err = json.Marshal(tamperedResult)
	if err != nil {
		t.Fatal(err)
	}
	tampered, err := json.Marshal(tamperedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(tampered, payload) {
		t.Fatal("tamper fixture did not change the encoded result")
	}
	if _, err := decodePreparedRouteManifest(tampered, claim, state); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("tamper error=%v", err)
	}
	otherTenant := claim
	otherTenant.OrgID = "org-other"
	if _, err := decodePreparedRouteManifest(payload, otherTenant, state); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("tenant error=%v", err)
	}
	otherGeneration := claim
	otherGeneration.ID = uuid.NewString()
	if _, err := decodePreparedRouteManifest(payload, otherGeneration, state); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("generation error=%v", err)
	}
	batch.Result["credential"] = map[string]any{"token": "must-not-persist"}
	if _, _, err := encodePreparedRouteManifest(
		claim, batch, ShadowComparison{Match: true}, now,
	); !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("sensitive result error=%v", err)
	}
}

func TestPreparedRouteSnapshotEnforcesTotalEncodedCapAndLegacyV1StillDecodes(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	batch.Result["oversized"] = strings.Repeat("x", maxPreparedRouteSnapshotBytes)
	if _, _, err := encodePreparedRouteManifest(
		claim, batch, ShadowComparison{Match: true}, now,
	); !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("cap error=%v", err)
	}

	legacy, err := NewEffectLedgerState(claim, preparedGitHubWorkItemsFixture(t, claim).Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeEffectLedgerState(encodeEffectLedgerState(legacy))
	if err != nil || decoded.SchemaVersion != "v1" || decoded.PreparedSnapshot != nil {
		t.Fatalf("legacy=%+v error=%v", decoded, err)
	}
}

func TestCompleteRouteExecutorRecoversPreparedManifestWithoutRecollection(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	ledger := &memoryEffectLedger{}
	prepared, err := ledger.PrepareRouteSnapshot(
		context.Background(), claim, batch,
		ShadowComparison{Match: true, NativeRecords: 16, PythonRecords: 16}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after the first sink accepted its write but before the
	// corresponding ledger transition committed, plus one already-committed
	// destination. Recovery must reconcile these exact snapshot rows.
	committedAt := now.Add(time.Second)
	startedAt := now.Add(500 * time.Millisecond)
	prepared.Effects[0].Status = GenerationBlockCommitted
	prepared.Effects[0].StartedAt = &startedAt
	prepared.Effects[0].CommittedAt = &committedAt
	prepared.Effects[1].Status = GenerationBlockWriting
	prepared.Effects[1].StartedAt = &startedAt
	ledger.state = prepared

	descriptor, ok := Descriptor("github", "work-items")
	if !ok || !descriptor.PreparedManifestRecovery || !descriptor.RouteReady || !descriptor.Plannable {
		t.Fatalf("github/work-items descriptor=%+v ok=%v", descriptor, ok)
	}
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady = true
	descriptor.Plannable = true
	handler := &staticCompleteRouteHandler{
		batch: CompleteRouteBatch{Result: map[string]any{"live_provider": "mutated"}},
	}
	sink := &memoryEffectSink{}
	executor := completeRouteExecutor(now.Add(10*time.Minute), handler, ledger, sink)
	credentials := &forbiddenCredentialRepository{}
	decryptor := &forbiddenCredentialDecryptor{}
	doer := &trackingCompleteRouteDoer{}
	executor.Credentials.Repository = credentials
	executor.Credentials.Decryptor = decryptor
	executor.Doer = doer
	executor.Committer.Readback = staticEffectReadback{inspections: map[string]EffectInspection{
		prepared.Effects[1].Destination: EffectExact,
	}}
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	// preparedPrepares stays at the single setup call: recovery LOADS the
	// snapshot and must never re-prepare one. The counter existed and was
	// never asserted, so nothing would have noticed a recovery path that
	// quietly rewrote the manifest it was supposed to be replaying.
	if !handler.normalizedAt.IsZero() || ledger.preparedLoads != 1 ||
		ledger.preparedPrepares != 1 ||
		credentials.calls != 0 || decryptor.calls != 0 || doer.requests != 0 ||
		result.Effects.Skipped != 1 || result.Effects.MarkedCommitted != 1 ||
		result.Effects.Written != len(workItemRouteDestinations())-2 ||
		result.Result["records"] != float64(16) {
		t.Fatalf(
			"handler_at=%s loads=%d prepares=%d credential_calls=%d decrypt_calls=%d requests=%d result=%+v stored_result=%v",
			handler.normalizedAt, ledger.preparedLoads, ledger.preparedPrepares,
			credentials.calls, decryptor.calls, doer.requests, result, result.Result,
		)
	}
	secondSink := &memoryEffectSink{}
	second := completeRouteExecutor(now.Add(20*time.Minute), handler, ledger, secondSink)
	secondResult, err := second.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if secondResult.Effects.Skipped != len(workItemRouteDestinations()) ||
		secondResult.Effects.Written != 0 || len(secondSink.destinations) != 0 ||
		ledger.preparedPrepares != 1 || !handler.normalizedAt.IsZero() {
		t.Fatalf("all-committed recovery result=%+v writes=%v", secondResult, secondSink.destinations)
	}
}

func TestCompleteRouteExecutorRecoversDurableIncompleteManifestWithoutRefetch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	batch.Result[githubWorkItemsIncompleteResultKey] = []GitHubWorkItemsIncomplete{{
		Component: "issue_comments", SubjectID: "42", Cause: "transient",
	}}
	// Stage a hostile/legacy candidate watermark in the durable payload. The
	// recovery reader, not trust in the original collector, owns suppression.
	if batch.Watermark == nil {
		t.Fatal("fixture must carry a candidate watermark")
	}
	ledger := &memoryEffectLedger{}
	if _, err := ledger.PrepareRouteSnapshot(
		context.Background(), claim, batch, ShadowComparison{Match: true}, now,
	); err != nil {
		t.Fatal(err)
	}

	descriptor, _ := Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true
	handler := &staticCompleteRouteHandler{batch: CompleteRouteBatch{
		Result: map[string]any{"live_provider": "changed"},
	}}
	sink := &memoryEffectSink{}
	executor := completeRouteExecutor(now.Add(time.Hour), handler, ledger, sink)
	credentials := &forbiddenCredentialRepository{}
	decryptor := &forbiddenCredentialDecryptor{}
	doer := &trackingCompleteRouteDoer{}
	executor.Credentials.Repository = credentials
	executor.Credentials.Decryptor = decryptor
	executor.Doer = doer

	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if result.Watermark != nil || !handler.normalizedAt.IsZero() ||
		credentials.calls != 0 || decryptor.calls != 0 || doer.requests != 0 ||
		result.Effects.Written != len(workItemRouteDestinations()) {
		t.Fatalf(
			"watermark=%v handler_at=%s credential_calls=%d decrypt_calls=%d requests=%d result=%+v",
			result.Watermark, handler.normalizedAt, credentials.calls, decryptor.calls,
			doer.requests, result,
		)
	}
	want := []GitHubWorkItemsIncomplete{{
		Component: "issue_comments", SubjectID: "42", Cause: "transient",
	}}
	if got, ok := result.Result[githubWorkItemsIncompleteResultKey].([]GitHubWorkItemsIncomplete); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered incomplete=%#v want=%+v", result.Result[githubWorkItemsIncompleteResultKey], want)
	}
}

func TestCompleteRouteExecutorSuppressesForgedLiveIncompleteWatermark(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	batch.Result[githubWorkItemsIncompleteResultKey] = []GitHubWorkItemsIncomplete{{
		Component: "milestones", Cause: "transient",
	}}
	if batch.Watermark == nil {
		t.Fatal("fixture must carry a forged candidate watermark")
	}

	descriptor, _ := Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true
	handler := &staticCompleteRouteHandler{batch: batch}
	ledger := &memoryEffectLedger{}
	executor := completeRouteExecutor(now, handler, ledger, &memoryEffectSink{})
	executor.Credentials.Repository = executorCredentialRepository{}
	executor.Credentials.Decryptor = executorCredentialDecryptor{}

	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if result.Watermark != nil || handler.normalizedAt.IsZero() ||
		ledger.preparedPrepares != 1 {
		t.Fatalf(
			"watermark=%v handler_at=%s prepared=%d",
			result.Watermark, handler.normalizedAt, ledger.preparedPrepares,
		)
	}
}

func TestCompleteRouteExecutorRecoversCrashImmediatelyAfterPrepare(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	ledger := &memoryEffectLedger{}
	if _, err := ledger.PrepareRouteSnapshot(
		context.Background(), claim, batch, ShadowComparison{Match: true}, now,
	); err != nil {
		t.Fatal(err)
	}
	descriptor, _ := Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true
	handler := &staticCompleteRouteHandler{batch: CompleteRouteBatch{
		Result: map[string]any{"live": "changed"},
	}}
	sink := &memoryEffectSink{}
	result, err := completeRouteExecutor(
		now.Add(time.Hour), handler, ledger, sink,
	).Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if result.Effects.Written != len(workItemRouteDestinations()) ||
		!handler.normalizedAt.IsZero() || ledger.preparedLoads != 1 ||
		ledger.preparedPrepares != 1 {
		t.Fatalf(
			"result=%+v handler_at=%s loads=%d prepares=%d",
			result, handler.normalizedAt, ledger.preparedLoads, ledger.preparedPrepares,
		)
	}
}

func TestCompleteRouteExecutorFailsClosedWhenPreparedSnapshotIsMissing(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	ledger := &memoryEffectLedger{}
	state, err := NewEffectLedgerState(claim, batch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	state.SchemaVersion = "v2"
	state.PreparedSnapshot = &PreparedRouteSnapshotReference{
		SchemaVersion: "v1", ContentDigest: strings.Repeat("a", 64), PayloadBytes: 1,
	}
	ledger.state = state
	descriptor, _ := Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true
	handler := &staticCompleteRouteHandler{batch: batch}
	_, err = completeRouteExecutor(
		now.Add(time.Hour), handler, ledger, &memoryEffectSink{},
	).Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrPreparedRouteSnapshotNotFound) || !handler.normalizedAt.IsZero() {
		t.Fatalf("error=%v handler_at=%s", err, handler.normalizedAt)
	}
}

func preparedGitHubWorkItemsFixture(t *testing.T, claim Claim) CompleteRouteBatch {
	t.Helper()
	destinations := workItemRouteDestinations()
	effects := make([]EffectBatch, 0, len(destinations))
	for _, destination := range destinations {
		policy := EffectReplaySafe
		if destination == "estimate_coverage_metrics_daily" {
			policy = EffectReadbackRequired
		}
		row, err := json.Marshal(map[string]any{
			"org_id": claim.OrgID, "destination": destination, "record": 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		effect, err := BuildEffectBatch(destination, policy, []json.RawMessage{row})
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	watermark := time.Date(2026, 8, 4, 11, 59, 0, 0, time.UTC)
	return CompleteRouteBatch{
		Effects: effects, Result: map[string]any{
			"records": 16, githubWorkItemsIncompleteResultKey: []GitHubWorkItemsIncomplete{},
		}, Watermark: &watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: 6, Pages: 6, Records: 16,
		},
	}
}

type forbiddenCredentialRepository struct{ calls int }

func (repository *forbiddenCredentialRepository) ResolveEncrypted(
	context.Context,
	providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	repository.calls++
	return providerfoundation.EncryptedCredential{}, errors.New("credential access during recovery")
}

type forbiddenCredentialDecryptor struct{ calls int }

func (decryptor *forbiddenCredentialDecryptor) Decrypt(
	secrets.Value,
) ([]byte, error) {
	decryptor.calls++
	return nil, errors.New("credential decrypt during recovery")
}

func preparedGitHubWorkItemsSession(
	t *testing.T,
	now time.Time,
) (Claim, *LeaseSession) {
	t.Helper()
	return preparedWorkItemsSession(t, now, "github", "work-items")
}

// preparedWorkItemsSession builds a claim for an arbitrary pair. Execute
// requires the descriptor and the claim to agree, so testing a guard that
// discriminates on the pair needs both sides moved together.
func preparedWorkItemsSession(
	t *testing.T,
	now time.Time,
	provider string,
	dataset string,
) (Claim, *LeaseSession) {
	t.Helper()
	unit := githubWorkItemOracleClaim().Unit
	unit.Provider, unit.Dataset = provider, dataset
	leases := newMemoryLeaseRepository(unit, "dispatching")
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, OrgID: unit.OrgID, Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim, &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
}

// The rolling-deployment boundary, stated in one place. An older worker
// validates only schema "v1", so the shared ledger document has to be
// self-describing enough that neither side can silently accept the other's
// shape: a v1 document must never carry a snapshot reference, and a v2 one is
// meaningless without a well-formed reference. The legacy round-trip test
// above only proves v1 still decodes; every direction below is a refusal no
// other test exercises.
func TestEffectLedgerStateBindsSchemaVersionToSnapshotPresence(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	_, reference, err := encodePreparedRouteManifest(
		claim, batch, ShadowComparison{Match: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewEffectLedgerState(claim, batch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	if base.SchemaVersion != "v1" || base.PreparedSnapshot != nil {
		t.Fatalf("baseline state=%+v", base)
	}
	nonHexDigest, emptyPayload := reference, reference
	nonHexDigest.ContentDigest = strings.Repeat("z", 64)
	emptyPayload.PayloadBytes = 0
	overCap, futurePayloadSchema := reference, reference
	overCap.PayloadBytes = maxPreparedRouteSnapshotBytes + 1
	futurePayloadSchema.SchemaVersion = "v2"

	for name, mutate := range map[string]func(*EffectLedgerState){
		"v1 carrying a snapshot reference": func(state *EffectLedgerState) {
			state.PreparedSnapshot = &reference
		},
		"v2 without a snapshot reference": func(state *EffectLedgerState) {
			state.SchemaVersion = "v2"
		},
		"v2 with a non-hex digest": func(state *EffectLedgerState) {
			state.SchemaVersion, state.PreparedSnapshot = "v2", &nonHexDigest
		},
		"v2 with an empty payload": func(state *EffectLedgerState) {
			state.SchemaVersion, state.PreparedSnapshot = "v2", &emptyPayload
		},
		"v2 above the payload cap": func(state *EffectLedgerState) {
			state.SchemaVersion, state.PreparedSnapshot = "v2", &overCap
		},
		"v2 naming a future payload schema": func(state *EffectLedgerState) {
			state.SchemaVersion, state.PreparedSnapshot = "v2", &futurePayloadSchema
		},
		"an unknown future ledger schema": func(state *EffectLedgerState) {
			state.SchemaVersion, state.PreparedSnapshot = "v3", &reference
		},
	} {
		t.Run(name, func(t *testing.T) {
			state := base
			state.Effects = append([]EffectLedgerEntry(nil), base.Effects...)
			mutate(&state)
			if err := state.validate(); !errors.Is(err, ErrEffectLedgerConflict) {
				t.Fatalf("validate error=%v", err)
			}
			if encoded := encodeEffectLedgerState(state); encoded != nil {
				t.Fatalf("encoded a state validate rejected: %s", encoded)
			}
			raw, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeEffectLedgerState(raw); !errors.Is(err, ErrEffectLedgerConflict) {
				t.Fatalf("decode error=%v", err)
			}
		})
	}
}

// Contract point 7: new workers may continue an existing route from a legacy
// v1 ledger, but github/work-items must require v2 plus a valid snapshot. The
// ledger here holds a well-formed v1 state AND a decodable payload, so a
// refusal cannot be attributed to a missing sidecar -- only to the route
// refusing to recover from a document that predates its recovery contract.
func TestPreparedRecoveryRefusesLegacyV1LedgerForGitHubWorkItems(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	payload, _, err := encodePreparedRouteManifest(
		claim, batch, ShadowComparison{Match: true}, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := NewEffectLedgerState(claim, batch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &memoryEffectLedger{state: legacy, preparedSnapshot: payload}
	descriptor, _ := Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true
	handler := &staticCompleteRouteHandler{batch: batch}
	_, err = completeRouteExecutor(
		now.Add(time.Hour), handler, ledger, &memoryEffectSink{},
	).Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrEffectRecoveryUnsafe) || !handler.normalizedAt.IsZero() {
		t.Fatalf("legacy ledger error=%v handler_at=%s", err, handler.normalizedAt)
	}
}

// PreparedManifestRecovery is a github/work-items contract, not a general
// capability. A descriptor carrying it for any other pair is a wiring mistake
// that must fail before the route touches credentials, not a route that
// quietly runs without its snapshot.
//
// The mistake has to be a COHERENT one to be reachable at all: Execute's first
// validation already rejects any descriptor that disagrees with its claim, so
// pointing a github/work-items descriptor at another provider tests that
// earlier check, not this guard. The reachable defect is a descriptor and
// claim that agree with each other on some other pair while still carrying the
// recovery flag. Measured, not assumed: an earlier version of this test used
// the incoherent form, and both R17 and R18 survived against it.
func TestPreparedManifestRecoveryIsRefusedOutsideGitHubWorkItems(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	// Both pairs must be ones a real claim can carry. "github/work-item-labels"
	// looks like the natural second case and is not: the alias family collapses
	// to work-items before a unit exists, so Claim.Validate rejects it and
	// Execute refuses at its first check without ever reaching this guard --
	// which is how R18 survived a version of this test that used it.
	for name, pair := range map[string]struct{ provider, dataset string }{
		"another provider": {provider: "linear", dataset: "work-items"},
		"another dataset":  {provider: "github", dataset: "prs"},
	} {
		t.Run(name, func(t *testing.T) {
			claim, session := preparedWorkItemsSession(t, now, pair.provider, pair.dataset)
			descriptor := CompleteRouteDescriptor{
				Provider: pair.provider, RequestedDataset: pair.dataset,
				RouteDataset: pair.dataset, Destinations: workItemRouteDestinations(),
				PreparedManifestRecovery: true, RouteReady: true, Plannable: true,
			}
			handler := &staticCompleteRouteHandler{
				batch: preparedGitHubWorkItemsFixture(t, claim),
			}
			ledger := &memoryEffectLedger{}
			_, err := completeRouteExecutor(
				now.Add(time.Hour), handler, ledger, &memoryEffectSink{},
			).Execute(context.Background(), session, descriptor)
			// The returned error alone is not enough: several later failures
			// wear the same sentinel once the route is already inside the
			// lease. effectLoads is the discriminator -- this guard runs
			// before session.Run, so a refusal that happened there means the
			// ledger was never consulted at all.
			if !errors.Is(err, ErrInvalidConfiguration) || !handler.normalizedAt.IsZero() ||
				ledger.effectLoads != 0 {
				t.Fatalf(
					"error=%v handler_at=%s effect_loads=%d",
					err, handler.normalizedAt, ledger.effectLoads,
				)
			}
		})
	}
}

// ledgerWithoutPreparedRecovery satisfies EffectLedger and nothing more, which
// is exactly the shape of a binary wired to a store that has no snapshot
// sidecar. Embedding the interface rather than the concrete ledger is what
// withholds the two PreparedEffectLedger methods.
type ledgerWithoutPreparedRecovery struct{ EffectLedger }

// The deploy-time half of fail-closed: a route that requires prepared recovery
// must refuse to run at all against a ledger with nowhere to put the snapshot,
// rather than discovering it after the first ClickHouse write.
func TestRouteRequiringPreparedRecoveryRefusesALedgerWithoutASnapshotStore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	_, session := preparedGitHubWorkItemsSession(t, now)
	descriptor, _ := Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true
	handler := &staticCompleteRouteHandler{}
	sink := &memoryEffectSink{}
	_, err := completeRouteExecutor(
		now.Add(time.Hour), handler,
		ledgerWithoutPreparedRecovery{EffectLedger: &memoryEffectLedger{}}, sink,
	).Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrInvalidConfiguration) || !handler.normalizedAt.IsZero() ||
		len(sink.destinations) != 0 {
		t.Fatalf("error=%v handler_at=%s writes=%v", err, handler.normalizedAt, sink.destinations)
	}
}

// A snapshot proves what the batch WAS; the descriptor says what the route may
// write now. Recovery must re-check the manifest against the live descriptor,
// because a destination set that changed between prepare and recovery means the
// stored effects no longer describe this route.
func TestPreparedRecoveryRevalidatesTheManifestAgainstTheLiveDescriptor(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	ledger := &memoryEffectLedger{}
	if _, err := ledger.PrepareRouteSnapshot(
		context.Background(), claim, batch, ShadowComparison{Match: true}, now,
	); err != nil {
		t.Fatal(err)
	}
	descriptor, _ := Descriptor("github", "work-items")
	full := workItemRouteDestinations()
	descriptor.Destinations = full[:len(full)-1]
	descriptor.RouteReady, descriptor.Plannable = true, true
	handler := &staticCompleteRouteHandler{batch: batch}
	sink := &memoryEffectSink{}
	_, err := completeRouteExecutor(
		now.Add(time.Hour), handler, ledger, sink,
	).Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrEffectLedgerConflict) || len(sink.destinations) != 0 {
		t.Fatalf("error=%v writes=%v", err, sink.destinations)
	}
}

// recordingEffectReadback remembers which destinations it was asked about, so
// a test can assert WHICH batch a recovery decision consulted rather than only
// what it decided.
type recordingEffectReadback struct {
	inspections map[string]EffectInspection
	asked       []string
}

func (readback *recordingEffectReadback) InspectEffect(
	_ context.Context,
	_ Claim,
	batch EffectBatch,
) (EffectInspection, error) {
	readback.asked = append(readback.asked, batch.Destination)
	inspection, known := readback.inspections[batch.Destination]
	if !known {
		// Loud, not zero-valued. Returning the zero EffectInspection for an
		// unexpected destination turns a mis-pairing into some generic
		// downstream error whose message never names the destination that was
		// actually inspected -- which is the one fact a reader needs.
		return inspection, fmt.Errorf(
			"readback asked about %q, which this test never staged", batch.Destination,
		)
	}
	return inspection, nil
}

// Recovery pairs prepared batches to ledger entries BY POSITION, so every
// producer of that order must use the same comparator. This pins it with a
// destination whose priority order and alphabetical order DISAGREE:
// github_blame_path_progress sorts last alphabetically and first by priority.
// Under the old plain-destination sort, index 0 of the batch slice was
// ai_attribution while index 0 of the ledger was github_blame_path_progress --
// so the readback would have inspected one destination's rows to resolve
// another's ledger entry, and the mis-pairing would look like an ordinary
// recovery decision.
func TestCommitPreparedPairsEachEffectWithItsOwnLedgerEntry(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	effects := make([]EffectBatch, 0, 2)
	for _, spec := range []struct {
		destination string
		policy      EffectRecoveryPolicy
	}{
		{"ai_attribution", EffectReplaySafe},
		{"github_blame_path_progress", EffectReadbackRequired},
	} {
		row, err := json.Marshal(map[string]any{
			"org_id": claim.OrgID, "destination": spec.destination,
		})
		if err != nil {
			t.Fatal(err)
		}
		effect, err := BuildEffectBatch(spec.destination, spec.policy, []json.RawMessage{row})
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	persisted, err := NewEffectLedgerState(claim, effects, now)
	if err != nil {
		t.Fatal(err)
	}
	// Find the priority entry rather than assuming its index. Asserting
	// position here would make a divergent comparator fail at the setup line,
	// which proves the ledger order changed but says nothing about pairing --
	// and pairing is the invariant under test.
	priority := -1
	for index, entry := range persisted.Effects {
		if entry.Destination == "github_blame_path_progress" {
			priority = index
		}
	}
	if priority < 0 {
		t.Fatalf("priority destination missing from the ledger: %+v", persisted.Effects)
	}
	startedAt := now.Add(time.Second)
	persisted.Effects[priority].Status = GenerationBlockWriting
	persisted.Effects[priority].StartedAt = &startedAt

	ledger := &memoryEffectLedger{state: persisted}
	sink := &memoryEffectSink{}
	readback := &recordingEffectReadback{
		inspections: map[string]EffectInspection{
			"github_blame_path_progress": EffectExact,
		},
	}
	committer := EffectCommitter{
		Ledger: ledger, Sink: sink, Readback: readback,
		Now: func() time.Time { return now.Add(time.Minute) },
	}
	result, err := committer.CommitPrepared(context.Background(), claim, effects, persisted)
	if err != nil {
		t.Fatal(err)
	}
	// The readback must have been asked about the destination whose ledger
	// entry was mid-write -- not whichever batch happened to share its index.
	if len(readback.asked) != 1 || readback.asked[0] != "github_blame_path_progress" {
		t.Fatalf("readback consulted %v, want [github_blame_path_progress]", readback.asked)
	}
	if result.MarkedCommitted != 1 || result.Written != 1 ||
		len(sink.destinations) != 1 || sink.destinations[0] != "ai_attribution" {
		t.Fatalf("result=%+v writes=%v", result, sink.destinations)
	}
}

// An oversized payload must never reach the JSON decoder. Note WHICH guard
// does that work: the reference's own PayloadBytes bound, reached through
// state.validate(), not the len(raw) cap further down decode -- that one is
// unreachable, because len(raw) must equal PayloadBytes and a reference over
// the cap never validates. An earlier version of this comment claimed to cover
// the decode-side cap; the mutation harness disproved it by leaving that
// clause's mutation alive while this test passed.
func TestPreparedRouteSnapshotDecodeRejectsAnOversizedPayload(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	oversized := make([]byte, maxPreparedRouteSnapshotBytes+1)
	digest := sha256.Sum256(oversized)
	state, err := NewEffectLedgerState(claim, batch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	state.SchemaVersion = "v2"
	// The reference agrees with the payload on length and digest, so the only
	// thing standing between this input and a 64 MiB decode is the cap itself.
	state.PreparedSnapshot = &PreparedRouteSnapshotReference{
		SchemaVersion: preparedRouteSnapshotSchemaVersion,
		ContentDigest: hex.EncodeToString(digest[:]),
		PayloadBytes:  len(oversized),
	}
	if _, err := decodePreparedRouteManifest(oversized, claim, state); !errors.Is(
		err, ErrEffectLedgerConflict,
	) {
		t.Fatalf("oversized decode error=%v", err)
	}
}

// Every key containsPreparedRouteSensitiveKey knows about must actually be
// refused. The existing tamper test covers one of them, which is enough for a
// whole-function mutation and useless against the deletion of a single name --
// and a single name is what a careless edit removes.
func TestPreparedRouteSnapshotRefusesEverySensitiveKey(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	for _, key := range []string{
		"authorization", "credential", "credentials", "token", "headers",
		"source_metadata", "integration_config", "ciphertext", "raw_payload",
		"response_body",
		// Spelling normalization: hyphens, case and surrounding space must not
		// smuggle a key past the check.
		"Authorization", "raw-payload", "  token  ",
	} {
		batch := preparedGitHubWorkItemsFixture(t, claim)
		batch.Result[key] = "must-not-persist"
		if _, _, err := encodePreparedRouteManifest(
			claim, batch, ShadowComparison{Match: true}, now,
		); !errors.Is(err, ErrEffectRecoveryUnsafe) {
			t.Fatalf("result key %q was accepted: error=%v", key, err)
		}
		nested := preparedGitHubWorkItemsFixture(t, claim)
		nested.Result["envelope"] = map[string]any{"inner": map[string]any{key: "x"}}
		if _, _, err := encodePreparedRouteManifest(
			claim, nested, ShadowComparison{Match: true}, now,
		); !errors.Is(err, ErrEffectRecoveryUnsafe) {
			t.Fatalf("nested result key %q was accepted: error=%v", key, err)
		}
	}
}

// Completion is withheld until every effect commits. Only the converse was
// proven -- that a fully committed batch completes -- and the converse is the
// easy half.
//
// The ordering is enforced by the caller's control flow, not by a guard inside
// Complete: internal/jobs/providerunit calls Repository.Complete ONLY on the
// `err == nil` arm of executor.Execute. So the property that actually protects
// the watermark is that Execute returns an error when a sink refuses
// mid-batch. This pins that, and pins that the snapshot survives for the next
// attempt -- a failure that both errored AND discarded the manifest would
// satisfy a naive "did it error" assertion while leaving the unit unable to
// recover.
func TestPreparedRecoveryWithholdsSuccessWhenASinkRefusesMidBatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	ledger := &memoryEffectLedger{}
	if _, err := ledger.PrepareRouteSnapshot(
		context.Background(), claim, batch, ShadowComparison{Match: true}, now,
	); err != nil {
		t.Fatal(err)
	}
	destinations := workItemRouteDestinations()
	sortStrings(destinations)
	refused := destinations[len(destinations)/2]
	sinkFailure := errors.New("sink refused the write")
	sink := &memoryEffectSink{failAfterWrite: refused, failure: sinkFailure}

	descriptor, _ := Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true
	handler := &staticCompleteRouteHandler{batch: batch}
	_, err := completeRouteExecutor(
		now.Add(time.Hour), handler, ledger, sink,
	).Execute(context.Background(), session, descriptor)
	if err == nil {
		t.Fatal("Execute reported success while a sink refused a write; the caller would then complete the unit and advance the watermark")
	}
	if !errors.Is(err, sinkFailure) {
		t.Fatalf("error=%v, want the sink's own failure", err)
	}
	// The manifest must still be there for the next attempt.
	recovered, loadErr := ledger.LoadRouteSnapshot(
		context.Background(), claim, ledger.state, now.Add(time.Hour),
	)
	if loadErr != nil {
		t.Fatalf("snapshot unusable after a withheld completion: %v", loadErr)
	}
	if len(recovered.Batch.Effects) != len(workItemRouteDestinations()) {
		t.Fatalf("recovered manifest is incomplete: %d effects", len(recovered.Batch.Effects))
	}
}

func sortStrings(values []string) {
	sort.Strings(values)
}

// TestCompleteRouteExecutorRecoversASnapshotItPreparedItself closes the proof
// gap CHAOS-3940 exposed.
//
// Every other recovery test here hands a batch straight to
// ledger.PrepareRouteSnapshot. That skips the executor's OWN application of
// applyGitHubWorkItemsIncompletePolicy, which is the step that rewrites
// batch.Result before the snapshot is persisted -- so those tests persisted a
// snapshot production never writes. Pre-fix the executor normalized an empty
// evidence set to a nil slice, the snapshot stored `"incomplete": null`, and
// recovery refused it as ErrEffectLedgerConflict (complete_route.go:253).
// Production wedged; the suite stayed green.
//
// This drives Execute twice against one ledger: the first call collects,
// normalizes, prepares and commits exactly as production does, and the second
// recovers from the artifact the first one left behind. A route that cannot
// resume its own durable snapshot is the failure this asserts against.
func TestCompleteRouteExecutorRecoversASnapshotItPreparedItself(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 20, 1, 1, 40, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "work-items")
	// The healthy shape: every optional enrichment phase succeeded, so the
	// route reports an empty -- not absent -- evidence set.
	batch := preparedGitHubWorkItemsFixture(t, claim)
	if entries, ok := batch.Result[githubWorkItemsIncompleteResultKey].([]GitHubWorkItemsIncomplete); !ok ||
		len(entries) != 0 {
		t.Fatalf("fixture must carry an empty evidence set: %#v",
			batch.Result[githubWorkItemsIncompleteResultKey])
	}

	descriptor, ok := Descriptor("github", "work-items")
	if !ok || !descriptor.PreparedManifestRecovery {
		t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
	}
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.Plannable = true, true

	ledger := &memoryEffectLedger{}
	collecting := &staticCompleteRouteHandler{batch: batch}
	collectingExecutor := completeRouteExecutor(now, collecting, ledger, &memoryEffectSink{})
	// The shared fixture pair issues a launchdarkly credential; this route's
	// claim is github, and Execute requires the two to agree.
	collectingExecutor.Credentials.Repository = executorCredentialRepository{}
	collectingExecutor.Credentials.Decryptor = executorCredentialDecryptor{}
	first, err := collectingExecutor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatalf("live collect and prepare failed: %v", err)
	}
	if first.Effects.Written != len(workItemRouteDestinations()) ||
		ledger.preparedPrepares != 1 {
		t.Fatalf("first pass result=%+v prepares=%d", first, ledger.preparedPrepares)
	}

	// Production's second attempt does NOT arrive on the first attempt's
	// session. It arrives on a fresh claim with a new owner and an advanced
	// attempt count -- either released back to `dispatching` after a failed
	// Complete, or recovered from an expired lease after a crash. Reusing the
	// original session would test a retry shape production never takes, so
	// re-claim here. The expired-lease path is the stricter of the two: it is
	// the one that also sets Recovered.
	retryAt := now.Add(10 * time.Minute)
	retryClaim, err := session.Repository.Claim(context.Background(), ClaimRequest{
		UnitID: claim.ID, OrgID: claim.OrgID, Owner: uuid.NewString(), Now: retryAt,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatalf("second attempt could not claim the unit: %v", err)
	}
	if retryClaim.Attempt <= claim.Attempt || retryClaim.Owner == claim.Owner ||
		!retryClaim.Recovered {
		t.Fatalf(
			"retry claim must be a distinct attempt: attempt %d->%d owner_changed=%v recovered=%v",
			claim.Attempt, retryClaim.Attempt, retryClaim.Owner != claim.Owner,
			retryClaim.Recovered,
		)
	}
	// The generation key is derived from the unit id alone (lease.go:146), so
	// the advanced attempt must still resolve to the same durable artifact.
	if retryClaim.GenerationKey() != claim.GenerationKey() {
		t.Fatalf("generation drifted across attempts: %q -> %q",
			claim.GenerationKey(), retryClaim.GenerationKey())
	}
	retrySession := &LeaseSession{
		Repository: session.Repository, Claim: retryClaim, LeaseDuration: time.Minute,
		Deadline: retryAt.Add(time.Hour), Now: func() time.Time { return retryAt },
	}

	// The durable artifact must be resumable. Recovery re-reads it through
	// JSON, so a nil slice persisted as `null` fails here and nowhere earlier.
	recovering := &staticCompleteRouteHandler{
		batch: CompleteRouteBatch{Result: map[string]any{"live_provider": "must not be used"}},
	}
	recoverySink := &memoryEffectSink{}
	second, err := completeRouteExecutor(
		retryAt, recovering, ledger, recoverySink,
	).Execute(context.Background(), retrySession, descriptor)
	if err != nil {
		t.Fatalf("recovery refused the snapshot this route prepared: %v", err)
	}
	if second.Effects.Skipped != len(workItemRouteDestinations()) ||
		second.Effects.Written != 0 || len(recoverySink.destinations) != 0 {
		t.Fatalf("recovery result=%+v writes=%v", second, recoverySink.destinations)
	}
	// Recovery replays the snapshot; it must not re-collect or re-prepare.
	if !recovering.normalizedAt.IsZero() || ledger.preparedPrepares != 1 {
		t.Fatalf("recovery recollected at=%s prepares=%d",
			recovering.normalizedAt, ledger.preparedPrepares)
	}
}
