package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

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

	tampered := bytes.Replace(
		payload, []byte(`"result":{"records":16}`),
		[]byte(`"result":{"records":17}`), 1,
	)
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

	descriptor, ok := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
	if !ok || !descriptor.PreparedManifestRecovery || descriptor.RouteReady || descriptor.RouteEnabled {
		t.Fatalf("unregistered descriptor=%+v ok=%v", descriptor, ok)
	}
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady = true
	descriptor.RouteEnabled = true
	handler := &staticCompleteRouteHandler{
		batch: CompleteRouteBatch{Result: map[string]any{"live_provider": "mutated"}},
	}
	sink := &memoryEffectSink{}
	executor := completeRouteExecutor(now.Add(10*time.Minute), handler, ledger, sink)
	executor.Committer.Readback = staticEffectReadback{inspections: map[string]EffectInspection{
		prepared.Effects[1].Destination: EffectExact,
	}}
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if !handler.normalizedAt.IsZero() || ledger.preparedLoads != 1 ||
		result.Effects.Skipped != 1 || result.Effects.MarkedCommitted != 1 ||
		result.Effects.Written != len(workItemRouteDestinations())-2 ||
		result.Result["records"] != float64(16) {
		t.Fatalf(
			"handler_at=%s loads=%d result=%+v stored_result=%v",
			handler.normalizedAt, ledger.preparedLoads, result, result.Result,
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
		!handler.normalizedAt.IsZero() {
		t.Fatalf("all-committed recovery result=%+v writes=%v", secondResult, secondSink.destinations)
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
	descriptor, _ := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.RouteEnabled = true, true
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
		!handler.normalizedAt.IsZero() || ledger.preparedLoads != 1 {
		t.Fatalf("result=%+v handler_at=%s loads=%d", result, handler.normalizedAt, ledger.preparedLoads)
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
	descriptor, _ := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.RouteEnabled = true, true
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
		Effects: effects, Result: map[string]any{"records": 16}, Watermark: &watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: 6, Pages: 6, Records: 16,
		},
	}
}

func preparedGitHubWorkItemsSession(
	t *testing.T,
	now time.Time,
) (Claim, *LeaseSession) {
	t.Helper()
	unit := githubWorkItemOracleClaim().Unit
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
	descriptor, _ := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.RouteEnabled = true, true
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
func TestPreparedManifestRecoveryIsRefusedOutsideGitHubWorkItems(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	batch := preparedGitHubWorkItemsFixture(t, claim)
	for name, retarget := range map[string]func(*CompleteRouteDescriptor){
		"another provider": func(descriptor *CompleteRouteDescriptor) {
			descriptor.Provider = "linear"
		},
		"another dataset": func(descriptor *CompleteRouteDescriptor) {
			descriptor.RouteDataset = "work-item-labels"
		},
	} {
		t.Run(name, func(t *testing.T) {
			descriptor, _ := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
			descriptor.Destinations = workItemRouteDestinations()
			descriptor.RouteReady, descriptor.RouteEnabled = true, true
			retarget(&descriptor)
			handler := &staticCompleteRouteHandler{batch: batch}
			_, err := completeRouteExecutor(
				now.Add(time.Hour), handler, &memoryEffectLedger{}, &memoryEffectSink{},
			).Execute(context.Background(), session, descriptor)
			if !errors.Is(err, ErrInvalidConfiguration) || !handler.normalizedAt.IsZero() {
				t.Fatalf("error=%v handler_at=%s", err, handler.normalizedAt)
			}
			_ = claim
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
	descriptor, _ := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
	descriptor.Destinations = workItemRouteDestinations()
	descriptor.RouteReady, descriptor.RouteEnabled = true, true
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
	descriptor, _ := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
	full := workItemRouteDestinations()
	descriptor.Destinations = full[:len(full)-1]
	descriptor.RouteReady, descriptor.RouteEnabled = true, true
	handler := &staticCompleteRouteHandler{batch: batch}
	sink := &memoryEffectSink{}
	_, err := completeRouteExecutor(
		now.Add(time.Hour), handler, ledger, sink,
	).Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrEffectLedgerConflict) || len(sink.destinations) != 0 {
		t.Fatalf("error=%v writes=%v", err, sink.destinations)
	}
}
