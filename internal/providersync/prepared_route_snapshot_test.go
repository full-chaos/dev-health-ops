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
