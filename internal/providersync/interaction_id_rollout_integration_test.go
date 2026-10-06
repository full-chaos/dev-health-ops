//go:build integration

package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
	"github.com/google/uuid"
)

// CHAOS-8790 ROLLOUT PROOF. In the roll an OLD pod can crash after it stored a
// prepared snapshot whose work_item_interactions rows have no interaction_id;
// a NEW pod then recovers that unit by replaying the snapshot.

// loggingSink names the effect a sink refused.
type loggingSink struct {
	t     *testing.T
	inner interface {
		EffectSink
		EffectReadback
	}
}

// The ledger harness's tenant ("org-acme") is not a UUID, and the ai_attribution
// adapter refuses any non-UUID tenant even for zero rows (its column is a
// UUID). This route's ai_attribution batch is empty, so the harness stands in
// for that one destination; it has nothing to do with work_item_interactions.
func (sink loggingSink) emptyAIAttribution(effect EffectBatch) bool {
	return effect.Destination == "ai_attribution" && len(effect.Rows) == 0
}

func (sink loggingSink) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if sink.emptyAIAttribution(effect) {
		return nil
	}
	err := sink.inner.WriteEffect(ctx, claim, effect)
	if err != nil {
		sink.t.Logf("    sink refused %s (%d rows): %v", effect.Destination, len(effect.Rows), err)
	}
	return err
}

func (sink loggingSink) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if sink.emptyAIAttribution(effect) {
		return EffectExact, nil
	}
	inspection, err := sink.inner.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectExact {
		sink.t.Logf("    readback of %s: %s err=%v", effect.Destination, inspection, err)
	}
	return inspection, err
}

func TestRolloutProofAnOldPodsSnapshotReplayedByANewPod(t *testing.T) {
	t.Run("control: the same snapshot in the NEW row shape recovers", func(t *testing.T) { rolloutProof(t, false, false) })
	t.Run("old row shape, interactions NOT landed", func(t *testing.T) { rolloutProof(t, true, false) })
	t.Run("old row shape, interactions landed as legacy rows", func(t *testing.T) { rolloutProof(t, true, true) })
}

func rolloutProof(t *testing.T, oldShape, interactionsLanded bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	familyFlags := map[string]bool{}
	for _, dataset := range workitemcontract.FamilyDatasets() {
		familyFlags[workItemFamilyFlagForDataset(dataset)] = true
	}
	flagsJSON, err := json.Marshal(familyFlags)
	if err != nil {
		t.Fatal(err)
	}
	harness := startProviderLedgerHarness(t, ctx, "gitlab", "work-items", string(flagsJSON))
	pool := harness.repository.Pool
	descriptor, ok := Descriptor("gitlab", "work-items")
	if !ok || !descriptor.PreparedManifestRecovery {
		t.Fatal("gitlab/work-items is not registered for prepared recovery")
	}
	realFor := func(lease providerfoundation.LeaseGuard) CompleteRouteHandler {
		deriver, err := NewGitLabWorkItemDeriver(harness.conn, lease,
			resolveStatusMappingConfig(t, "real"), investmentConfigPath(t, "real"))
		if err != nil {
			t.Fatal(err)
		}
		return GitLabWorkItemsRouteHandler{
			StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10, Derived: deriver,
		}
	}
	routeFor := func(handler CompleteRouteHandler) driftRoute {
		return driftRoute{
			provider: "gitlab", dataset: "work-items", flags: string(flagsJSON),
			baseURL: "https://gitlab.com", handler: handler,
			sink: func(conn driver.Conn, lease providerfoundation.LeaseGuard) interface {
				EffectSink
				EffectReadback
			} {
				family, err := NewGitLabWorkItemFamilyClickHouseEffects(conn, lease, nil)
				if err != nil {
					t.Fatal(err)
				}
				return loggingSink{t: t, inner: family}
			},
		}
	}
	newDoer := func() providerfoundation.HTTPDoer {
		return fakehttp.Client(&gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()})
	}
	count := func(query string, args ...any) uint64 {
		t.Helper()
		var n uint64
		if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	chCount := func(query string) uint64 {
		t.Helper()
		var n uint64
		if err := harness.conn.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	watermarksBefore := count(`SELECT count(*) FROM public.sync_watermarks`)

	// ---- the OLD pod: the snapshot it stored has work_item_interactions rows
	// WITHOUT interaction_id (the previous release's row shape), and its first
	// effect (work_items) landed before it died. Built from the real route's
	// own batch, rewritten only where the previous release differed, and
	// stored through the repository's own snapshot writer.
	oldLease := leaseGuardAt(harness.repository, harness.claim, harness.now)
	batch, err := realFor(oldLease).Collect(ctx, harness.claim,
		providerfoundation.Credential{Provider: "gitlab", ID: harness.claim.CredentialID},
		gitLabWorkItemsClient(t, newDoer()), harness.now)
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := ProductionContractComparator{}.CompareCompleteRoute(ctx, harness.claim, batch)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := preparedRouteEffectsForCommit(harness.claim, batch.Effects, nil)
	if err != nil {
		t.Fatal(err)
	}
	strippedRows := 0
	for index, effect := range effects {
		if effect.Destination != "work_item_interactions" || !oldShape {
			continue
		}
		stripped := make([]json.RawMessage, 0, len(effect.Rows))
		for _, raw := range effect.Rows {
			var row map[string]any
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if _, had := row["interaction_id"]; !had {
				t.Fatal("the new route's interaction row has no interaction_id")
			}
			delete(row, "interaction_id")
			encoded, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			stripped = append(stripped, encoded)
			strippedRows++
		}
		rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, stripped)
		if err != nil {
			t.Fatal(err)
		}
		effects[index] = rebuilt
	}
	if oldShape && strippedRows != 2 {
		t.Fatalf("rewrote %d interaction rows, want the 2 gitlab comments", strippedRows)
	}
	desired, err := NewEffectLedgerState(harness.claim, effects, harness.now)
	if err != nil {
		t.Fatal(err)
	}
	byDestination := map[string]EffectBatch{}
	for _, effect := range effects {
		byDestination[effect.Destination] = effect
	}
	// the snapshot lists its effects in the ledger's own order
	storedEffects := make([]storedPreparedEffect, 0, len(effects))
	for _, entry := range desired.Effects {
		effect := byDestination[entry.Destination]
		storedEffects = append(storedEffects, storedPreparedEffect{
			Destination: effect.Destination, ContentDigest: effect.ContentDigest,
			Recovery: effect.Recovery, Rows: effect.Rows, PayloadBytes: effect.PayloadBytes,
		})
	}
	resultJSON, err := json.Marshal(batch.Result)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(storedPreparedRouteSnapshot{
		SchemaVersion: preparedRouteSnapshotSchemaVersion, Generation: harness.claim.GenerationKey(),
		OrgID: harness.claim.OrgID, Provider: "gitlab", Dataset: "work-items", NormalizedAt: harness.now,
		Effects: storedEffects, Result: resultJSON, Watermark: batch.Watermark, Evidence: batch.Evidence,
		Comparison: comparison,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	reference := PreparedRouteSnapshotReference{
		SchemaVersion: preparedRouteSnapshotSchemaVersion,
		ContentDigest: hex.EncodeToString(digest[:]), PayloadBytes: len(payload),
	}
	if _, err := harness.repository.prepareRouteSnapshotPayload(ctx, harness.claim, desired, payload, reference, harness.now); err != nil {
		t.Fatal(err)
	}
	oldSink := routeFor(nil).sink(harness.conn, oldLease)
	landedDestination := "work_items"
	if interactionsLanded {
		landedDestination = "work_item_interactions"
	}
	landed := -1
	for index, entry := range desired.Effects {
		if entry.Destination == landedDestination {
			landed = index
		}
	}
	if landed < 0 {
		t.Fatal("no work_items effect in the ledger")
	}
	var landedEffect EffectBatch
	for _, effect := range effects {
		if effect.Destination == landedDestination {
			landedEffect = effect
		}
	}
	if err := harness.repository.BeginEffect(ctx, harness.claim, landed, landedEffect.ContentDigest, harness.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if interactionsLanded {
		// The previous release's writer: the old column list, no interaction_id.
		for _, raw := range landedEffect.Rows {
			var row struct {
				WorkItemID string    `json:"work_item_id"`
				Provider   string    `json:"provider"`
				Type       string    `json:"interaction_type"`
				OccurredAt time.Time `json:"occurred_at"`
				Actor      *string   `json:"actor"`
				BodyLength uint32    `json:"body_length"`
				LastSynced time.Time `json:"last_synced"`
				OrgID      string    `json:"org_id"`
			}
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if err := harness.conn.Exec(ctx,
				`INSERT INTO work_item_interactions (work_item_id, provider, interaction_type, occurred_at, actor, body_length, last_synced, org_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
				row.WorkItemID, row.Provider, row.Type, row.OccurredAt, row.Actor, row.BodyLength, row.LastSynced, row.OrgID); err != nil {
				t.Fatal(err)
			}
		}
	} else if err := oldSink.WriteEffect(ctx, harness.claim, landedEffect); err != nil {
		t.Fatal(err)
	}
	t.Logf("OLD POD: stored an old-shape snapshot (%d effects, %d interaction rows without interaction_id), landed %s, then died",
		len(effects), strippedRows, landedEffect.Destination)

	// ---- the NEW pod recovers the unit (and the worker retries it)
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		at := harness.now.Add(time.Duration(attempt) * 61 * time.Second)
		fresh, err := NewPostgresRepository(pool)
		if err != nil {
			t.Fatal(err)
		}
		recovered, err := fresh.Claim(ctx, ClaimRequest{
			UnitID: firstUnitID, OrgID: harness.claim.OrgID, Owner: uuid.NewString(),
			Now: at, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
		})
		if err != nil {
			t.Fatalf("attempt %d claim: %v", attempt, err)
		}
		session := &LeaseSession{
			Repository: fresh, Claim: recovered, LeaseDuration: time.Minute,
			Deadline: at.Add(5 * time.Minute), Now: func() time.Time { return at },
		}
		counting := &countingDoer{delegate: newDoer()}
		lease := leaseGuardAt(fresh, recovered, at)
		newRoute := routeFor(realFor(lease))
		sinks := newRoute.sink(harness.conn, lease)
		result, err := driftExecutor(newRoute, harness, fresh, recovered, at,
			fakehttp.Client(counting), sinks, sinks).Execute(ctx, session, descriptor)
		state, stateErr := fresh.LoadEffects(ctx, recovered, at)
		statuses := []string{}
		for _, effect := range state.Effects {
			statuses = append(statuses, effect.Destination+"="+string(effect.Status))
		}
		t.Logf("NEW POD attempt %d: err=%v | invalid_config=%v ledger_conflict=%v recovery_unsafe=%v recovery_ambiguous=%v | provider_requests=%d | effects=%+v | ledger(%v)=%v",
			attempt, err, errors.Is(err, ErrInvalidConfiguration), errors.Is(err, ErrEffectLedgerConflict),
			errors.Is(err, ErrEffectRecoveryUnsafe), errors.Is(err, ErrEffectRecoveryAmbiguous),
			counting.requests, result.Effects, stateErr, statuses)
		lastErr = err
		if counting.requests != 0 {
			t.Fatalf("attempt %d made %d provider requests; a snapshot replay makes none", attempt, counting.requests)
		}
		interactionsStatus := ""
		for _, effect := range state.Effects {
			if effect.Destination == "work_item_interactions" {
				interactionsStatus = string(effect.Status)
			}
		}
		if oldShape && !interactionsLanded {
			// the refusal is the adapter's, it is the same on every attempt, and
			// it leaves the interactions effect un-committed (never written with '')
			if !errors.Is(err, ErrInvalidConfiguration) || interactionsStatus == string(GenerationBlockCommitted) {
				t.Fatalf("attempt %d: err=%v interactions=%q, want the adapter's refusal and an un-committed effect", attempt, err, interactionsStatus)
			}
		} else if err != nil || interactionsStatus != string(GenerationBlockCommitted) {
			t.Fatalf("attempt %d: err=%v interactions=%q, want a clean recovery", attempt, err, interactionsStatus)
		}
	}
	keyedAfterRecovery := chCount(`SELECT count() FROM work_item_interactions FINAL WHERE interaction_id != ''`)
	legacyAfterRecovery := chCount(`SELECT count() FROM work_item_interactions FINAL WHERE interaction_id = ''`)
	t.Logf("after the recovery attempts: last err=%v; keyed interaction rows=%d legacy rows=%d; sync_watermarks rows before=%d after=%d",
		lastErr, keyedAfterRecovery, legacyAfterRecovery, watermarksBefore, count(`SELECT count(*) FROM public.sync_watermarks`))
	wantKeyed, wantLegacy := uint64(0), uint64(0)
	if !oldShape {
		wantKeyed = 2
	}
	if interactionsLanded {
		wantLegacy = 2
	}
	if keyedAfterRecovery != wantKeyed || legacyAfterRecovery != wantLegacy {
		t.Fatalf("keyed=%d legacy=%d after recovery, want keyed=%d legacy=%d", keyedAfterRecovery, legacyAfterRecovery, wantKeyed, wantLegacy)
	}
	if got := count(`SELECT count(*) FROM public.sync_watermarks`); got != watermarksBefore {
		t.Fatalf("recovery moved the watermark: %d rows, was %d", got, watermarksBefore)
	}

	// ---- (c) what the worker does when the attempts are spent: Fail the unit
	if oldShape && !interactionsLanded {
		failedAt := harness.now.Add(2 * time.Hour)
		failer, err := NewPostgresRepository(pool)
		if err != nil {
			t.Fatal(err)
		}
		failClaim, err := failer.Claim(ctx, ClaimRequest{
			UnitID: firstUnitID, OrgID: harness.claim.OrgID, Owner: uuid.NewString(),
			Now: failedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := failer.Fail(ctx, failClaim, "provider_unit_exhausted", failedAt, failedAt.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM public.sync_run_units WHERE id = $1`, firstUnitID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		t.Logf("(c) the old unit after Fail (the worker's step when its attempts are spent, providerunit.go): status=%s", status)
		if status != "failed" {
			t.Fatalf("the unit is %q, want failed", status)
		}
	}

	// ---- (b)(d) the NEXT run of the same scope: a new unit, the same window
	secondUnitID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, 'org-acme', $2, $3, $4, 'gitlab', 'work-items', 'medium',
    'incremental', '2026-07-01T00:00:00Z', '2026-07-31T23:59:59Z',
    'dispatching', $5::jsonb, NOW()
)`, secondUnitID, firstRunID, firstIntegrationID, firstSourceID, string(flagsJSON)); err != nil {
		t.Fatal(err)
	}
	nextAt := harness.now.Add(3 * time.Hour)
	nextRepo, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	nextClaim, err := nextRepo.Claim(ctx, ClaimRequest{
		UnitID: secondUnitID, OrgID: harness.claim.OrgID, Owner: uuid.NewString(),
		Now: nextAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	nextSession := &LeaseSession{
		Repository: nextRepo, Claim: nextClaim, LeaseDuration: time.Minute,
		Deadline: nextAt.Add(5 * time.Minute), Now: func() time.Time { return nextAt },
	}
	nextLease := leaseGuardAt(nextRepo, nextClaim, nextAt)
	nextRoute := routeFor(realFor(nextLease))
	nextSinks := nextRoute.sink(harness.conn, nextLease)
	nextResult, nextErr := driftExecutor(nextRoute, harness, nextRepo, nextClaim, nextAt,
		newDoer(), nextSinks, nextSinks).Execute(ctx, nextSession, descriptor)
	t.Logf("(b) NEXT RUN (new unit, same window, new code): err=%v effects=%+v", nextErr, nextResult.Effects)
	if nextErr != nil {
		t.Fatalf("the next run failed: %v", nextErr)
	}
	{
		if err := nextRepo.Complete(ctx, nextClaim, map[string]any{"go_provider_route": map[string]any{
			"effects_written": nextResult.Effects.Written, "effects_skipped": nextResult.Effects.Skipped, "records": nextResult.Comparison.NativeRecords,
		}}, nextResult.Watermark, nextAt, nextAt.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	keyed := chCount(`SELECT count() FROM work_item_interactions FINAL WHERE interaction_id != ''`)
	view := chCount(`SELECT count() FROM work_item_interactions_current`)
	marks := count(`SELECT count(*) FROM public.sync_watermarks`)
	t.Logf("(b)(d) after the next run: keyed interaction rows=%d view rows=%d (the 2 gitlab comments, ids 501 and 502); sync_watermarks rows=%d", keyed, view, marks)
	if keyed != 2 || view != 2 {
		t.Fatalf("after the next run keyed=%d view=%d, want 2 and 2", keyed, view)
	}
	if marks == watermarksBefore {
		t.Fatal("the next run did not advance the watermark")
	}
}
