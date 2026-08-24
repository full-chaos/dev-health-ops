//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestGitHubWorkItemEffectRecoveryHoldsCompletionAndWatermarks exercises the
// real Postgres effect ledger around both crash windows owned by this layer:
// one destination lands before its CommitEffect acknowledgement, then every
// effect commits before the worker can call PostgresRepository.Complete.
// The counts are DERIVED from the github destination manifest, never spelled:
// CHAOS-4194 moved it from sixteen to eighteen, and a literal here would have
// pinned recovery to a shape github had stopped producing.
// Neither window may advance an alias watermark.
func TestGitHubWorkItemEffectRecoveryHoldsCompletionAndWatermarks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	unitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, unitID, "incremental", `{
		"family_dataset_work_items": true,
		"family_dataset_work_item_labels": true,
		"family_dataset_work_item_history": true,
		"family_dataset_work_item_comments": true
	}`)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	first, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{
		WorkItems: []json.RawMessage{
			json.RawMessage(`{"org_id":"org-acme","work_item_id":"gh:acme/api#1"}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := newSemanticWorkItemEffectBackend()
	backend.failAfterWrite = "work_item_interactions"
	backend.failure = errors.New("process exited after ClickHouse accepted rows")
	firstSink := githubWorkItemEffectsFixture(backend)
	firstSink.Lease = leaseGuardAt(repository, first, now)
	_, err = (EffectCommitter{
		Ledger: repository, Sink: firstSink, Readback: firstSink,
		Now: func() time.Time { return now },
	}).Commit(ctx, first, effects, now)
	if !errors.Is(err, backend.failure) {
		t.Fatalf("first commit error=%v", err)
	}
	assertWorkItemUnitStillRunningWithoutWatermark(t, ctx, pool, unitID)

	recoveryNow := now.Add(61 * time.Second)
	recovered, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: recoveryNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend.failAfterWrite = ""
	recoveredSink := githubWorkItemEffectsFixture(backend)
	recoveredSink.Lease = leaseGuardAt(repository, recovered, recoveryNow)
	result, err := (EffectCommitter{
		Ledger: repository, Sink: recoveredSink, Readback: recoveredSink,
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recovered, effects, now)
	if err != nil || result.MarkedCommitted != 1 ||
		result.MarkedCommitted+result.Skipped+result.Written != len(githubWorkItemRouteDestinations()) {
		t.Fatalf("recovery result=%+v error=%v", result, err)
	}
	assertWorkItemUnitStillRunningWithoutWatermark(t, ctx, pool, unitID)

	// Simulate a second death after every effect was durably committed but
	// before Complete atomically stamps SUCCESS + family watermarks.
	thirdNow := recoveryNow.Add(61 * time.Second)
	third, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: thirdNow,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	thirdSink := githubWorkItemEffectsFixture(backend)
	thirdSink.Lease = leaseGuardAt(repository, third, thirdNow)
	result, err = (EffectCommitter{
		Ledger: repository, Sink: thirdSink, Readback: thirdSink,
		Now: func() time.Time { return thirdNow },
	}).Commit(ctx, third, effects, now)
	if err != nil || result.Skipped != len(githubWorkItemRouteDestinations()) || result.Written != 0 ||
		result.MarkedCommitted != 0 {
		t.Fatalf("all-committed recovery result=%+v error=%v", result, err)
	}
	assertWorkItemUnitStillRunningWithoutWatermark(t, ctx, pool, unitID)
}

// TestGitHubWorkItemPreparedSnapshotRecoversAcrossPostgresAndClickHouse is the
// cross-store crash proof for D17. ClickHouse accepts a real work_items row,
// the process dies while Postgres still says that effect is writing, and a
// fresh claim must read back that exact durable snapshot without credentials,
// HTTP, collection, or mutable derivation inputs. Typed incompleteness still
// terminalizes the effects/result but withholds every family watermark.
func TestGitHubWorkItemPreparedSnapshotRecoversAcrossPostgresAndClickHouse(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := postgres.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	unitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, unitID, "incremental", `{
		"sync_prs": true,
		"family_dataset_work_items": true,
		"family_dataset_work_item_labels": true,
		"family_dataset_work_item_projects": true,
		"family_dataset_work_item_history": true,
		"family_dataset_work_item_comments": true
	}`)
	// ai_attribution uses a UUID tenant column while the other 15 destinations
	// use String. A real composite replay therefore needs the same UUID-shaped
	// tenant production supplies, even though the AI batch is empty here.
	recoveryOrgID := uuid.NewString()
	for _, statement := range []string{
		"UPDATE public.integrations SET org_id = $1",
		"UPDATE public.integration_sources SET org_id = $1",
		"UPDATE public.integration_datasets SET org_id = $1",
		"UPDATE public.sync_runs SET org_id = $1",
		"UPDATE public.sync_run_units SET org_id = $1",
	} {
		if _, err := pool.Exec(ctx, statement, recoveryOrgID); err != nil {
			t.Fatal(err)
		}
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	normalizedAt := time.Date(2026, 8, 8, 12, 0, 0, 123000000, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: recoveryOrgID, Owner: uuid.NewString(), Now: normalizedAt,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	repoID := uuid.New()
	row := githubWorkItemRow{
		WorkItemID: "gh:acme/api#3606", Provider: "github",
		Title: "durable recovery", Type: "issue", Status: "open",
		RepoID: &repoID, Assignees: []string{}, Labels: []string{},
		CreatedAt: normalizedAt.Add(-time.Hour), UpdatedAt: normalizedAt,
		OrgID: claim.OrgID, LastSynced: normalizedAt,
	}
	encodedRow, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{
		WorkItems: []json.RawMessage{encodedRow},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidateWatermark := claim.BeforeAt.UTC()
	batch := CompleteRouteBatch{
		Effects: effects,
		Result: map[string]any{
			"records": 1,
			githubWorkItemsIncompleteResultKey: []GitHubWorkItemsIncomplete{{
				Component: "issue_comments", SubjectID: "3606", Cause: "transient",
			}},
		},
		Watermark: &candidateWatermark,
		Evidence: FetchEvidence{
			Provider: "github", Dataset: "work-items", Requests: 4, Pages: 4, Records: 1,
		},
	}
	prepared, err := repository.PrepareRouteSnapshot(
		ctx, claim, batch, ShadowComparison{Match: true, NativeRecords: 1, PythonRecords: 1},
		normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.SchemaVersion != "v2" || prepared.PreparedSnapshot == nil ||
		len(prepared.Effects) != len(githubWorkItemRouteDestinations()) {
		t.Fatalf("prepared ledger=%+v", prepared)
	}

	workItemsIndex := -1
	var workItemsEffect EffectBatch
	for index, effect := range effects {
		if effect.Destination == "work_items" {
			workItemsIndex = index
			workItemsEffect = effect
			break
		}
	}
	if workItemsIndex < 0 {
		t.Fatal("prepared manifest has no work_items effect")
	}
	firstSink, err := NewGitHubWorkItemClickHouseEffects(
		conn, leaseGuardAt(repository, claim, normalizedAt.Add(time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.BeginEffect(
		ctx, claim, workItemsIndex, workItemsEffect.ContentDigest,
		normalizedAt.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := firstSink.WriteEffect(ctx, claim, workItemsEffect); err != nil {
		t.Fatal(err)
	}
	// Deliberately omit CommitEffect: this is the process-death window.

	recoveryNow := normalizedAt.Add(61 * time.Second)
	recovered, err := repository.Claim(ctx, ClaimRequest{
		UnitID: unitID, OrgID: recoveryOrgID, Owner: uuid.NewString(), Now: recoveryNow,
		LeaseDuration: 5 * time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := repository.LoadEffects(ctx, recovered, recoveryNow)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.SchemaVersion != "v2" || persisted.PreparedSnapshot == nil ||
		persisted.Effects[workItemsIndex].Status != GenerationBlockWriting {
		t.Fatalf("recovered ledger=%+v", persisted)
	}

	recoveredSink, err := NewGitHubWorkItemClickHouseEffects(
		conn, leaseGuardAt(repository, recovered, recoveryNow),
	)
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: repository, Claim: recovered, LeaseDuration: 5 * time.Minute,
		Deadline: recoveryNow.Add(10 * time.Minute), Now: func() time.Time { return recoveryNow },
	}
	descriptor, ok := Descriptor("github", "work-items")
	if !ok || !descriptor.RouteReady || !descriptor.PreparedManifestRecovery {
		t.Fatalf("recovery descriptor=%+v ok=%v", descriptor, ok)
	}
	descriptor.Plannable = true
	descriptor.Destinations = githubWorkItemRouteDestinations()
	handler := &staticCompleteRouteHandler{batch: CompleteRouteBatch{
		Result: map[string]any{"mutable_live_input": true},
	}}
	executor := completeRouteExecutorWithCommitClock(
		recoveryNow, recoveryNow.Add(time.Second), handler, repository, recoveredSink,
	)
	credentials := &forbiddenCredentialRepository{}
	decryptor := &forbiddenCredentialDecryptor{}
	doer := &trackingCompleteRouteDoer{}
	executor.Credentials.Repository = credentials
	executor.Credentials.Decryptor = decryptor
	executor.Doer = doer
	executor.Committer.Readback = recoveredSink
	result, err := executor.Execute(ctx, session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if !handler.normalizedAt.IsZero() || credentials.calls != 0 || decryptor.calls != 0 ||
		doer.requests != 0 || result.Watermark != nil || result.Effects.MarkedCommitted != 1 ||
		result.Effects.Written != len(githubWorkItemRouteDestinations())-1 {
		t.Fatalf(
			"handler_at=%s credential_calls=%d decrypt_calls=%d requests=%d result=%+v",
			handler.normalizedAt, credentials.calls, decryptor.calls, doer.requests, result,
		)
	}
	incomplete, ok := result.Result[githubWorkItemsIncompleteResultKey].([]GitHubWorkItemsIncomplete)
	if !ok || len(incomplete) != 1 || incomplete[0].Component != "issue_comments" ||
		incomplete[0].SubjectID != "3606" || incomplete[0].Cause != "transient" {
		t.Fatalf("recovered incomplete=%#v", result.Result[githubWorkItemsIncompleteResultKey])
	}
	inspection, err := recoveredSink.InspectEffect(ctx, recovered, workItemsEffect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("work_items readback=%s error=%v", inspection, err)
	}
	committed, err := repository.LoadEffects(ctx, recovered, recoveryNow.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for index, effect := range committed.Effects {
		if effect.Status != GenerationBlockCommitted ||
			effect.Destination != prepared.Effects[index].Destination ||
			effect.ContentDigest != prepared.Effects[index].ContentDigest {
			t.Fatalf("committed effect[%d]=%+v prepared=%+v", index, effect, prepared.Effects[index])
		}
	}

	completedAt := recoveryNow.Add(3 * time.Second)
	if err := repository.Complete(
		ctx, recovered, result.Result, result.Watermark, recoveryNow, completedAt,
	); err != nil {
		t.Fatal(err)
	}
	var status string
	var evidenceMatches bool
	var watermarkCount, snapshotCount int
	if err := pool.QueryRow(ctx, `
SELECT status,
       result::jsonb -> 'incomplete' =
         '[{"component":"issue_comments","subject_id":"3606","cause":"transient"}]'::jsonb
FROM public.sync_run_units
WHERE id = $1`, unitID).Scan(&status, &evidenceMatches); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.sync_watermarks
WHERE org_id = $1 AND source_id = 'acme/api'
  AND dataset_key IN (
    'work-items', 'work-item-labels', 'work-item-projects',
    'work-item-history', 'work-item-comments'
	  )`, recoveryOrgID).Scan(&watermarkCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2`, recovered.OrgID, recovered.ID,
	).Scan(&snapshotCount); err != nil {
		t.Fatal(err)
	}
	if status != "success" || !evidenceMatches || watermarkCount != 0 || snapshotCount != 0 {
		t.Fatalf(
			"status=%q evidence=%t watermarks=%d snapshots=%d",
			status, evidenceMatches, watermarkCount, snapshotCount,
		)
	}
}

func assertWorkItemUnitStillRunningWithoutWatermark(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	unitID string,
) {
	t.Helper()
	var status string
	var watermarkCount int
	if err := pool.QueryRow(ctx, `
SELECT status FROM public.sync_run_units WHERE id = $1`, unitID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.sync_watermarks
WHERE org_id = 'org-acme'
  AND source_id = 'acme/api'
  AND dataset_key IN (
    'work-items', 'work-item-labels', 'work-item-projects',
    'work-item-history', 'work-item-comments'
  )`).Scan(&watermarkCount); err != nil {
		t.Fatal(err)
	}
	if status != "running" || watermarkCount != 0 {
		t.Fatalf("status=%q work-item watermarks=%d", status, watermarkCount)
	}
}
