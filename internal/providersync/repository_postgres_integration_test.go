//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresLeaseClaimRenewRecoveryAndTerminalFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	firstOwner := uuid.NewString()
	first, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: firstOwner, Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Attempt != 1 || first.Recovered || first.GenerationKey() != "sync-unit:"+firstUnitID ||
		first.ProcessorFlags["sync_git"] != true || first.DatasetOptions["include_archived"] != false {
		t.Fatalf("first claim=%+v", first)
	}
	if _, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now.Add(30 * time.Second),
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	}); !errors.Is(err, ErrUnitNotClaimable) {
		t.Fatalf("live claim steal error=%v", err)
	}
	if err := repository.Renew(ctx, first, now.Add(30*time.Second), now.Add(90*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.Assert(ctx, first, now.Add(89*time.Second)); err != nil {
		t.Fatal(err)
	}
	// Dataset options are intentionally joined live when the already-planned
	// unit is claimed. Admin PATCH can therefore replace a frozen provider
	// control before first claim or expired-lease recovery without rewriting the
	// unit row itself.
	if _, err := pool.Exec(ctx, `
UPDATE public.integration_datasets
SET options = '{"include_archived":false,"comments_limit":37}'::jsonb
WHERE integration_id = $1 AND dataset_key = 'commits'`, firstIntegrationID); err != nil {
		t.Fatal(err)
	}

	secondOwner := uuid.NewString()
	second, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: secondOwner, Now: now.Add(91 * time.Second),
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Recovered || second.Attempt != 2 || second.GenerationKey() != first.GenerationKey() ||
		second.DatasetOptions["comments_limit"] != float64(37) {
		t.Fatalf("recovered claim=%+v", second)
	}
	if err := repository.Assert(ctx, first, now.Add(92*time.Second)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner assert error=%v", err)
	}
	if err := repository.Assert(ctx, second, now.Add(92*time.Second)); err != nil {
		t.Fatal(err)
	}
	var attempts, recoveryCount int
	var retryReason string
	if err := pool.QueryRow(ctx, `
SELECT attempts, expired_lease_retry_count, last_retry_reason
FROM public.sync_run_units WHERE id = $1`, firstUnitID).Scan(&attempts, &recoveryCount, &retryReason); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || recoveryCount != 1 || retryReason != "expired_lease" {
		t.Fatalf("attempts=%d recovery_count=%d retry_reason=%q", attempts, recoveryCount, retryReason)
	}

	generationBlocks, err := providerfoundation.BuildGenerationBlocks(
		second.GenerationKey(),
		"provider_records",
		[]providerfoundation.NormalizedEnvelope{{
			SchemaVersion: "v1", Provider: "github", OrgID: "org-acme",
			IntegrationID: firstIntegrationID, EntityType: "repository",
			SourceID: "github:repo:acme/api", DedupeKey: "github:repository:github:repo:acme/api",
			ObservedAt: now, Provenance: providerfoundation.Provenance{Source: "github_rest", Confidence: "1.0"},
			Attributes: map[string]string{"name": "acme/api"},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	desired, err := NewGenerationJournalState(generationBlocks, now.Add(92*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repository.Prepare(ctx, second, desired, now.Add(92*time.Second))
	if err != nil || prepared.Blocks[0].Status != GenerationBlockPending {
		t.Fatalf("prepared=%+v error=%v", prepared, err)
	}
	if err := repository.BeginBlock(ctx, second, 0, generationBlocks[0].ContentDigest(), now.Add(93*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.BeginBlock(ctx, second, 0, generationBlocks[0].ContentDigest(), now.Add(94*time.Second)); !errors.Is(err, ErrGenerationBlockAmbiguous) {
		t.Fatalf("ambiguous block replay error=%v", err)
	}
	if err := repository.ResolveBlock(
		ctx, second, 0, generationBlocks[0].ContentDigest(),
		GenerationBlockRetryPending, now.Add(95*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	resumed, err := repository.Prepare(ctx, second, desired, now.Add(96*time.Second))
	if err != nil || resumed.Blocks[0].Status != GenerationBlockPending ||
		resumed.Blocks[0].StartedAt != nil {
		t.Fatalf("reset=%+v error=%v", resumed, err)
	}
	if err := repository.BeginBlock(ctx, second, 0, generationBlocks[0].ContentDigest(), now.Add(97*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repository.ResolveBlock(
		ctx, second, 0, generationBlocks[0].ContentDigest(),
		GenerationBlockMarkCommitted, now.Add(98*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	resumed, err = repository.Prepare(ctx, second, desired, now.Add(99*time.Second))
	if err != nil || resumed.Blocks[0].Status != GenerationBlockCommitted {
		t.Fatalf("resumed=%+v error=%v", resumed, err)
	}
	conflict := desired
	conflict.Generation = "sync-unit:different"
	if _, err := repository.Prepare(ctx, second, conflict, now.Add(100*time.Second)); !errors.Is(err, ErrGenerationJournalConflict) {
		t.Fatalf("manifest conflict error=%v", err)
	}

	effectBatches := []EffectBatch{
		effectBatchFixture(
			t, "work_items", EffectReplaySafe,
			`{"org_id":"org-acme","work_item_id":"linear:ENG-1"}`,
		),
		effectBatchFixture(
			t, "work_item_transitions", EffectReadbackRequired,
			`{"org_id":"org-acme","work_item_id":"linear:ENG-1","occurred_at":"2026-07-23T12:00:00Z"}`,
		),
	}
	effectDesired, err := NewEffectLedgerState(
		second, effectBatches, now.Add(101*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	effectPrepared, err := repository.PrepareEffects(
		ctx, second, effectDesired, now.Add(101*time.Second),
	)
	if err != nil || len(effectPrepared.Effects) != 2 {
		t.Fatalf("effect prepared=%+v error=%v", effectPrepared, err)
	}
	firstEffect := effectPrepared.Effects[0]
	if err := repository.BeginEffect(
		ctx, second, firstEffect.Index, firstEffect.ContentDigest,
		now.Add(102*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := repository.ResolveEffect(
		ctx, second, firstEffect.Index, firstEffect.ContentDigest,
		GenerationBlockRetryPending, now.Add(103*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := repository.BeginEffect(
		ctx, second, firstEffect.Index, firstEffect.ContentDigest,
		now.Add(104*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := repository.CommitEffect(
		ctx, second, firstEffect.Index, firstEffect.ContentDigest,
		now.Add(105*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	effectLoaded, err := repository.LoadEffects(
		ctx, second, now.Add(106*time.Second),
	)
	if err != nil || effectLoaded.Effects[0].Status != GenerationBlockCommitted ||
		effectLoaded.Effects[1].Status != GenerationBlockPending {
		t.Fatalf("effect loaded=%+v error=%v", effectLoaded, err)
	}
	// The dedicated effect ledger must coexist with the singleton repository
	// recovery journal without rewriting or weakening its v2 payload contract.
	resumed, err = repository.Load(ctx, second, now.Add(106*time.Second))
	if err != nil || resumed.Blocks[0].Status != GenerationBlockCommitted {
		t.Fatalf("generation journal after effect ledger=%+v error=%v", resumed, err)
	}

	if _, err := pool.Exec(ctx, "UPDATE public.sync_runs SET status = 'failed' WHERE id = $1", firstRunID); err != nil {
		t.Fatal(err)
	}
	if err := repository.Assert(ctx, second, now.Add(93*time.Second)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("terminal run assert error=%v", err)
	}
	if err := repository.Renew(ctx, second, now.Add(93*time.Second), now.Add(153*time.Second)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("terminal run renew error=%v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE public.sync_runs SET status = 'running' WHERE id = $1", firstRunID); err != nil {
		t.Fatal(err)
	}
	finalizeClaimToken := "live-finalize-claim"
	finalizeClaimExpiresAt := now.Add(5 * time.Minute)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_dispatch_outbox (
    id, org_id, sync_run_id, kind, status, available_at, attempts,
    claim_token, claim_expires_at, claim_transport, claim_route_generation,
    created_at, updated_at
) VALUES (
    $1, 'org-acme', $2, 'finalize_sync_run', 'pending', $3, 1,
    $4, $5, 'celery', 1, $6, $6
)`,
		uuid.NewString(), firstRunID, now.Add(10*time.Minute),
		finalizeClaimToken, finalizeClaimExpiresAt, now,
	); err != nil {
		t.Fatal(err)
	}
	// CHAOS-3427: this watermark deliberately overshoots the unit's window end
	// (the fixture's before_at is 2026-07-23T12:00:00Z, i.e. `now`) by 106
	// seconds. Before the C10(c) write boundary landed it was persisted
	// verbatim; it is now clamped to the coverage bound, because a unit that
	// only fetched up to its window end cannot claim data past it. The
	// assertion below therefore expects the CLAMPED value -- see
	// wantWatermark.
	watermark := now.Add(106 * time.Second)
	completedAt := now.Add(107 * time.Second)
	wantWatermark := *second.BeforeAt
	if err := repository.Complete(
		ctx, second, map[string]any{"records": 1}, &watermark,
		now.Add(91*time.Second), completedAt,
	); err != nil {
		t.Fatal(err)
	}
	var status string
	var persistedWatermark time.Time
	if err := pool.QueryRow(ctx, `
SELECT unit.status, watermark.last_synced_at
FROM public.sync_run_units AS unit
JOIN public.sync_watermarks AS watermark
  ON watermark.org_id = unit.org_id
 AND watermark.source_id = 'acme/api'
 AND watermark.dataset_key = 'commits'
WHERE unit.id = $1`, firstUnitID).Scan(&status, &persistedWatermark); err != nil {
		t.Fatal(err)
	}
	if status != "success" || !persistedWatermark.Equal(wantWatermark) {
		t.Fatalf("status=%s watermark=%s, want %s (the requested %s is past the "+
			"unit's window end and must be clamped to it)",
			status, persistedWatermark, wantWatermark, watermark)
	}
	var finalizeCount int
	var persistedClaimToken string
	var persistedClaimExpiry, availableAt time.Time
	if err := pool.QueryRow(ctx, `
SELECT count(*), min(claim_token), min(claim_expires_at), min(available_at)
FROM public.sync_dispatch_outbox
WHERE sync_run_id = $1 AND kind = 'finalize_sync_run' AND status = 'pending'`,
		firstRunID,
	).Scan(&finalizeCount, &persistedClaimToken, &persistedClaimExpiry, &availableAt); err != nil {
		t.Fatal(err)
	}
	if finalizeCount != 1 || persistedClaimToken != finalizeClaimToken ||
		!persistedClaimExpiry.Equal(finalizeClaimExpiresAt) || !availableAt.Equal(completedAt) {
		t.Fatalf(
			"finalize outbox count=%d token=%q expiry=%s available_at=%s",
			finalizeCount, persistedClaimToken, persistedClaimExpiry, availableAt,
		)
	}

	// Optional GitHub enrichment failures are durable completion evidence, but
	// they are not evidence that the five-alias family is current. Even if a
	// caller forges a candidate watermark, Complete must persist the effects'
	// typed incompleteness and suppress every alias watermark atomically.
	degradedUnitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, degradedUnitID, "incremental", `{
		"sync_prs": true,
		"family_dataset_work_items": true,
		"family_dataset_work_item_labels": true,
		"family_dataset_work_item_projects": true,
		"family_dataset_work_item_history": true,
		"family_dataset_work_item_comments": true
	}`)
	degradedClaimedAt := completedAt.Add(time.Second)
	degradedClaim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: degradedUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: degradedClaimedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var degradedResult map[string]any
	if err := json.Unmarshal([]byte(`{
		"records": 7,
		"incomplete": [{"component":"milestones","cause":"transient"}]
	}`), &degradedResult); err != nil {
		t.Fatal(err)
	}
	forgedWatermark := degradedClaimedAt.Add(time.Hour)
	degradedCompletedAt := degradedClaimedAt.Add(time.Second)
	if err := repository.Complete(
		ctx, degradedClaim, degradedResult, &forgedWatermark,
		degradedClaimedAt, degradedCompletedAt,
	); err != nil {
		t.Fatal(err)
	}
	var degradedStatus string
	var degradedEvidenceMatches, degradedAuditMatches bool
	var degradedWatermarkCount int
	if err := pool.QueryRow(ctx, `
SELECT status,
       result::jsonb -> 'incomplete' =
         '[{"component":"milestones","cause":"transient"}]'::jsonb,
       result::jsonb -> 'family_datasets' =
         '["work-items","work-item-labels","work-item-projects","work-item-history","work-item-comments"]'::jsonb
FROM public.sync_run_units
WHERE id = $1`, degradedUnitID).Scan(
		&degradedStatus, &degradedEvidenceMatches, &degradedAuditMatches,
	); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM public.sync_watermarks
WHERE org_id = 'org-acme'
  AND source_id = 'acme/api'
  AND dataset_key IN (
    'work-items', 'work-item-labels', 'work-item-projects',
    'work-item-history', 'work-item-comments'
  )`,
	).Scan(&degradedWatermarkCount); err != nil {
		t.Fatal(err)
	}
	if degradedStatus != "success" || !degradedEvidenceMatches ||
		!degradedAuditMatches || degradedWatermarkCount != 0 {
		t.Fatalf(
			"degraded status=%q evidence=%t audit=%t alias_watermarks=%d",
			degradedStatus, degradedEvidenceMatches, degradedAuditMatches,
			degradedWatermarkCount,
		)
	}

	// The Python planner collapses the enabled GitHub work-item family onto one
	// canonical work-items unit. Completion keeps that claim identity while
	// atomically fanning its watermark and result audit back out to the enabled
	// aliases.
	familyUnitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, familyUnitID, "incremental", `{
		"sync_prs": true,
		"family_dataset_work_items": true,
		"family_dataset_work_item_labels": true,
		"family_dataset_work_item_projects": true,
		"family_dataset_work_item_history": true,
		"family_dataset_work_item_comments": true
	}`)
	familyClaimedAt := degradedCompletedAt.Add(time.Second)
	familyClaim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: familyUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: familyClaimedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Same CHAOS-3427 clamp as the single-dataset completion above: this
	// proposed watermark sits past the seeded unit's window end, so every
	// alias row lands on the coverage bound instead. Fanning out to three
	// aliases is what is under test here, and the clamp must apply uniformly
	// to all of them -- a per-alias divergence would be a real defect.
	familyWatermark := familyClaimedAt.Add(time.Second)
	wantFamilyWatermark := *familyClaim.BeforeAt
	if err := repository.Complete(
		ctx, familyClaim, map[string]any{
			"records":    7,
			"incomplete": []GitHubWorkItemsIncomplete{},
		}, &familyWatermark,
		familyClaimedAt, familyClaimedAt.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	var familyStatus, familyDataset, watermarkKeys string
	var familyAuditMatches bool
	var watermarkCount int
	if err := pool.QueryRow(ctx, `
SELECT status, dataset_key,
       result::jsonb -> 'family_datasets' =
         '["work-items","work-item-labels","work-item-projects","work-item-history","work-item-comments"]'::jsonb
FROM public.sync_run_units
WHERE id = $1`, familyUnitID).Scan(
		&familyStatus, &familyDataset, &familyAuditMatches,
	); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*), string_agg(dataset_key, ',' ORDER BY dataset_key)
FROM public.sync_watermarks
WHERE org_id = 'org-acme'
  AND source_id = 'acme/api'
  AND dataset_key IN (
    'work-items', 'work-item-labels', 'work-item-projects',
    'work-item-history', 'work-item-comments'
  )
  AND last_synced_at = $1`, wantFamilyWatermark).Scan(&watermarkCount, &watermarkKeys); err != nil {
		t.Fatal(err)
	}
	if familyStatus != "success" || familyDataset != "work-items" || !familyAuditMatches ||
		watermarkCount != 5 || watermarkKeys != "work-item-comments,work-item-history,work-item-labels,work-item-projects,work-items" {
		t.Fatalf(
			"family status=%q dataset=%q audit=%t watermarks=%d keys=%q",
			familyStatus, familyDataset, familyAuditMatches, watermarkCount, watermarkKeys,
		)
	}

	// Unsupported family flags fail before the completion transaction. Even
	// with a non-nil proposed watermark, the unit stays running and no alias
	// watermark can be advanced.
	malformedUnitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, malformedUnitID, "incremental", `{
		"family_dataset_work_items": true,
		"family_dataset_future_alias": false
	}`)
	malformedClaimedAt := familyClaimedAt.Add(3 * time.Second)
	malformedClaim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: malformedUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: malformedClaimedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	malformedWatermark := familyWatermark.Add(time.Hour)
	if err := repository.Complete(
		ctx, malformedClaim, map[string]any{
			"records":    1,
			"incomplete": []GitHubWorkItemsIncomplete{},
		}, &malformedWatermark,
		malformedClaimedAt, malformedClaimedAt.Add(time.Second),
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("malformed family completion error=%v", err)
	}
	var malformedStatus string
	var malformedWatermarkCount int
	if err := pool.QueryRow(
		ctx, "SELECT status FROM public.sync_run_units WHERE id = $1", malformedUnitID,
	).Scan(&malformedStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.sync_watermarks
WHERE org_id = 'org-acme' AND source_id = 'acme/api' AND last_synced_at = $1`,
		malformedWatermark,
	).Scan(&malformedWatermarkCount); err != nil {
		t.Fatal(err)
	}
	if malformedStatus != "running" || malformedWatermarkCount != 0 {
		t.Fatalf(
			"malformed status=%q advanced_watermarks=%d",
			malformedStatus, malformedWatermarkCount,
		)
	}
}

func seedWorkItemAliasUnit(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	unitID string,
	mode string,
	processorFlags string,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, 'org-acme', $2, $3, $4, 'github', 'work-items', 'medium',
    $5, '2026-07-22T12:00:00Z', '2026-07-23T12:00:00Z',
    'dispatching', $6::jsonb, NOW()
)`, unitID, firstRunID, firstIntegrationID, firstSourceID, mode, processorFlags); err != nil {
		t.Fatal(err)
	}
}

// createProviderSyncFixture is DERIVED FROM THE ALEMBIC MIGRATIONS, column
// for column and constraint for constraint, not hand-guessed to match what
// the repository code happens to read today. This is the CHAOS-4050
// conversion, mirroring #1836 (internal/joboutbox) and #1844 (internal/jobs/
// metrics/daily): a prior version of this fixture invented its own schema
// for nine tables independently of alembic, the exact class of gap that let
// CHAOS-4041 (a nonexistent org_id column) and CHAOS-4043 (a text/varchar
// type conflict) ship green. The per-table authorities:
//
//   - integration_credentials: stub only (id uuid PK) -- alembic 0001 owns
//     its full shape, out of scope here; it exists solely so the real FK
//     below on integrations.credential_id is enforceable.
//   - integrations / integration_sources / integration_datasets / sync_runs /
//     sync_run_units: alembic 0015, plus 0106's unavailable_* columns on
//     integration_datasets and 0093's tenant-fence unique constraint on
//     sync_run_units. Columns alembic requires but this suite's repository
//     code never reads or writes (integrations.name/provider/is_active/
//     schedule_cron/timezone/created_at/updated_at; integration_sources.
//     provider/is_enabled/discovered_at/last_seen_at/last_sync_*;
//     integration_datasets.is_enabled; sync_runs.integration_id/triggered_by/
//     mode/total_units/completed_units/failed_units/started_at/completed_at/
//     result/created_at/trace_parent; sync_run_units.retry_exhausted_at) are
//     deliberately NOT reproduced -- adding them would force every INSERT in
//     this package to invent values for columns no assertion here ever
//     observes, without buying back any additional safety. Their real FKs/
//     unique keys that DO depend only on columns already present are kept
//     (integration_sources/integration_datasets/sync_runs -> integrations;
//     sync_run_units -> integration_sources, sync_runs; integration_datasets'
//     uq_integration_datasets_org_integration_dataset). The
//     uq_integration_sources_org_integration_provider_external constraint is
//     skipped because it is keyed partly on the omitted provider column.
//   - sync_run_unit_chunk_checkpoints / sync_run_unit_effect_chunks: alembic
//     0102. aggregate_digest and manifest_digest are varchar(64) in 0102, not
//     the unbounded text this fixture previously declared -- the same
//     hand-authored-text-masks-a-varchar-conflict shape CHAOS-4043 hit on
//     daily_metrics_runs.status, just not yet exploited by a live query here.
//   - sync_run_unit_effect_snapshots: alembic 0092+0093 (snapshotFixtureDDL,
//     unchanged, already guarded by tests/test_effect_snapshot_migration.py).
//   - sync_watermarks: alembic 0001+0015. The prior fixture also carried a
//     fabricated UNIQUE (org_id, repo_id, target) that does not exist
//     anywhere in alembic -- 0001 only ever indexed (org_id, repo_id), never
//     made it unique with target. That invented constraint made this suite
//     MORE restrictive than production, the opposite direction from
//     CHAOS-4041 but the same root cause: schema asserted here without a
//     migration behind it. Only the real 0015 constraint
//     (uq_sync_watermark_org_source_dataset) survives.
//   - sync_dispatch_outbox / sync_dispatch_transport_routes: alembic 0020+
//     0049. The prior fixture had no transport-route table, no
//     ck_sync_dispatch_outbox_claim_route_coherence /
//     ck_sync_dispatch_outbox_dispatched_route_coherence CHECK constraints,
//     and no trg_sync_dispatch_outbox_route_fence trigger -- production
//     refuses to let an outbox row claim a transport when its kind's Celery
//     route is paused, and this suite had no way to observe that at all.
//
// tests/test_providersync_fixture_ddl_matches_migrations.py fails if this
// drifts from the migrations above.
func createProviderSyncFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE public.integration_credentials (
			id uuid PRIMARY KEY
		)`,
		`CREATE TABLE public.integrations (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			credential_id uuid REFERENCES public.integration_credentials(id),
			config jsonb NOT NULL DEFAULT '{}'::jsonb
		)`,
		`CREATE TABLE public.integration_sources (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			integration_id uuid NOT NULL REFERENCES public.integrations(id),
			external_id text NOT NULL, full_name text NOT NULL,
			metadata jsonb NOT NULL DEFAULT '{}'::jsonb
		)`,
		`CREATE TABLE public.integration_datasets (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			integration_id uuid NOT NULL REFERENCES public.integrations(id),
			dataset_key text NOT NULL, options jsonb NOT NULL DEFAULT '{}'::jsonb,
			unavailable_reason varchar(64), unavailable_since timestamptz,
			unavailable_last_seen_at timestamptz,
			CONSTRAINT uq_integration_datasets_org_integration_dataset
				UNIQUE (org_id, integration_id, dataset_key)
		)`,
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY, org_id text NOT NULL, status text NOT NULL,
			credential_id uuid, credential_fingerprint text, auth_source text
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			integration_id uuid NOT NULL,
			source_id uuid NOT NULL REFERENCES public.integration_sources(id),
			provider text NOT NULL,
			dataset_key text NOT NULL, cost_class text NOT NULL, mode text NOT NULL,
			since_at timestamptz, before_at timestamptz, status text NOT NULL,
			attempts integer NOT NULL DEFAULT 0, available_at timestamptz,
			error text, result json, processor_flags jsonb, lease_owner text,
			lease_expires_at timestamptz, last_heartbeat_at timestamptz,
			duration_seconds integer, rate_limit_deferrals integer NOT NULL DEFAULT 0,
			rate_limit_first_seen_at timestamptz,
			budget_deferrals integer NOT NULL DEFAULT 0,
			budget_first_deferred_at timestamptz,
			first_blocked_at timestamptz,
			expired_lease_retry_count integer NOT NULL DEFAULT 0,
			last_retry_reason text, updated_at timestamptz NOT NULL,
			CONSTRAINT uq_sync_run_units_org_id_id_effect_snapshots
				UNIQUE (org_id, id)
		)`,
		`CREATE TABLE public.sync_run_unit_chunk_checkpoints (
			org_id text NOT NULL, sync_run_unit_id uuid NOT NULL, schema_version text NOT NULL DEFAULT 'v1', generation text NOT NULL,
			provider text NOT NULL, dataset_key text NOT NULL, route_version text NOT NULL,
			normalized_at timestamptz NOT NULL, next_cursor text NOT NULL DEFAULT '',
			inventory_complete boolean NOT NULL DEFAULT false, next_ordinal integer NOT NULL DEFAULT 0,
			prepared_chunks integer NOT NULL DEFAULT 0, total_chunks integer NOT NULL DEFAULT 0,
			final_ordinal integer NOT NULL DEFAULT -1, aggregate_result jsonb,
			aggregate_digest varchar(64), committed_rows bigint NOT NULL DEFAULT 0,
			owner text NOT NULL, lease_expires_at timestamptz NOT NULL,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			CONSTRAINT ck_sync_chunk_checkpoint_next_ordinal CHECK (next_ordinal >= 0),
			CONSTRAINT ck_sync_chunk_checkpoint_prepared_chunks CHECK (prepared_chunks >= 0),
			CONSTRAINT ck_sync_chunk_checkpoint_total_chunks CHECK (total_chunks >= 0),
			CONSTRAINT ck_sync_chunk_checkpoint_final_ordinal CHECK (final_ordinal >= -1),
			CONSTRAINT ck_sync_chunk_checkpoint_cursor CHECK (length(next_cursor) <= 4096),
			CONSTRAINT ck_sync_chunk_checkpoint_committed_rows CHECK (committed_rows >= 0),
			CONSTRAINT ck_sync_chunk_checkpoint_complete_fence CHECK (
				inventory_complete = false OR
				(total_chunks > 0 AND next_ordinal = total_chunks AND prepared_chunks = total_chunks)),
			PRIMARY KEY (org_id, sync_run_unit_id, generation),
			FOREIGN KEY (org_id, sync_run_unit_id) REFERENCES public.sync_run_units(org_id, id) ON DELETE CASCADE
		)`,
		`CREATE TABLE public.sync_run_unit_effect_chunks (
			org_id text NOT NULL, sync_run_unit_id uuid NOT NULL, schema_version text NOT NULL DEFAULT 'v1', generation text NOT NULL,
			route_version text NOT NULL, ordinal integer NOT NULL, total_chunks integer NOT NULL DEFAULT 0,
			cursor_before text NOT NULL DEFAULT '', cursor_after text NOT NULL DEFAULT '',
			inventory_complete boolean NOT NULL DEFAULT false, payload jsonb NOT NULL, ledger jsonb NOT NULL,
			payload_bytes integer NOT NULL, manifest_digest varchar(64) NOT NULL,
			status text NOT NULL DEFAULT 'pending', created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			CONSTRAINT ck_sync_chunk_ordinal CHECK (ordinal >= 0),
			CONSTRAINT ck_sync_chunk_total CHECK (total_chunks = 0 OR ordinal < total_chunks),
			CONSTRAINT ck_sync_chunk_cursors CHECK (
				length(cursor_before) <= 4096 AND length(cursor_after) <= 4096),
			CONSTRAINT ck_sync_chunk_payload_bytes CHECK (
				payload_bytes >= 1 AND payload_bytes <= 2097152),
			CONSTRAINT ck_sync_chunk_payload_object CHECK (jsonb_typeof(payload) = 'object'),
			CONSTRAINT ck_sync_chunk_ledger_object CHECK (jsonb_typeof(ledger) = 'object'),
			CONSTRAINT ck_sync_chunk_status CHECK (status IN ('pending', 'writing', 'committed')),
			PRIMARY KEY (org_id, sync_run_unit_id, generation, ordinal),
			FOREIGN KEY (org_id, sync_run_unit_id, generation)
				REFERENCES public.sync_run_unit_chunk_checkpoints(org_id, sync_run_unit_id, generation) ON DELETE CASCADE
		)`,
		// Must stay equivalent to the 0092+0093 snapshot schema. This fixture
		// previously dropped all three CHECK constraints and widened
		// content_digest to text, so every integration test here ran against a
		// schema more permissive than production -- the class of gap where a
		// test proves the code works on a table that does not exist anywhere
		// real. tests/test_effect_snapshot_migration.py::
		// test_integration_fixture_ddl_matches_snapshot_migrations fails if the two
		// drift apart.
		snapshotFixtureDDL,
		`CREATE TABLE public.sync_watermarks (
			id uuid PRIMARY KEY, org_id text NOT NULL, repo_id text NOT NULL,
			source_id text NOT NULL, target text NOT NULL, dataset_key text NOT NULL,
			last_synced_at timestamptz, updated_at timestamptz NOT NULL,
			CONSTRAINT uq_sync_watermark_org_source_dataset
				UNIQUE (org_id, source_id, dataset_key)
		)`,
		// sync_dispatch_transport_routes plus the two trigger functions below
		// are alembic 0049 verbatim. Production refuses to let an outbox row
		// claim a transport while its kind's Celery route is paused (or has
		// no active route at all) -- this table, the seeded rows, and the
		// trigger are what enforce that, and none of it existed in this
		// fixture before CHAOS-4050.
		`CREATE TABLE public.sync_dispatch_transport_routes (
			kind text PRIMARY KEY, transport text NOT NULL, generation bigint NOT NULL,
			paused boolean NOT NULL, paused_at timestamptz,
			rollback_transport text NOT NULL,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			CONSTRAINT ck_sync_dispatch_transport_routes_kind CHECK (
				kind IN ('dispatch_sync_run', 'finalize_sync_run', 'post_sync', 'reference_discovery')),
			CONSTRAINT ck_sync_dispatch_transport_routes_transport CHECK (
				transport IN ('celery', 'river')),
			CONSTRAINT ck_sync_dispatch_transport_routes_rollback CHECK (
				rollback_transport = 'celery'),
			CONSTRAINT ck_sync_dispatch_transport_routes_generation CHECK (generation >= 1),
			CONSTRAINT ck_sync_dispatch_transport_routes_pause_timestamp CHECK (
				(paused AND paused_at IS NOT NULL) OR (NOT paused AND paused_at IS NULL))
		)`,
		`INSERT INTO public.sync_dispatch_transport_routes
			(kind, transport, generation, paused, paused_at, rollback_transport, created_at, updated_at)
		SELECT kind, 'celery', 1, false, NULL, 'celery', now(), now()
		FROM unnest(ARRAY['dispatch_sync_run', 'finalize_sync_run', 'post_sync', 'reference_discovery']) AS kind`,
		`CREATE FUNCTION enforce_sync_dispatch_route_generation()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.generation < OLD.generation THEN
				RAISE EXCEPTION 'sync dispatch route generation cannot decrease';
			END IF;
			IF (
				NEW.transport IS DISTINCT FROM OLD.transport
				OR NEW.paused IS DISTINCT FROM OLD.paused
				OR NEW.paused_at IS DISTINCT FROM OLD.paused_at
				OR NEW.rollback_transport IS DISTINCT FROM OLD.rollback_transport
			) AND NEW.generation <= OLD.generation THEN
				RAISE EXCEPTION
					'sync dispatch route state change requires generation increase';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER trg_sync_dispatch_route_generation
		BEFORE UPDATE ON public.sync_dispatch_transport_routes
		FOR EACH ROW
		EXECUTE FUNCTION enforce_sync_dispatch_route_generation()`,
		`CREATE FUNCTION enforce_sync_dispatch_outbox_route_fence()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		DECLARE
			active_transport text;
			active_generation bigint;
		BEGIN
			IF (NEW.claim_token IS NULL) <> (NEW.claim_expires_at IS NULL) THEN
				RAISE EXCEPTION
					'sync dispatch claim token and expiry must change together';
			END IF;

			IF NEW.claim_token IS NOT NULL
			   AND (
				   NEW.claim_transport IS NULL
				   OR NEW.claim_route_generation IS NULL
			   ) THEN
				SELECT transport, generation
				INTO active_transport, active_generation
				FROM public.sync_dispatch_transport_routes
				WHERE kind = NEW.kind
				  AND transport = 'celery'
				  AND paused = FALSE;
				IF NOT FOUND THEN
					RAISE EXCEPTION
						'sync dispatch kind has no active celery route';
				END IF;
				NEW.claim_transport := active_transport;
				NEW.claim_route_generation := active_generation;
			END IF;

			IF NEW.status = 'dispatched'
			   AND NEW.last_error IS DISTINCT FROM 'feature_disabled' THEN
				NEW.dispatched_transport := COALESCE(
					NEW.dispatched_transport,
					NEW.claim_transport,
					OLD.claim_transport
				);
				NEW.dispatched_route_generation := COALESCE(
					NEW.dispatched_route_generation,
					NEW.claim_route_generation,
					OLD.claim_route_generation
				);
			ELSE
				NEW.dispatched_transport := NULL;
				NEW.dispatched_route_generation := NULL;
				NEW.transport_job_id := NULL;
			END IF;

			IF NEW.claim_token IS NULL THEN
				NEW.claim_transport := NULL;
				NEW.claim_route_generation := NULL;
			END IF;
			RETURN NEW;
		END;
		$$`,
		`CREATE TABLE public.sync_dispatch_outbox (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			kind text NOT NULL, status text NOT NULL, available_at timestamptz NOT NULL,
			attempts integer NOT NULL, last_error text, dispatched_at timestamptz,
			claim_token text, claim_expires_at timestamptz, claim_transport text,
			claim_route_generation bigint, dispatched_transport text,
			dispatched_route_generation bigint, transport_job_id text,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			CONSTRAINT uq_sync_dispatch_outbox_run_kind UNIQUE (sync_run_id, kind),
			CONSTRAINT ck_sync_dispatch_outbox_claim_route_coherence CHECK (
				(claim_token IS NULL AND claim_expires_at IS NULL
					AND claim_transport IS NULL AND claim_route_generation IS NULL)
				OR (claim_token IS NOT NULL AND claim_expires_at IS NOT NULL
					AND claim_transport IS NOT NULL AND claim_route_generation IS NOT NULL)),
			CONSTRAINT ck_sync_dispatch_outbox_dispatched_route_coherence CHECK (
				(status = 'dispatched' AND (
					(last_error = 'feature_disabled' AND dispatched_transport IS NULL
						AND dispatched_route_generation IS NULL AND transport_job_id IS NULL)
					OR ((last_error IS NULL OR last_error <> 'feature_disabled')
						AND dispatched_transport IS NOT NULL
						AND dispatched_route_generation IS NOT NULL)))
				OR (status <> 'dispatched' AND dispatched_transport IS NULL
					AND dispatched_route_generation IS NULL AND transport_job_id IS NULL))
		)`,
		`CREATE TRIGGER trg_sync_dispatch_outbox_route_fence
		BEFORE INSERT OR UPDATE ON public.sync_dispatch_outbox
		FOR EACH ROW
		EXECUTE FUNCTION enforce_sync_dispatch_outbox_route_fence()`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func seedProviderSyncFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.integration_credentials (id) VALUES ($1)`,
			[]any{firstCredentialID}},
		{`INSERT INTO public.integrations (id, org_id, credential_id, config)
		  VALUES ($1, 'org-acme', $2, '{"api_url":"https://api.github.com"}')`,
			[]any{firstIntegrationID, firstCredentialID}},
		{`INSERT INTO public.integration_sources
		  (id, org_id, integration_id, external_id, full_name, metadata)
		  VALUES ($1, 'org-acme', $2, 'acme/api', 'acme/api', '{"default_branch":"main"}')`,
			[]any{firstSourceID, firstIntegrationID}},
		{`INSERT INTO public.integration_datasets
		  (id, org_id, integration_id, dataset_key, options)
		  VALUES ($1, 'org-acme', $2, 'commits', '{"include_archived":false}')`,
			[]any{uuid.NewString(), firstIntegrationID}},
		{`INSERT INTO public.sync_runs
		  (id, org_id, status, credential_id, credential_fingerprint, auth_source)
		  VALUES ($1, 'org-acme', 'running', $2, 'safe-fingerprint', 'integration_credential')`,
			[]any{firstRunID, firstCredentialID}},
		{`INSERT INTO public.sync_run_units (
			id, org_id, sync_run_id, integration_id, source_id, provider,
			dataset_key, cost_class, mode, since_at, before_at, status,
			processor_flags, updated_at
		  ) VALUES (
			$1, 'org-acme', $2, $3, $4, 'github', 'commits', 'medium',
			'incremental', '2026-07-22T12:00:00Z', '2026-07-23T12:00:00Z',
			'dispatching', '{"sync_git":true,"sync_commits":true}', NOW()
		  )`, []any{firstUnitID, firstRunID, firstIntegrationID, firstSourceID}},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

// snapshotFixtureDDL mirrors the 0092 table plus 0093 tenant FK.
const snapshotFixtureDDL = `CREATE TABLE public.sync_run_unit_effect_snapshots (
	org_id text NOT NULL,
	sync_run_unit_id uuid NOT NULL,
	generation text NOT NULL,
	provider text NOT NULL,
	dataset_key text NOT NULL,
	schema_version text NOT NULL,
	content_digest varchar(64) NOT NULL,
	payload_bytes integer NOT NULL,
	payload bytea NOT NULL,
	created_at timestamptz NOT NULL,
	CONSTRAINT ck_sync_run_unit_effect_snapshots_payload_bytes
		CHECK (payload_bytes >= 1 AND payload_bytes <= 67108864),
	CONSTRAINT ck_sync_run_unit_effect_snapshots_payload_length
		CHECK (length(payload) = payload_bytes),
	CONSTRAINT ck_sync_run_unit_effect_snapshots_schema_version
		CHECK (schema_version = 'v1'),
	PRIMARY KEY (org_id, sync_run_unit_id, generation),
	CONSTRAINT fk_sync_run_unit_effect_snapshots_tenant_unit
		FOREIGN KEY (org_id, sync_run_unit_id)
		REFERENCES public.sync_run_units(org_id, id)
		ON DELETE CASCADE
)`

// workItemsFamilyUnitID is the CHAOS-3940 fixture unit: a healthy GitHub
// work-items claim carrying the complete five-dataset family flag set.
const workItemsFamilyUnitID = "66666666-6666-4666-8666-666666666666"

// TestPostgresCompleteLandsGitHubWorkItemsFamilyWatermarks pins CHAOS-3940 at
// the effect boundary.
//
// github/work-items produced zero successful units after the Go cutover. Every
// one committed all sixteen ClickHouse destinations and then failed, so the
// ledger reported "committed" while the unit never terminalized and not one of
// the five family watermarks ever advanced past the cutover instant. A test
// that only asserted Complete was CALLED would have passed throughout.
//
// This drives the two real applications of applyGitHubWorkItemsIncompletePolicy
// in their production order -- CompleteRouteExecutor.Execute, then
// PostgresRepository.Complete -- against the real completion SQL, and asserts
// the observable effect: the unit is terminal and all five alias watermarks
// carry the window's close.
func TestPostgresCompleteLandsGitHubWorkItemsFamilyWatermarks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key, options)
VALUES ($1, 'org-acme', $2, 'work-items', '{}'::jsonb)`,
		uuid.NewString(), firstIntegrationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, 'org-acme', $2, $3, $4, 'github', 'work-items', 'medium',
    'incremental', '2026-08-19T20:00:00Z', '2026-08-20T01:00:00Z',
    'dispatching',
    '{"sync_prs":true,
      "family_dataset_work_items":true,
      "family_dataset_work_item_labels":true,
      "family_dataset_work_item_history":true,
      "family_dataset_work_item_comments":true,
      "family_dataset_work_item_projects":true}'::jsonb,
    NOW()
)`, workItemsFamilyUnitID, firstRunID, firstIntegrationID, firstSourceID); err != nil {
		t.Fatal(err)
	}

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, time.August, 20, 1, 1, 40, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: workItemsFamilyUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: startedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Exactly what GitHubWorkItemsRouteHandler.Collect emits when every optional
	// enrichment phase succeeded, then normalized once by the executor.
	windowClose := time.Date(2026, time.August, 20, 1, 0, 0, 0, time.UTC)
	routeResult, routeWatermark, err := applyGitHubWorkItemsIncompletePolicy(
		claim.Provider, claim.Dataset,
		map[string]any{
			"records":                          48,
			githubWorkItemsIncompleteResultKey: []GitHubWorkItemsIncomplete{},
		},
		&windowClose,
	)
	if err != nil {
		t.Fatalf("executor-side policy rejected a healthy batch: %v", err)
	}

	if err := repository.Complete(
		ctx, claim, routeResult, routeWatermark, startedAt,
		startedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("Complete refused a healthy GitHub work-items unit: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM public.sync_run_units WHERE id = $1`,
		workItemsFamilyUnitID,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "success" {
		t.Fatalf("unit status=%q want success", status)
	}

	// The effect that matters: every alias watermark advanced to the window
	// close. While the bug stood these five rows did not exist at all, so the
	// planner re-fetched the same window forever and coverage saw no gap.
	rows, err := pool.Query(ctx, `
SELECT dataset_key, last_synced_at FROM public.sync_watermarks
WHERE org_id = 'org-acme' AND source_id = $1 ORDER BY dataset_key`,
		claim.SourceExternalID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	landed := map[string]time.Time{}
	for rows.Next() {
		var datasetKey string
		var syncedAt time.Time
		if err := rows.Scan(&datasetKey, &syncedAt); err != nil {
			t.Fatal(err)
		}
		landed[datasetKey] = syncedAt.UTC()
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, datasetKey := range []string{
		"work-items", "work-item-labels", "work-item-history",
		"work-item-comments", "work-item-projects",
	} {
		got, present := landed[datasetKey]
		if !present {
			t.Fatalf("watermark for %q never landed; landed=%v", datasetKey, landed)
		}
		if !got.Equal(windowClose) {
			t.Fatalf("watermark %q=%v want %v", datasetKey, got, windowClose)
		}
	}
	if len(landed) != 5 {
		t.Fatalf("unexpected watermark set: %v", landed)
	}
}
