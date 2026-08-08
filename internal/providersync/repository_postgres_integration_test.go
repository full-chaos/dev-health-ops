//go:build integration

package providersync

import (
	"context"
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

	// The Python planner collapses the enabled GitHub work-item family onto one
	// canonical work-items unit. Completion keeps that claim identity while
	// atomically fanning its watermark and result audit back out to the enabled
	// aliases.
	familyUnitID := uuid.NewString()
	seedWorkItemAliasUnit(t, ctx, pool, familyUnitID, "incremental", `{
		"sync_prs": true,
		"family_dataset_work_items": true,
		"family_dataset_work_item_labels": true,
		"family_dataset_work_item_comments": true
	}`)
	familyClaimedAt := completedAt.Add(time.Second)
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
		ctx, familyClaim, map[string]any{"records": 7}, &familyWatermark,
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
         '["work-items","work-item-labels","work-item-comments"]'::jsonb
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
  AND dataset_key IN ('work-items', 'work-item-labels', 'work-item-comments')
  AND last_synced_at = $1`, wantFamilyWatermark).Scan(&watermarkCount, &watermarkKeys); err != nil {
		t.Fatal(err)
	}
	if familyStatus != "success" || familyDataset != "work-items" || !familyAuditMatches ||
		watermarkCount != 3 || watermarkKeys != "work-item-comments,work-item-labels,work-items" {
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
		ctx, malformedClaim, map[string]any{"records": 1}, &malformedWatermark,
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

func createProviderSyncFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE public.integrations (
			id uuid PRIMARY KEY, org_id text NOT NULL, credential_id uuid,
			config jsonb NOT NULL DEFAULT '{}'::jsonb
		)`,
		`CREATE TABLE public.integration_sources (
			id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
			external_id text NOT NULL, full_name text NOT NULL,
			metadata jsonb NOT NULL DEFAULT '{}'::jsonb
		)`,
		`CREATE TABLE public.integration_datasets (
			id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
			dataset_key text NOT NULL, options jsonb NOT NULL DEFAULT '{}'::jsonb
		)`,
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY, org_id text NOT NULL, status text NOT NULL,
			credential_id uuid, credential_fingerprint text, auth_source text
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL,
			integration_id uuid NOT NULL, source_id uuid NOT NULL, provider text NOT NULL,
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
			last_retry_reason text, updated_at timestamptz NOT NULL
		)`,
		// Must stay byte-for-byte equivalent to migration 0092. This fixture
		// previously dropped all three CHECK constraints and widened
		// content_digest to text, so every integration test here ran against a
		// schema more permissive than production -- the class of gap where a
		// test proves the code works on a table that does not exist anywhere
		// real. tests/test_effect_snapshot_migration.py::
		// test_integration_fixture_ddl_matches_migration_0088 fails if the two
		// drift apart.
		snapshotFixtureDDL,
		`CREATE TABLE public.sync_watermarks (
			id uuid PRIMARY KEY, org_id text NOT NULL, repo_id text NOT NULL,
			source_id text NOT NULL, target text NOT NULL, dataset_key text NOT NULL,
			last_synced_at timestamptz, updated_at timestamptz NOT NULL,
			UNIQUE (org_id, repo_id, target),
			UNIQUE (org_id, source_id, dataset_key)
		)`,
		`CREATE TABLE public.sync_dispatch_outbox (
			id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL,
			kind text NOT NULL, status text NOT NULL, available_at timestamptz NOT NULL,
			attempts integer NOT NULL, last_error text, dispatched_at timestamptz,
			claim_token text, claim_expires_at timestamptz, claim_transport text,
			claim_route_generation bigint, dispatched_transport text,
			dispatched_route_generation bigint, transport_job_id text,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			UNIQUE (sync_run_id, kind)
		)`,
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

// snapshotFixtureDDL mirrors migration 0092 exactly, constraints included.
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
	FOREIGN KEY (sync_run_unit_id) REFERENCES public.sync_run_units(id)
		ON DELETE CASCADE
)`
