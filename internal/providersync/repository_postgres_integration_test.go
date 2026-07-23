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
		UnitID: firstUnitID, Owner: firstOwner, Now: now,
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
		UnitID: firstUnitID, Owner: uuid.NewString(), Now: now.Add(30 * time.Second),
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

	secondOwner := uuid.NewString()
	second, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, Owner: secondOwner, Now: now.Add(91 * time.Second),
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Recovered || second.Attempt != 2 || second.GenerationKey() != first.GenerationKey() {
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
			expired_lease_retry_count integer NOT NULL DEFAULT 0,
			last_retry_reason text, updated_at timestamptz NOT NULL
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
