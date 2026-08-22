//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestQueryExecutedProofEvidenceDistinguishesRealFromEmptySuccess is the
// CHAOS-4060 positive/negative control set: it seeds the exact motivating
// shapes from CHAOS-4048/CHAOS-4049 -- a Go route that actually wrote rows, a
// Go route whose window happened to be empty, a legacy Python row that
// "succeeded" with zero persisted rows (the pagerduty/teams counterexample),
// a legacy Python row that genuinely persisted rows, a unit that would count
// except it never reached success, and a malformed result blob -- and asserts
// the evidence query tells exactly those cases apart.
func TestQueryExecutedProofEvidenceDistinguishesRealFromEmptySuccess(t *testing.T) {
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

	credentialID := uuid.NewString()
	integrationID := uuid.NewString()
	sourceID := uuid.NewString()
	runID := uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.integration_credentials (id) VALUES ($1)`, credentialID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.integrations (id, org_id, credential_id, config)
		 VALUES ($1, 'org-acme', $2, '{}')`, integrationID, credentialID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.integration_sources
		 (id, org_id, integration_id, external_id, full_name, metadata)
		 VALUES ($1, 'org-acme', $2, 'acme/api', 'acme/api', '{}')`,
		sourceID, integrationID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.sync_runs
		 (id, org_id, integration_id, status, credential_id, credential_fingerprint, auth_source)
		 VALUES ($1, 'org-acme', $2, 'success', $3, 'safe-fingerprint', 'integration_credential')`,
		runID, integrationID, credentialID,
	); err != nil {
		t.Fatal(err)
	}

	type seedUnit struct {
		provider, dataset, status, result string
	}
	seeds := []seedUnit{
		// Go route, real rows written: proven.
		{"github", "prs", "success", `{"go_provider_route":{"effects_written":3,"effects_skipped":0}}`},
		// Go route, success but nothing to write this window: not proven --
		// a clean empty run is not evidence the writer path has ever landed
		// a row.
		{"github", "commits", "success", `{"go_provider_route":{"effects_written":0}}`},
		// The CHAOS-4049 motivating counterexample: legacy Python "success"
		// that persisted nothing, 149 times over in prod. Must not count.
		{"pagerduty", "teams", "success", `{"persisted":0}`},
		{"pagerduty", "teams", "success", `{"persisted":0}`},
		// Legacy Python row that genuinely persisted rows: proven.
		{"jira", "incidents", "success", `{"persisted":5}`},
		// Real effects_written, but the unit never reached success: not
		// proven. A row's own claimed effect count is not evidence unless
		// the unit terminalized successfully.
		{"gitlab", "security", "failed", `{"go_provider_route":{"effects_written":9}}`},
		// Malformed/non-numeric result payload: must not crash the query and
		// must not manufacture evidence out of an unparseable value.
		{"launchdarkly", "feature-flags", "success", `{"go_provider_route":{"effects_written":"NaN"}}`},
	}
	for _, seed := range seeds {
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
	id, org_id, sync_run_id, integration_id, source_id, provider,
	dataset_key, cost_class, mode, status, processor_flags, result, updated_at
) VALUES (
	$1, 'org-acme', $2, $3, $4, $5, $6, 'medium', 'incremental', $7,
	'{}', $8, NOW()
)`,
			uuid.NewString(), runID, integrationID, sourceID,
			seed.provider, seed.dataset, seed.status, seed.result,
		); err != nil {
			t.Fatalf("seed %+v: %v", seed, err)
		}
	}

	evidence, err := QueryExecutedProofEvidence(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	want := ExecutedProofEvidence{
		"github/prs":     true,
		"jira/incidents": true,
	}
	if len(evidence) != len(want) {
		t.Fatalf("evidence = %+v, want exactly %+v", evidence, want)
	}
	for key := range want {
		if !evidence[key] {
			t.Errorf("evidence missing proof for %q: %+v", key, evidence)
		}
	}
	// Negative controls: none of these ever proves itself, no matter how the
	// gate consults it.
	for _, absent := range []string{
		"github/commits", "pagerduty/teams", "gitlab/security", "launchdarkly/feature-flags",
	} {
		if evidence[absent] {
			t.Errorf("evidence wrongly proved %q: %+v", absent, evidence)
		}
	}
}
