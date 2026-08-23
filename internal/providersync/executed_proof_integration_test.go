//go:build integration

package providersync

import (
	"context"
	"reflect"
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
// except it never reached success, a malformed result blob, and a legacy row
// planned directly under an ALIAS identity that must canonicalize onto its
// writer -- and asserts the evidence query's tri-state (Proven / Attempted /
// neither) tells exactly those cases apart.
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
		{"github", "prs", "success", `{"go_provider_route":{"records":3,"effects_written":1}}`},
		// Go route, success but nothing to write this window: not proven --
		// a clean empty run is not evidence the writer path has ever landed
		// a row.
		{"github", "commits", "success", `{"go_provider_route":{"records":0,"effects_written":0}}`},
		// The codex-review regression case: a route that commits an EMPTY
		// effect batch (an optional upstream API returning nothing this
		// window) still increments effects_written once per batch, but
		// records -- the actual row count -- is 0. Must not count: proving
		// on effects_written here would readmit the exact CHAOS-4049 shape
		// (a "successful" unit that persisted nothing) through the gate's
		// own evidence source.
		{"github", "deployments", "success", `{"go_provider_route":{"records":0,"effects_written":1}}`},
		// The CHAOS-4049 motivating counterexample: legacy Python "success"
		// that persisted nothing, 149 times over in prod. Must not count.
		{"pagerduty", "teams", "success", `{"persisted":0}`},
		{"pagerduty", "teams", "success", `{"persisted":0}`},
		// Legacy Python row that genuinely persisted rows: proven.
		{"jira", "incidents", "success", `{"persisted":5}`},
		// Real records, but the unit never reached success: not proven. A
		// row's own claimed record count is not evidence unless the unit
		// terminalized successfully.
		{"gitlab", "security", "failed", `{"go_provider_route":{"records":9}}`},
		// Malformed/non-numeric result payload: must not crash the query and
		// must not manufacture evidence out of an unparseable value.
		{"launchdarkly", "feature-flags", "success", `{"go_provider_route":{"records":"NaN"}}`},
		// A value that IS all-digits but overflows bigint (19+ digits) must
		// not crash the whole evidence query either -- it is silently not
		// evidence, exactly like a malformed value, rather than an
		// unhandled Postgres "value out of range" error that would fail
		// every OTHER pair's evidence too.
		{"gitlab", "blame", "success", `{"go_provider_route":{"records":99999999999999999999}}`},
		{"jira", "work-items", "success", `{"persisted":99999999999999999999}`},
		// Boundary control: an 18-digit value is always in bigint range and
		// must still prove -- the digit-length bound must not reject a
		// legitimate (if implausibly large) value that happens to be long.
		{"gitlab", "cicd", "success", `{"go_provider_route":{"records":999999999999999999}}`},
		// The codex-review round-3 alias-canonicalization case: a
		// pre-CHAOS-4054 row planned directly under the "tests" alias
		// identity (Python never collapsed aliases onto their canonical
		// writer the way the Go registry does). This is real, durable proof
		// of the SAME writer github/cicd owns -- canonicalRouteIdentity
		// folds "tests" onto "cicd", so this row must prove github/cicd even
		// though no row is ever seeded under "cicd" itself for github.
		{"github", "tests", "success", `{"persisted":8}`},
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

	// CHAOS-4114: QueryExecutedProofEvidence now reads the maintained ledger
	// instead of rescanning sync_run_units, so this test projects the seeded
	// history into the ledger exactly the way alembic 0109 does and then
	// makes every original assertion against the read. That is the whole
	// equivalence claim of this ticket, checked on the hardest fixture the
	// repository has: the malformed blob, the bigint overflow on both key
	// shapes, the 18-digit boundary, the empty-but-successful window, the
	// zero-row effect batch, and the pre-CHAOS-4054 alias row. If the ledger
	// projection meant anything different from the scan it replaced, one of
	// the assertions below would have to move.
	if _, err := pool.Exec(ctx, ExecutedProofLedgerBackfillSQL); err != nil {
		t.Fatalf("backfill executed-proof ledger: %v", err)
	}
	// Running it a second time must change nothing. The migration is not
	// re-runnable in practice, but the same statement is the recovery tool an
	// operator reaches for, and a backfill that double-counted or clobbered
	// would be discovered at the worst possible moment.
	if _, err := pool.Exec(ctx, ExecutedProofLedgerBackfillSQL); err != nil {
		t.Fatalf("re-run executed-proof ledger backfill: %v", err)
	}

	evidence, err := QueryExecutedProofEvidence(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}

	// The scan this ledger replaces, run against the same fixture. Every
	// assertion below is made against `evidence` (the ledger read); this
	// compares the two directly so a future change to either side that made
	// them disagree fails here rather than silently changing what the gate
	// blocks.
	legacy, err := queryExecutedProofEvidenceByFullScan(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(evidence, legacy) {
		t.Fatalf("ledger read and legacy full scan disagree:\n ledger = %+v\n  scan  = %+v",
			evidence, legacy)
	}

	wantProven := map[string]bool{
		"github/prs":     true,
		"jira/incidents": true,
		"gitlab/cicd":    true,
		// Proven ONLY via the alias-canonicalization fold from the
		// github/tests seed row above -- no row is ever seeded directly
		// under github/cicd.
		"github/cicd": true,
	}
	for key := range wantProven {
		if !evidence.Proven[key] {
			t.Errorf("evidence.Proven missing %q: %+v", key, evidence.Proven)
		}
	}
	if len(evidence.Proven) != len(wantProven) {
		t.Fatalf("evidence.Proven = %+v, want exactly %+v", evidence.Proven, wantProven)
	}

	// Every seeded pair (any status) must be Attempted, regardless of
	// whether it ever proved itself -- that is what lets a genuinely
	// unproven pair distinguish itself from one that was simply never tried.
	wantAttempted := map[string]bool{
		"github/prs": true, "github/commits": true, "github/deployments": true,
		"pagerduty/teams": true, "jira/incidents": true, "gitlab/security": true,
		"launchdarkly/feature-flags": true, "gitlab/blame": true, "jira/work-items": true,
		"gitlab/cicd": true, "github/cicd": true,
	}
	for key := range wantAttempted {
		if !evidence.Attempted[key] {
			t.Errorf("evidence.Attempted missing %q: %+v", key, evidence.Attempted)
		}
	}
	if len(evidence.Attempted) != len(wantAttempted) {
		t.Fatalf("evidence.Attempted = %+v, want exactly %+v", evidence.Attempted, wantAttempted)
	}

	// Negative controls: attempted but never proven, no matter how the gate
	// consults it.
	for _, unproven := range []string{
		"github/commits", "github/deployments", "pagerduty/teams", "gitlab/security",
		"launchdarkly/feature-flags", "gitlab/blame", "jira/work-items",
	} {
		if evidence.Proven[unproven] {
			t.Errorf("evidence wrongly proved %q: %+v", unproven, evidence.Proven)
		}
		if !evidence.Attempted[unproven] {
			t.Errorf("evidence should still record %q as attempted: %+v", unproven, evidence.Attempted)
		}
	}

	// A pair nobody ever seeded at all: never attempted, never proven --
	// the bootstrap-friendly case ExecutedProofSatisfied lets through.
	for _, neverTouched := range []string{"github/blame", "linear/work-items"} {
		if evidence.Proven[neverTouched] || evidence.Attempted[neverTouched] {
			t.Errorf("evidence has %q, want it absent from both sets entirely: proven=%v attempted=%v",
				neverTouched, evidence.Proven[neverTouched], evidence.Attempted[neverTouched])
		}
	}
}

// queryExecutedProofEvidenceByFullScan is the pre-CHAOS-4114 implementation,
// preserved here as the oracle the ledger read is checked against. It is
// deliberately a copy of the production body with only the SQL constant
// swapped: a shared helper parameterized by query would let a change to the
// scanning/canonicalization half alter both sides at once and still pass.
func queryExecutedProofEvidenceByFullScan(
	ctx context.Context, db *pgxpool.Pool,
) (*ExecutedProofEvidence, error) {
	rows, err := db.Query(ctx, executedProofEvidenceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	evidence := &ExecutedProofEvidence{
		Proven:    make(map[string]bool),
		Attempted: make(map[string]bool),
	}
	for rows.Next() {
		var provider, dataset string
		var proven bool
		if err := rows.Scan(&provider, &dataset, &proven); err != nil {
			return nil, err
		}
		key := matrixKey(provider, canonicalRouteIdentity(dataset))
		evidence.Attempted[key] = true
		if proven {
			evidence.Proven[key] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return evidence, nil
}
