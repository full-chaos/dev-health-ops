//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ledgerRow is what an operator (and the gate) can actually observe.
type ledgerRow struct {
	attemptedAt time.Time
	provenAt    *time.Time
}

func readLedger(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]ledgerRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT provider, dataset_key, attempted_at, proven_at
FROM public.sync_executed_proof_ledger`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ledger := map[string]ledgerRow{}
	for rows.Next() {
		var provider, dataset string
		var entry ledgerRow
		if err := rows.Scan(&provider, &dataset, &entry.attemptedAt, &entry.provenAt); err != nil {
			t.Fatal(err)
		}
		ledger[provider+"/"+dataset] = entry
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ledger
}

func startLedgerPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
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
	return pool
}

// TestPostgresCompleteStampsExecutedProofLedger drives the REAL completion
// path -- Claim then Complete, the same two calls providerunit.Handler makes
// -- and asserts the ledger the CHAOS-4060 gate now reads came out right.
//
// It matters that this goes through Complete rather than calling
// RecordExecutedProofTerminal directly: the defect this ticket has to prevent
// is not "the stamp computes the wrong verdict", it is "a write path exists
// that never stamps at all". A test that calls the stamp itself cannot fail
// for that reason.
func TestPostgresCompleteStampsExecutedProofLedger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startLedgerPostgres(t, ctx)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, time.July, 23, 12, 1, 40, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: startedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Exactly the payload shape providerunit.Handler builds around a real
	// route result (internal/jobs/providerunit/providerunit.go).
	provenAtCall := startedAt.Add(2 * time.Second)
	if err := repository.Complete(ctx, claim, map[string]any{
		"go_provider_route": map[string]any{
			"effects_written": 1, "effects_skipped": 0, "records": 7,
		},
	}, nil, startedAt, provenAtCall); err != nil {
		t.Fatalf("Complete refused a healthy unit: %v", err)
	}

	ledger := readLedger(t, ctx, pool)
	entry, ok := ledger["github/commits"]
	if !ok {
		t.Fatalf("Complete wrote no ledger row at all: %+v", ledger)
	}
	if entry.provenAt == nil {
		t.Fatalf("github/commits completed with records=7 and is not proven: %+v", entry)
	}
	firstProvenAt := *entry.provenAt

	// A LATER completion of the same pair that persisted nothing must not
	// un-prove it. This is the invariant bool_or carried in the query the
	// ledger replaces: proof is permanent, and a quiet window afterwards is
	// not counter-evidence. Getting this wrong would make the gate block a
	// working route the first time it had nothing to fetch.
	secondUnitID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, 'org-acme', $2, $3, $4, 'github', 'commits', 'medium',
    'incremental', '2026-07-23T12:00:00Z', '2026-07-24T12:00:00Z',
    'dispatching', '{"sync_git":true,"sync_commits":true}', NOW()
)`, secondUnitID, firstRunID, firstIntegrationID, firstSourceID); err != nil {
		t.Fatal(err)
	}
	emptyStartedAt := startedAt.Add(time.Hour)
	emptyClaim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: secondUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: emptyStartedAt, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Complete(ctx, emptyClaim, map[string]any{
		"go_provider_route": map[string]any{
			// The CHAOS-4049 regression shape: a batch committed with zero
			// rows in it. effects_written is 1 and records is 0, and only
			// records may count.
			"effects_written": 1, "effects_skipped": 0, "records": 0,
		},
	}, nil, emptyStartedAt, emptyStartedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ledger = readLedger(t, ctx, pool)
	entry = ledger["github/commits"]
	if entry.provenAt == nil || !entry.provenAt.Equal(firstProvenAt) {
		t.Fatalf("an empty later window changed the proving instant: got %+v want %s",
			entry.provenAt, firstProvenAt)
	}
}

// TestExecutedProofLedgerAttemptedIsMonotoneAndNeverClobbersProof pins the two
// directions a re-plan could corrupt the ledger: moving attempted_at forward
// (which would erase when a pair was first tried) and, far worse, resetting
// proven_at (which would silently block a route that had already proved
// itself).
func TestExecutedProofLedgerAttemptedIsMonotoneAndNeverClobbersProof(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startLedgerPostgres(t, ctx)
	createProviderSyncFixture(t, ctx, pool)

	first := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if err := RecordExecutedProofAttempted(
		ctx, pool, []string{"GitHub", " github "}, []string{"Commits", "commits"}, first,
	); err != nil {
		t.Fatal(err)
	}
	ledger := readLedger(t, ctx, pool)
	if len(ledger) != 1 {
		t.Fatalf("case-and-whitespace variants of one pair produced %d rows: %+v",
			len(ledger), ledger)
	}
	if _, ok := ledger["github/commits"]; !ok {
		t.Fatalf("pair was not normalized to lowercase: %+v", ledger)
	}

	// Prove it, then re-plan it.
	proven := first.Add(time.Hour)
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_executed_proof_ledger SET proven_at = $1`, proven); err != nil {
		t.Fatal(err)
	}
	if err := RecordExecutedProofAttempted(
		ctx, pool, []string{"github"}, []string{"commits"}, first.Add(2*time.Hour),
	); err != nil {
		t.Fatal(err)
	}
	ledger = readLedger(t, ctx, pool)
	entry := ledger["github/commits"]
	if !entry.attemptedAt.Equal(first) {
		t.Errorf("re-planning moved attempted_at to %s, want the original %s",
			entry.attemptedAt, first)
	}
	if entry.provenAt == nil || !entry.provenAt.Equal(proven) {
		t.Errorf("re-planning disturbed proven_at: got %+v want %s", entry.provenAt, proven)
	}

	// An empty pair list must be a no-op, not an error: a zero-unit plan is
	// legitimate and must not fail the materialization it belongs to.
	if err := RecordExecutedProofAttempted(ctx, pool, nil, nil, first); err != nil {
		t.Errorf("empty attempt batch errored: %v", err)
	}
	// A pair with an empty half is dropped rather than written: it could only
	// ever produce a row no route lookup can match.
	if err := RecordExecutedProofAttempted(
		ctx, pool, []string{"", "gitlab"}, []string{"prs", "  "}, first,
	); err != nil {
		t.Fatal(err)
	}
	if ledger := readLedger(t, ctx, pool); len(ledger) != 1 {
		t.Errorf("empty identity halves reached the ledger: %+v", ledger)
	}
}

// TestExecutedProofLedgerRefusesUnnormalizedIdentity proves the lowercase
// invariant is enforced by the DATABASE, not by writer discipline. A writer
// that forgot lower() would otherwise create a second, invisible row for the
// same pair: the gate looks pairs up in lowercase, so the route's proof would
// sit in a row nothing ever reads and the route would read as never-attempted
// -- the fail-open direction.
func TestExecutedProofLedgerRefusesUnnormalizedIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startLedgerPostgres(t, ctx)
	createProviderSyncFixture(t, ctx, pool)

	for _, refused := range []struct{ provider, dataset string }{
		{"GitHub", "commits"},
		{"github", "Commits"},
		{"", "commits"},
		{"github", "   "},
	} {
		_, err := pool.Exec(ctx, `
INSERT INTO public.sync_executed_proof_ledger (provider, dataset_key, attempted_at)
VALUES ($1, $2, NOW())`, refused.provider, refused.dataset)
		if err == nil {
			t.Errorf("the database accepted an unnormalized identity %q/%q",
				refused.provider, refused.dataset)
			continue
		}
		if !strings.Contains(err.Error(), "ck_sync_executed_proof_ledger") {
			t.Errorf("%q/%q was refused for the wrong reason: %v",
				refused.provider, refused.dataset, err)
		}
	}
}
