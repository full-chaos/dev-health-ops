//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-4290, #2241 r2 Finding 3.
//
// A native finalize executor scopes its ClickHouse reads and writes by the
// run's TARGET DAY, taken from the claimed Run. ClaimFinalize's RETURNING
// clause listed five columns and target_day was not among them, so
// claim.Run.TargetDay was the zero time.Time and the executor would have
// computed for year 1 -- reading nothing and writing nothing, on every run.
//
// Why this needs REAL Postgres and could not be caught by the existing proofs:
// the defect is in a RETURNING clause. A fake store constructs Run directly and
// therefore always has whatever fields the test chose to set, and the
// real-ClickHouse two-writer proof hard-coded its date in the fake family
// rather than reading the Run it was handed. Every existing test supplied the
// field the query omits, which is exactly why none of them saw it.
//
// ClaimPartition already returns target_day. The finalize claim was the odd one
// out, and that asymmetry is what made it read as correct.
func TestClaimFinalizePopulatesTheRunsTargetDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		runID = "00000000-0000-4000-8000-000000000031"
		orgID = "00000000-0000-4000-8000-000000000039"
	)
	// Deliberately NOT the day the clock would suggest: a zero TargetDay and a
	// TargetDay accidentally defaulted from now() are different bugs, and a
	// fixture dated "today" cannot tell them apart.
	targetDay := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 4, 18, 0, 0, 0, time.UTC)

	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs
        (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
        VALUES ($1,$2,$3,'daily-v1','running','pending',$4,$4)`,
		runID, orgID, targetDay, now); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	claim, err := store.ClaimFinalize(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("ClaimFinalize returned no claim for a running run with no partitions")
	}
	if claim.Run.TargetDay.IsZero() {
		t.Fatal("claim.Run.TargetDay is the ZERO time -- a native finalize family " +
			"scoping its reads by the run's day would compute year 1, reading and " +
			"writing nothing, on every single run")
	}
	if got := claim.Run.TargetDay.UTC(); !got.Equal(targetDay) {
		t.Fatalf("claim.Run.TargetDay = %s, want %s -- the claim must carry the run's "+
			"OWN day, not the clock's", got.Format(time.RFC3339), targetDay.Format(time.RFC3339))
	}
	// The other claimed fields must survive the added column: a RETURNING/Scan
	// pair that drifts by one position corrupts every field after the gap, and
	// would still satisfy the assertion above.
	if claim.Run.ID != runID || claim.Run.OrganizationID != orgID {
		t.Fatalf("claim.Run = {ID:%q Org:%q}, want {%q %q} -- RETURNING and Scan "+
			"have drifted out of alignment", claim.Run.ID, claim.Run.OrganizationID, runID, orgID)
	}
	if claim.Run.Status != "running" || claim.Token == "" {
		t.Fatalf("claim.Run.Status=%q token empty=%v", claim.Run.Status, claim.Token == "")
	}
}
