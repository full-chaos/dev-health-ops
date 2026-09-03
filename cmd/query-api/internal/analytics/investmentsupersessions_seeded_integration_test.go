//go:build integration

package analytics

// Seeded real-engine proof for CHAOS-4441 plan.md section 5a's dedup
// obligation: a work_unit_id present in work_unit_supersessions must be
// invisible to every reader composed from latestWorkUnitInvestmentsSource,
// UNCONDITIONALLY -- independent of investmentMembershipScopeFilter's own
// scope_enabled gate (investmentsupersessions.go's binding condition).
//
// Reuses investmentquality_seeded_integration_test.go's harness
// (seededQualitySchemaDDL, seedQualityRows, splitSQLStatements) and its
// already-proven reader (resolveEvidenceQualityStats) rather than
// inventing a second one -- this test's only new claim is the exclusion,
// not the aggregate arithmetic that file already covers.
//
// SCOPE STATE FOR THIS TEST: work_unit_membership_runs is left empty, same
// as investmentquality_seeded_integration_test.go, so scope_enabled = 0
// (scope_mode = "unscoped_no_marker") -- investmentMembershipScopeFilter's
// own OR-condition passes every row through regardless of membership
// scoping. That is deliberate: it isolates the supersession exclusion from
// the membership-scope gate, proving the two mechanisms are independent as
// the binding condition requires, not merely that "some" gate excludes the
// row.

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func seedSupersession(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID, supersededWorkUnitID, supersededByRunID string) {
	t.Helper()
	insert := fmt.Sprintf(
		"INSERT INTO work_unit_supersessions (org_id, superseded_work_unit_id, superseded_by_run_id, superseded_at) VALUES ('%s', '%s', '%s', toDateTime64('2026-01-07 00:00:00', 3))",
		orgID, supersededWorkUnitID, supersededByRunID,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed work_unit_supersessions: %v", err)
	}
}

// TestSupersededWorkUnitsAreExcludedFromInvestmentQuality proves the
// exclusion two ways in one run: a control (no supersession seeded --
// count still includes the row, so the test cannot pass vacuously) and
// the actual claim (supersession seeded -- the row disappears).
func TestSupersededWorkUnitsAreExcludedFromInvestmentQuality(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const orgID = "chaos-4441-supersession-exclusion"
	seedQualityRows(t, ctx, conn, orgID, []seededQualityRow{
		{workUnitID: "wu-survives", evidence: 0.90, band: "high"},
		{workUnitID: "wu-superseded", evidence: 0.20, band: "low"},
	})

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	batch := model.AnalyticsRequestInput{
		Breakdowns: []model.BreakdownRequestInput{{
			Dimension: model.DimensionInputRepo,
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{
				StartDate: mustGraphQLDate("2026-01-01"),
				EndDate:   mustGraphQLDate("2026-01-08"),
			},
			TopN: 10,
		}},
		UseInvestment: boolPtr(true),
	}

	// CONTROL: before any supersession is seeded, both rows are visible --
	// this is what makes the assertion after seeding decisive rather than
	// a test that could never have failed.
	before, err := resolveEvidenceQualityStats(ctx, client, orgID, batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats (control): %v", err)
	}
	if before == nil || before.Total != 2 {
		t.Fatalf("control: Total = %+v, want 2 (both rows visible pre-supersession)", before)
	}

	seedSupersession(t, ctx, conn, orgID, "wu-superseded", "run-2")

	after, err := resolveEvidenceQualityStats(ctx, client, orgID, batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats (after supersession): %v", err)
	}
	if after == nil {
		t.Fatal("expected a populated EvidenceQualityStats after supersession, got nil")
	}
	if after.Total != 1 {
		t.Fatalf("Total = %d, want 1 (wu-superseded excluded, wu-survives still counted)", after.Total)
	}
	if after.Mean == nil || *after.Mean != 0.90 {
		t.Fatalf("Mean = %v, want 0.90 (only wu-survives' evidence_quality should remain)", after.Mean)
	}
}
