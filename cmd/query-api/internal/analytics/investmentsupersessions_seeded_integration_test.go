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
// SCOPE STATE: the test runs the claim under BOTH scope_enabled values.
// Phase 1 leaves work_unit_membership_runs empty (scope_enabled = 0,
// scope_mode = "unscoped_no_marker") -- investmentMembershipScopeFilter's
// own OR-condition passes every row through regardless of membership
// scoping, isolating the supersession exclusion from the membership gate.
// Phase 2 (oci-image's peer-read finding, the only prior gap) then seeds a
// completed membership run with BOTH work units present in
// work_unit_membership under it, flipping scope_mode to "scoped"
// (scope_enabled = 1) -- so the membership filter's own IN-clause would, on
// its own, ADMIT wu-superseded. Only phase 2 proves the binding condition
// for real: that the exclusion holds even when the scope gate would not
// have provided it, not merely when the scope gate is inert.

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
// exclusion under both scope_enabled values: a control (no supersession
// seeded -- count still includes the row, so the test cannot pass
// vacuously), the claim under scope_enabled=0, then the claim under
// scope_enabled=1 with membership rows present for the superseded unit
// too -- proving the exclusion is not merely redundant with the scope
// gate.
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

	// PHASE 2: flip scope_enabled to 1 ("scoped" mode). A completed
	// membership run whose completed_at is >= the investments' computed_at
	// (2026-01-06, the fixed value seedQualityRows uses), with BOTH work
	// units -- including wu-superseded -- present in work_unit_membership
	// under that run_id. The membership filter's own IN-clause would, on its
	// own, ADMIT wu-superseded here: this is exactly the case that would
	// silently resurrect it if the supersession exclusion were folded into
	// the scope gate's OR-condition instead of being unconditional.
	const membershipRunID = "run-current"
	const investmentsComputedAt = "2026-01-06 00:00:00"
	if err := conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at) VALUES ('%s', '%s', toDateTime64('%s', 3))",
		orgID, membershipRunID, investmentsComputedAt,
	)); err != nil {
		t.Fatalf("seed work_unit_membership_runs: %v", err)
	}
	for i, workUnitID := range []string{"wu-survives", "wu-superseded"} {
		insert := fmt.Sprintf(
			"INSERT INTO work_unit_membership (org_id, node_type, node_id, work_unit_id, category_kind, category, weight, is_dominant, categorization_status, computed_at, run_id) VALUES ('%s', 'issue', 'node-%d', '%s', 'theme', 'Feature Work', 1.0, 1, 'ok', toDateTime64('%s', 3), '%s')",
			orgID, i, workUnitID, investmentsComputedAt, membershipRunID,
		)
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed work_unit_membership: %v", err)
		}
	}

	// Control on the control: confirm scope_enabled genuinely flipped,
	// using the same telemetry helper the standing scope-gate contract
	// relies on -- a mistake in the membership seed (wrong run_id, an
	// earlier completed_at) would silently leave this phase testing
	// scope_enabled=0 again under a different name.
	scopeState, err := FetchInvestmentMembershipScopeState(ctx, client, orgID, 5)
	if err != nil {
		t.Fatalf("FetchInvestmentMembershipScopeState: %v", err)
	}
	if scopeState.ScopeMode != "scoped" {
		t.Fatalf("scope_mode = %q, want %q -- this phase proves nothing unless scope_enabled is actually 1", scopeState.ScopeMode, "scoped")
	}

	scoped, err := resolveEvidenceQualityStats(ctx, client, orgID, batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats (scope_enabled=1): %v", err)
	}
	if scoped == nil || scoped.Total != 1 {
		t.Fatalf("scope_enabled=1: Total = %+v, want 1 (wu-superseded stays excluded even though the membership filter alone would admit it)", scoped)
	}
	if scoped.Mean == nil || *scoped.Mean != 0.90 {
		t.Fatalf("scope_enabled=1: Mean = %v, want 0.90", scoped.Mean)
	}
}
