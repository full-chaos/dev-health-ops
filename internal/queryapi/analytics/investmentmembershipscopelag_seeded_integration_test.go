//go:build integration

package analytics

// Seeded real-engine proof of the membership scope gate's lag contract:
// while the membership projection trails the newest investment rows, reads
// stay scoped to the latest COMPLETE membership run; the gate reads
// unscoped only when the organisation has no complete run at all.

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type scopeLagHarness struct {
	conn   stdclickhouse.Conn
	client *dhclickhouse.Client
	batch  model.AnalyticsRequestInput
}

func startScopeLagHarness(t *testing.T, ctx context.Context) *scopeLagHarness {
	t.Helper()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return &scopeLagHarness{
		conn:   conn,
		client: client,
		batch: model.AnalyticsRequestInput{
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
		},
	}
}

// seedInvestmentAt writes one work_unit_investments row with an explicit
// computed_at, so a test can place an investment before or after a
// membership marker.
func (h *scopeLagHarness) seedInvestmentAt(t *testing.T, ctx context.Context, orgID, workUnitID, computedAt string) {
	t.Helper()
	insert := fmt.Sprintf(
		"INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id) VALUES ('%s', toDateTime64('2026-01-01 00:00:00',3), toDateTime64('2026-01-05 00:00:00',3), NULL, NULL, 'fte_days', 1.0, map(), map(), '', 0.5, 'moderate', 'ok', '', 'v1', 'hash', 'run', toDateTime64('%s',3), NULL, NULL, '%s')",
		workUnitID, computedAt, orgID,
	)
	if err := h.conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed work_unit_investments: %v", err)
	}
}

// seedMembershipRun publishes a complete membership run: its membership
// rows first, then the marker, the same order the projection writes them.
// A runID of "__legacy__" writes its rows with run_id = ” (the legacy
// row shape the reader's legacy predicate expects).
func (h *scopeLagHarness) seedMembershipRun(t *testing.T, ctx context.Context, orgID, runID, completedAt string, workUnitIDs ...string) {
	t.Helper()
	rowRunID := runID
	if runID == legacyRunID {
		rowRunID = ""
	}
	for i, workUnitID := range workUnitIDs {
		insert := fmt.Sprintf(
			"INSERT INTO work_unit_membership (org_id, node_type, node_id, work_unit_id, category_kind, category, weight, is_dominant, categorization_status, computed_at, run_id) VALUES ('%s', 'issue', 'node-%s-%d', '%s', 'theme', 'Feature Work', 1.0, 1, 'ok', toDateTime64('%s', 3), '%s')",
			orgID, runID, i, workUnitID, completedAt, rowRunID,
		)
		if err := h.conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed work_unit_membership: %v", err)
		}
	}
	if err := h.conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at) VALUES ('%s', '%s', toDateTime64('%s', 3))",
		orgID, runID, completedAt,
	)); err != nil {
		t.Fatalf("seed work_unit_membership_runs: %v", err)
	}
}

func (h *scopeLagHarness) total(t *testing.T, ctx context.Context, client QueryClient, orgID string) int {
	t.Helper()
	stats, err := resolveEvidenceQualityStats(ctx, client, orgID, h.batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats: %v", err)
	}
	if stats == nil {
		return 0
	}
	return stats.Total
}

// TestInvestmentScope_ProjectionLagStaysScopedToLatestCompleteRun seeds the
// state a materialize cycle passes through for a few seconds: a work unit
// written after the latest complete membership marker, not yet in any run.
// The read must stay on that marker's work units.
func TestInvestmentScope_ProjectionLagStaysScopedToLatestCompleteRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	h := startScopeLagHarness(t, ctx)

	const orgID = "scope-lag-org"
	h.seedInvestmentAt(t, ctx, orgID, "wu-projected", "2026-01-06 00:00:00")
	h.seedInvestmentAt(t, ctx, orgID, "wu-stale-generation", "2026-01-06 00:00:00")
	h.seedMembershipRun(t, ctx, orgID, "run-current", "2026-01-06 00:00:05", "wu-projected")

	// Control: with the marker newest, the read is scoped (1 of 2 units).
	if got := h.total(t, ctx, h.client, orgID); got != 1 {
		t.Fatalf("control (marker newest): Total = %d, want 1", got)
	}

	// The materializer lands a newer investment row before the next marker.
	h.seedInvestmentAt(t, ctx, orgID, "wu-fresh", "2026-01-06 00:00:09")

	state, err := FetchInvestmentMembershipScopeState(ctx, h.client, orgID, 5)
	if err != nil {
		t.Fatalf("FetchInvestmentMembershipScopeState: %v", err)
	}
	if state.ScopeMode != "scoped_projection_lag" || state.LagSeconds != 4 || state.RunID != "run-current" {
		t.Fatalf("state = %+v, want {scoped_projection_lag 4 run-current}", state)
	}
	if got := h.total(t, ctx, h.client, orgID); got != 1 {
		t.Fatalf("projection lag: Total = %d, want 1 (only run-current's work unit; wu-stale-generation and wu-fresh stay out until a marker includes them)", got)
	}
	if got := h.total(t, ctx, PinInvestmentMembershipScope(h.client), orgID); got != 1 {
		t.Fatalf("projection lag, pinned client: Total = %d, want 1", got)
	}

	// The next marker lands and includes the fresh unit.
	h.seedMembershipRun(t, ctx, orgID, "run-next", "2026-01-06 00:00:12", "wu-projected", "wu-fresh")
	if got := h.total(t, ctx, h.client, orgID); got != 2 {
		t.Fatalf("after next marker: Total = %d, want 2", got)
	}
}

// TestInvestmentScope_NoCompleteRunReadsUnscoped pins the one remaining
// unscoped case: an organisation without any complete membership run.
func TestInvestmentScope_NoCompleteRunReadsUnscoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	h := startScopeLagHarness(t, ctx)

	const orgID = "scope-no-marker-org"
	h.seedInvestmentAt(t, ctx, orgID, "wu-a", "2026-01-06 00:00:00")
	h.seedInvestmentAt(t, ctx, orgID, "wu-b", "2026-01-06 00:00:09")
	// A run whose rows landed but whose marker did not is incomplete and
	// must not scope the read.
	if err := h.conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO work_unit_membership (org_id, node_type, node_id, work_unit_id, category_kind, category, weight, is_dominant, categorization_status, computed_at, run_id) VALUES ('%s', 'issue', 'node-x', 'wu-a', 'theme', 'Feature Work', 1.0, 1, 'ok', toDateTime64('2026-01-06 00:00:01', 3), 'run-in-flight')",
		orgID,
	)); err != nil {
		t.Fatalf("seed in-flight membership row: %v", err)
	}

	state, err := FetchInvestmentMembershipScopeState(ctx, h.client, orgID, 5)
	if err != nil {
		t.Fatalf("FetchInvestmentMembershipScopeState: %v", err)
	}
	if state.ScopeMode != "unscoped_no_marker" || state.RunID != "" {
		t.Fatalf("state = %+v, want unscoped_no_marker with no run", state)
	}
	for name, client := range map[string]QueryClient{"raw": h.client, "pinned": PinInvestmentMembershipScope(h.client)} {
		if got := h.total(t, ctx, client, orgID); got != 2 {
			t.Fatalf("%s client, no complete run: Total = %d, want 2 (unscoped)", name, got)
		}
	}
}

// TestInvestmentScope_LegacyMarkerScopesToLegacyRows pins the legacy
// marker path through the pinned client: the "__legacy__" run scopes to
// run_id = ” rows at their newest computed_at per node.
func TestInvestmentScope_LegacyMarkerScopesToLegacyRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	h := startScopeLagHarness(t, ctx)

	const orgID = "scope-legacy-org"
	h.seedInvestmentAt(t, ctx, orgID, "wu-legacy", "2026-01-06 00:00:00")
	h.seedInvestmentAt(t, ctx, orgID, "wu-outside", "2026-01-06 00:00:09")
	h.seedMembershipRun(t, ctx, orgID, legacyRunID, "2026-01-06 00:00:05", "wu-legacy")

	for name, client := range map[string]QueryClient{"raw": h.client, "pinned": PinInvestmentMembershipScope(h.client)} {
		if got := h.total(t, ctx, client, orgID); got != 1 {
			t.Fatalf("%s client, legacy marker: Total = %d, want 1", name, got)
		}
	}
}

// TestInvestmentScope_OneResolutionPerRequest pins the request contract: a
// marker that lands between two queries of one request does not change the
// second query's scope. A new request sees the new marker.
func TestInvestmentScope_OneResolutionPerRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	h := startScopeLagHarness(t, ctx)

	const orgID = "scope-request-org"
	h.seedInvestmentAt(t, ctx, orgID, "wu-a", "2026-01-06 00:00:00")
	h.seedInvestmentAt(t, ctx, orgID, "wu-b", "2026-01-06 00:00:00")
	h.seedMembershipRun(t, ctx, orgID, "run-a", "2026-01-06 00:00:05", "wu-a")

	pinned := PinInvestmentMembershipScope(h.client)
	requestCtx := WithInvestmentMembershipScopeRequest(ctx)
	if got := h.total(t, requestCtx, pinned, orgID); got != 1 {
		t.Fatalf("first query of the request: Total = %d, want 1", got)
	}

	h.seedMembershipRun(t, ctx, orgID, "run-b", "2026-01-06 00:00:10", "wu-a", "wu-b")

	if got := h.total(t, requestCtx, pinned, orgID); got != 1 {
		t.Fatalf("second query of the same request: Total = %d, want 1 (still run-a's scope)", got)
	}
	if got := h.total(t, WithInvestmentMembershipScopeRequest(ctx), pinned, orgID); got != 2 {
		t.Fatalf("next request: Total = %d, want 2 (run-b's scope)", got)
	}
}
