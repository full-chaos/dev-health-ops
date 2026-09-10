//go:build integration

// CHAOS-5546 red-first proof against a REAL ClickHouse engine: a plain
// `UNION ALL` of CompileSankey's per-dimension branches has no guaranteed
// row order without an explicit outer ORDER BY. Confirmed independently
// (outside this test, against the live dev-health stack) by resolving the
// go-api-prove investmentFull request's exact bindings into literal SQL
// and running it four times in a row: three DIFFERENT branch orderings
// came back (REPO/THEME/TEAM, THEME/REPO/TEAM x2, TEAM/THEME/REPO) from
// the IDENTICAL query text. This test exercises the same class of query
// (a 3-dimension investment-path Sankey nodes UNION) against a fresh
// Testcontainers engine and asserts the fix holds: every repeated
// execution of the SAME compiled query returns nodes grouped by the
// dimension's position in the REQUESTED PATH, in the SAME order, every
// time -- not "some order", the requested one.
//
// Path chosen -- WORK_TYPE, SUBCATEGORY, THEME -- deliberately avoids
// TEAM/REPO/AUTHOR so the fixture needs only work_unit_investments (no
// team-vote or repos join tables): all three dimensions read straight off
// that table's own columns (dbColumn, validate.go:184-189).
package analytics

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

func seedSankeyOrderUnit(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID, workUnitID, workType, subcategory string, weight float64) {
	t.Helper()
	const fromTS = "2026-01-01 00:00:00"
	const toTS = "2026-01-05 00:00:00"
	const computedAt = "2026-01-06 00:00:00"
	insert := fmt.Sprintf(
		"INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id) VALUES "+
			"('%s', toDateTime64('%s',3), toDateTime64('%s',3), NULL, NULL, 'fte_days', 1.0, map(), map('%s', %g), '', 1.0, 'high', 'ok', '', 'v1', 'hash', 'run', toDateTime64('%s',3), '%s', 'unit', '%s')",
		workUnitID, fromTS, toTS, subcategory, weight, computedAt, workType, orgID,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed work_unit_investments (%s): %v", workUnitID, err)
	}
}

// TestCompileSankey_SeededRealClickHouse_NodeOrderIsPathOrderEveryRun is
// the durable, CI-enrolled regression test for CHAOS-5546. It runs the
// EXACT SAME compiled nodes query against a real engine several times in
// a row (no max_threads pin -- the whole point is to exercise the
// engine's normal parallel UNION ALL execution, the same shape that
// produced 3 different orderings live) and asserts every run comes back
// identically ordered: grouped by requested path position, then value
// DESC/node_id ASC within each group.
func TestCompileSankey_SeededRealClickHouse_NodeOrderIsPathOrderEveryRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
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

	const orgID = "seeded-sankey-order"
	// Three units, one per dimension value set, chosen so within-group
	// order (value DESC, node_id ASC) is unambiguous and none of the
	// three groups tie on cardinality or naming with each other.
	seedSankeyOrderUnit(t, ctx, conn, orgID, "wu-1", "feature", "quality.testing", 5)
	seedSankeyOrderUnit(t, ctx, conn, orgID, "wu-2", "bug", "feature_delivery.build", 2)
	seedSankeyOrderUnit(t, ctx, conn, orgID, "wu-3", "chore", "risk.security", 1)

	// max_threads left at its default (no pin) -- the fix must hold under
	// the engine's normal parallel execution plan, not just a
	// single-threaded one.
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	req := SankeyRequest{
		Path:      []Dimension{DimensionSubcategory, DimensionWorkType, DimensionTheme},
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		MaxNodes:  30,
		MaxEdges:  200,
	}
	nodesQuery, _, err := CompileSankey(req, orgID, 30, true, nil)
	if err != nil {
		t.Fatalf("CompileSankey error = %v", err)
	}

	// SUBCATEGORY, then WORK_TYPE, then THEME -- req.Path's own order,
	// not alphabetical (TEAM < THEME < WORK_TYPE and SUBCATEGORY sorts
	// between REPO and TEAM) and not value-sorted across groups (every
	// group here happens to share the same 5/2/1 value shape, so a
	// value-DESC-only bug would produce the SAME wrong-but-flat order
	// every time and this test alone would not catch it -- the SQL-level
	// unit test above pins that half of the claim explicitly).
	wantIDs := []string{
		"SUBCATEGORY:quality.testing", "SUBCATEGORY:feature_delivery.build", "SUBCATEGORY:risk.security",
		"WORK_TYPE:feature", "WORK_TYPE:bug", "WORK_TYPE:chore",
		"THEME:quality", "THEME:feature_delivery", "THEME:risk",
	}

	const runs = 8
	for i := 0; i < runs; i++ {
		nodes, _, err := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQuery}, nil)
		if err != nil {
			t.Fatalf("run %d: ExecuteSankeyQueries error = %v", i, err)
		}
		if len(nodes) != len(wantIDs) {
			t.Fatalf("run %d: got %d nodes, want %d: %+v", i, len(nodes), len(wantIDs), nodes)
		}
		for j, n := range nodes {
			if n.ID != wantIDs[j] {
				t.Fatalf("run %d: node[%d] = %q, want %q (full order: %v)", i, j, n.ID, wantIDs[j], nodeIDs(nodes))
			}
		}
	}
}

func nodeIDs(nodes []model.SankeyNode) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.ID
	}
	return out
}
