//go:build integration

package analytics

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	exactSumParts       = 16
	exactSumRowsPerPart = 5000
)

// seedExactSumPart writes one part of exactSumRowsPerPart work units, each
// carrying one subcategory. Its weights span eight orders of magnitude, so a
// Float64 sum of them depends on the order it is taken in. The weights are a
// function of logicalPart only: two subcategories seeded over the same set of
// logical parts carry the same multiset of weights, so their exact sums tie.
func seedExactSumPart(ctx context.Context, t *testing.T, conn stdclickhouse.Conn, orgID, subcategory string, physicalPart, logicalPart int) {
	t.Helper()
	insert := fmt.Sprintf(`
INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value,
    theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band,
    categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash,
    categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id)
SELECT
    concat('%[1]s-', toString(%[2]d), '-', toString(number)),
    toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-05 00:00:00', 3), NULL, NULL, 'fte_days', 1.0,
    map(), map('%[1]s', ((number * 7919 + %[3]d * 104729) %% 1000 + 1) / 3.0 * pow(10, toInt64((number + %[3]d) %% 9) - 4)),
    '', 1.0, 'high', 'ok', '', 'v1', 'hash', 'run', toDateTime64('2026-01-06 00:00:00', 3), 'story', 'unit', '%[4]s'
FROM numbers(%[5]d)`, subcategory, physicalPart, logicalPart, orgID, exactSumRowsPerPart)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed %s part %d: %v", subcategory, physicalPart, err)
	}
}

// The same flow-matrix request, run again and again on a real engine at its
// default thread count, returns one answer. The two themes carry the same
// multiset of weights in a different part order, so their exact values tie and
// the node_id / source+target tie-break alone orders them; a Float64 sum lets
// the engine's merge order move their last bits and reorder them.
func TestFlowMatrixInvestmentAnswerIsTheSameOnEveryRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
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

	const orgID = "seeded-flow-matrix-exact-sum"
	for part := 0; part < exactSumParts; part++ {
		seedExactSumPart(ctx, t, conn, orgID, "feature_delivery.build", part, part)
		seedExactSumPart(ctx, t, conn, orgID, "quality.testing", part, exactSumParts-1-part)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	nodesQuery, edgesQuery, err := CompileFlowMatrix(FlowMatrixRequest{
		Dimension: DimensionTheme, Measure: MeasureCount,
		StartDate: mustDate(t, "2026-01-01"), EndDate: mustDate(t, "2026-01-31"),
		MaxNodes: 50, MaxEdges: 200,
	}, orgID, 60, nil)
	if err != nil {
		t.Fatalf("CompileFlowMatrix error = %v", err)
	}

	const runs = 20
	var firstNodes []model.SankeyNode
	var firstEdges []model.SankeyEdge
	for run := 0; run < runs; run++ {
		nodes, edges, err := ExecuteFlowMatrix(ctx, client, nodesQuery, edgesQuery)
		if err != nil {
			t.Fatalf("run %d: ExecuteFlowMatrix error = %v", run, err)
		}
		if run == 0 {
			firstNodes, firstEdges = nodes, edges
			continue
		}
		if !reflect.DeepEqual(nodes, firstNodes) {
			t.Fatalf("run %d nodes differ from run 0:\n run 0: %s\n run %d: %s", run, flowNodesText(firstNodes), run, flowNodesText(nodes))
		}
		if !reflect.DeepEqual(edges, firstEdges) {
			t.Fatalf("run %d edges differ from run 0:\n run 0: %s\n run %d: %s", run, flowEdgesText(firstEdges), run, flowEdgesText(edges))
		}
	}

	if len(firstNodes) != 2 || firstNodes[0].ID != "THEME:feature_delivery" || firstNodes[1].ID != "THEME:quality" ||
		firstNodes[0].Value == nil || firstNodes[1].Value == nil || *firstNodes[0].Value != *firstNodes[1].Value {
		t.Fatalf("nodes = %s, want THEME:feature_delivery then THEME:quality with one equal value", flowNodesText(firstNodes))
	}
	if len(firstEdges) != 2 || firstEdges[0].Source != "THEME:feature_delivery" || firstEdges[1].Source != "THEME:quality" ||
		firstEdges[0].Value == nil || firstEdges[1].Value == nil || *firstEdges[0].Value != *firstEdges[1].Value {
		t.Fatalf("edges = %s, want the feature_delivery then the quality self-pair with one equal value", flowEdgesText(firstEdges))
	}
}

func flowNodesText(nodes []model.SankeyNode) string {
	out := ""
	for _, n := range nodes {
		out += fmt.Sprintf("%s=%s ", n.ID, flowValueText(n.Value))
	}
	return out
}

func flowEdgesText(edges []model.SankeyEdge) string {
	out := ""
	for _, e := range edges {
		out += fmt.Sprintf("%s>%s=%s ", e.Source, e.Target, flowValueText(e.Value))
	}
	return out
}

func flowValueText(v *float64) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%.17g", *v)
}
