//go:build integration

package workgraph

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// workGraphArtifacts picked its per-node evidence label with
// any(evidence), which ClickHouse documents as implementation-defined once
// a node has several live edges -- two readers (or one reader run twice
// across a part merge) can each pick a different edge's evidence for the
// SAME node, with identical degree and row order either side. This file
// pins the fix: a node's evidence is the evidence of its live edge with
// the greatest (last_synced, edge_id) -- argMax(evidence, (last_synced,
// edge_id)) -- and that pick is stable across repeated reads and across a
// forced part merge.
const (
	tieNodeMulti = "issue:ARTIFACT-TIEBREAK-MULTI"
	tieNodeExact = "issue:ARTIFACT-TIEBREAK-EXACT-TIE"
)

type tieEvidenceEdge struct {
	edgeID     string
	targetID   string
	evidence   string
	lastSynced time.Time
}

// TestResolveArtifactsEvidenceTieBreakIsDeterministic seeds two nodes:
//
//   - tieNodeMulti has four edges at three distinct last_synced values,
//     two of which (bbb-tie / ccc-tie) share the SAME last_synced. The
//     overall winner must be aaa-latest: its last_synced is strictly
//     greater than every other edge's, even though its edge_id sorts
//     BEFORE bbb-tie/ccc-tie lexicographically -- proving last_synced is
//     the primary key, not edge_id.
//   - tieNodeExact has two edges sharing the EXACT SAME last_synced and
//     nothing else to break the tie -- proving edge_id is the secondary,
//     deterministic key (tie-edge-2 > tie-edge-1).
//
// Both picks are re-asserted after a second, independent ResolveArtifacts
// call, and again after `OPTIMIZE TABLE work_graph_edges FINAL` forces a
// real part merge -- any() was observed to disagree exactly under that
// condition (across parts/merges); this test proves the fix does not.
func TestResolveArtifactsEvidenceTieBreakIsDeterministic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const orgID = "org-artifact-evidence-tiebreak"

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	t1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t2.Add(time.Hour)

	// tieNodeMulti: aaa-early (t1) < {bbb-tie, ccc-tie} (both t2, tied) <
	// aaa-latest (t3). Winner must be aaa-latest ("ev-latest") on
	// last_synced alone, despite its edge_id sorting before bbb-tie's.
	seedTieEvidenceEdges(t, ctx, admin, orgID, tieNodeMulti, []tieEvidenceEdge{
		{"aaa-early", "pr:owner/repo#501", "ev-early", t1},
		{"bbb-tie", "pr:owner/repo#502", "ev-tie-bbb", t2},
		{"ccc-tie", "pr:owner/repo#503", "ev-tie-ccc", t2},
		{"aaa-latest", "pr:owner/repo#504", "ev-latest", t3},
	})

	// tieNodeExact: both edges share t2 exactly -- only edge_id can break
	// the tie. tie-edge-2 > tie-edge-1 lexicographically, so it must win.
	seedTieEvidenceEdges(t, ctx, admin, orgID, tieNodeExact, []tieEvidenceEdge{
		{"tie-edge-1", "pr:owner/repo#601", "ev-from-1", t2},
		{"tie-edge-2", "pr:owner/repo#602", "ev-from-2", t2},
	})

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	filters := &model.WorkGraphEdgeFilterInput{Limit: 1000}

	assertTieBreakWinners := func(label string) {
		t.Helper()
		artifacts, err := ResolveArtifacts(ctx, client, orgID, filters)
		if err != nil {
			t.Fatalf("%s: ResolveArtifacts: %v", label, err)
		}
		got := map[string]string{}
		degree := map[string]int{}
		for _, row := range artifacts.Rows {
			if row.Evidence != nil {
				got[row.NodeID] = *row.Evidence
			}
			degree[row.NodeID] = row.Degree
		}
		if degree[tieNodeMulti] != 4 {
			t.Fatalf("%s: %s degree = %d, want 4", label, tieNodeMulti, degree[tieNodeMulti])
		}
		if got[tieNodeMulti] != "ev-latest" {
			t.Fatalf("%s: %s evidence = %q, want %q (the edge with the greatest last_synced, "+
				"regardless of edge_id ordering)", label, tieNodeMulti, got[tieNodeMulti], "ev-latest")
		}
		if degree[tieNodeExact] != 2 {
			t.Fatalf("%s: %s degree = %d, want 2", label, tieNodeExact, degree[tieNodeExact])
		}
		if got[tieNodeExact] != "ev-from-2" {
			t.Fatalf("%s: %s evidence = %q, want %q (edge_id breaks an exact last_synced tie: "+
				"tie-edge-2 > tie-edge-1)", label, tieNodeExact, got[tieNodeExact], "ev-from-2")
		}
	}

	// Two independent calls with nothing in between: the pick must not
	// vary run to run (any() offered no such guarantee).
	assertTieBreakWinners("first read")
	assertTieBreakWinners("second read, same parts")

	// Force a real merge of every active part, then read again: the fix
	// must be immune to physical merge order, which is exactly the axis
	// any() was observed to vary on.
	if err := admin.Exec(ctx, "OPTIMIZE TABLE work_graph_edges FINAL"); err != nil {
		t.Fatalf("OPTIMIZE TABLE work_graph_edges FINAL: %v", err)
	}
	assertTieBreakWinners("read after forced OPTIMIZE ... FINAL merge")
}

// TestResolveArtifactsEvidenceMatchesPythonReaderLiteral runs the EXACT
// SQL text resolve_work_graph_artifacts (work_graph.py) issues -- the
// same WHERE clause shape (org_id plus the liveEdgeClause tombstone
// filter, scope.go:303) and the same
// argMax(evidence, (last_synced, edge_id)) expression this fix applies in
// both readers -- directly against the admin connection, and asserts its
// per-node (degree, evidence) pairs match ResolveArtifacts's Go query
// exactly. Both queries are independently constructed (this file does not
// import or shell out to Python), so agreement here is a genuine
// cross-implementation parity proof of the shared tie-break rule, not a
// tautology against a shared helper.
func TestResolveArtifactsEvidenceMatchesPythonReaderLiteral(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const orgID = "org-artifact-evidence-python-parity"

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	t1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t2.Add(time.Hour)

	seedTieEvidenceEdges(t, ctx, admin, orgID, tieNodeMulti, []tieEvidenceEdge{
		{"aaa-early", "pr:owner/repo#501", "ev-early", t1},
		{"bbb-tie", "pr:owner/repo#502", "ev-tie-bbb", t2},
		{"ccc-tie", "pr:owner/repo#503", "ev-tie-ccc", t2},
		{"aaa-latest", "pr:owner/repo#504", "ev-latest", t3},
	})
	seedTieEvidenceEdges(t, ctx, admin, orgID, tieNodeExact, []tieEvidenceEdge{
		{"tie-edge-1", "pr:owner/repo#601", "ev-from-1", t2},
		{"tie-edge-2", "pr:owner/repo#602", "ev-from-2", t2},
	})
	// A tombstoned identity: the Python literal's liveEdgeClause must drop
	// it exactly like the Go reader does, so a divergence in tombstone
	// handling between the two SQL literals cannot hide behind this test.
	seedTieEvidenceEdges(t, ctx, admin, orgID, "issue:ARTIFACT-TIEBREAK-RETIRED", []tieEvidenceEdge{
		{"retired-edge", "pr:owner/repo#701", "ev-retired", t1},
	})
	if err := admin.Exec(ctx, `
        INSERT INTO work_graph_edges (
            edge_id, source_type, source_id, target_type, target_id, edge_type,
            provenance, confidence, evidence,
            discovered_at, last_synced, event_ts, org_id, is_deleted
        ) VALUES ('retired-edge', 'issue', 'issue:ARTIFACT-TIEBREAK-RETIRED', 'pr', 'pr:owner/repo#701',
                  'implements', 'native', 0.9, 'ev-retired-tombstone', ?, ?, ?, ?, 1)
    `, t2, t2, t2, orgID); err != nil {
		t.Fatalf("insert tombstone version: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	filters := &model.WorkGraphEdgeFilterInput{Limit: 1000}
	goResult, err := ResolveArtifacts(ctx, client, orgID, filters)
	if err != nil {
		t.Fatalf("ResolveArtifacts: %v", err)
	}
	goByNode := map[string]struct {
		degree   int
		evidence string
	}{}
	for _, row := range goResult.Rows {
		ev := ""
		if row.Evidence != nil {
			ev = *row.Evidence
		}
		goByNode[row.NodeID] = struct {
			degree   int
			evidence string
		}{row.Degree, ev}
	}

	pythonByNode := queryPythonArtifactsLiteral(t, ctx, admin, orgID)

	if len(pythonByNode) != len(goByNode) {
		t.Fatalf("python literal returned %d node(s), go reader returned %d: python=%v go=%v",
			len(pythonByNode), len(goByNode), pythonByNode, goByNode)
	}
	for nodeID, py := range pythonByNode {
		go_, ok := goByNode[nodeID]
		if !ok {
			t.Fatalf("node %s present in python literal result but not in go reader result", nodeID)
		}
		if go_.degree != py.degree || go_.evidence != py.evidence {
			t.Fatalf("node %s mismatch: go={degree:%d evidence:%q} python={degree:%d evidence:%q}",
				nodeID, go_.degree, go_.evidence, py.degree, py.evidence)
		}
	}
	if _, retired := pythonByNode["issue:ARTIFACT-TIEBREAK-RETIRED"]; retired {
		t.Fatalf("python literal surfaced the retired node; its only edge is tombstoned and must be hidden")
	}
	if _, retired := goByNode["issue:ARTIFACT-TIEBREAK-RETIRED"]; retired {
		t.Fatalf("go reader surfaced the retired node; its only edge is tombstoned and must be hidden")
	}
}

func seedTieEvidenceEdges(t *testing.T, ctx context.Context, admin stdclickhouse.Conn, orgID, nodeID string, edges []tieEvidenceEdge) {
	t.Helper()
	for _, e := range edges {
		if err := admin.Exec(ctx, `
            INSERT INTO work_graph_edges (
                edge_id, source_type, source_id, target_type, target_id, edge_type,
                provenance, confidence, evidence,
                discovered_at, last_synced, event_ts, org_id
            ) VALUES (?, 'issue', ?, 'pr', ?, 'implements', 'native', 0.9, ?, ?, ?, ?, ?)
        `, e.edgeID, nodeID, e.targetID, e.evidence, e.lastSynced, e.lastSynced, e.lastSynced, orgID); err != nil {
			t.Fatalf("insert edge %s (%s) for %s: %v", e.edgeID, e.evidence, nodeID, err)
		}
	}
}

// queryPythonArtifactsLiteral runs work_graph.py's resolve_work_graph_artifacts
// query text verbatim (the WHERE clause reduces to org_id plus the
// liveEdgeClause tombstone filter -- scope.go:299-309 -- since no
// theme/repo filter is active here; both readers render that same clause
// shape twice, once per UNION ALL branch).
func queryPythonArtifactsLiteral(t *testing.T, ctx context.Context, admin stdclickhouse.Conn, orgID string) map[string]struct {
	degree   int
	evidence string
} {
	t.Helper()
	where := fmt.Sprintf(`WHERE org_id = '%s' AND is_deleted = 0 AND (org_id, source_type, source_id, edge_type, target_type, target_id) NOT IN (
                SELECT org_id, source_type, source_id, edge_type, target_type, target_id
                FROM work_graph_edges
                WHERE org_id = '%s'
                GROUP BY org_id, source_type, source_id, edge_type, target_type, target_id
                HAVING argMax(is_deleted, last_synced) = 1
            )`, orgID, orgID)

	query := fmt.Sprintf(`
        SELECT node_type, node_id, uniqExact(edge_id) AS degree, argMax(evidence, (last_synced, edge_id)) AS evidence
        FROM (
            SELECT source_type AS node_type, source_id AS node_id, edge_id, evidence, last_synced
            FROM work_graph_edges
            %s
            UNION ALL
            SELECT target_type AS node_type, target_id AS node_id, edge_id, evidence, last_synced
            FROM work_graph_edges
            %s
        )
        GROUP BY node_type, node_id
        ORDER BY degree DESC, node_id ASC
        LIMIT 1000
    `, where, where)

	rows, err := admin.Query(ctx, query)
	if err != nil {
		t.Fatalf("python-literal artifacts query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := map[string]struct {
		degree   int
		evidence string
	}{}
	for rows.Next() {
		var nodeType, nodeID, evidence string
		var degree uint64
		if err := rows.Scan(&nodeType, &nodeID, &degree, &evidence); err != nil {
			t.Fatalf("python-literal artifacts scan: %v", err)
		}
		out[nodeID] = struct {
			degree   int
			evidence string
		}{int(degree), evidence}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("python-literal artifacts rows: %v", err)
	}
	return out
}
