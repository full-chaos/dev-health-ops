//go:build integration

package workgraph

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-8108 on a real ClickHouse and the migrated schema. The unit tests pin
// which statement is sent; only an executed statement shows that ClickHouse
// takes it (the UNION of a LowCardinality select with a String select) and
// that it counts what the rows are.
//
// Seed, one org:
//   - work_graph_edges: five `blocks` edges between issues, one of them in two
//     versions, and one `implements` edge.
//   - work_item_dependencies: three `blocks` rows, one of them the same edge as
//     a work_graph_edges row (the work graph builder writes dependency edges to
//     both), and one `relates` row.
//
// So: 6 edges with no filter (5 + 1; the second version is not an edge), and
// 7 edges for the BLOCKS filter (5 primary + 3 dependency - 1 in both).
func TestResolveEdgesTotalCountIsTheMatchingEdgesOnARealStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	const orgID = "org-8108-total-count"
	const otherOrgID = "org-8108-other"

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

	base := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	type graphEdge struct {
		edgeID, source, target, edgeType, targetType, evidence string
		lastSynced                                             time.Time
	}
	graphEdges := []graphEdge{
		{"8108000000000000000000000000000000000000000000000000000000000001", "jira:I-1", "jira:I-2", "blocks", "issue", "v1", base},
		{"8108000000000000000000000000000000000000000000000000000000000001", "jira:I-1", "jira:I-2", "blocks", "issue", "v2", base.Add(time.Hour)},
		{"8108000000000000000000000000000000000000000000000000000000000002", "jira:I-3", "jira:I-4", "blocks", "issue", "v1", base},
		{"8108000000000000000000000000000000000000000000000000000000000003", "jira:I-5", "jira:I-6", "blocks", "issue", "v1", base},
		{"8108000000000000000000000000000000000000000000000000000000000004", "jira:I-7", "jira:I-8", "blocks", "issue", "v1", base},
		{"8108000000000000000000000000000000000000000000000000000000000005", "jira:I-9", "jira:I-10", "blocks", "issue", "v1", base},
		{"8108000000000000000000000000000000000000000000000000000000000006", "jira:I-1", "pr:owner/repo#1", "implements", "pr", "v1", base},
	}
	insertEdge := func(org string, e graphEdge) {
		t.Helper()
		if err := admin.Exec(ctx, `
            INSERT INTO work_graph_edges (
                edge_id, source_type, source_id, target_type, target_id, edge_type,
                repo_id, provider, provenance, confidence, evidence,
                discovered_at, last_synced, event_ts, org_id
            ) VALUES (?, 'issue', ?, ?, ?, ?, NULL, 'jira', 'native', ?, ?, ?, ?, ?, ?)
        `, e.edgeID, e.source, e.targetType, e.target, e.edgeType, float32(1.0), e.evidence,
			e.lastSynced, e.lastSynced, e.lastSynced, org); err != nil {
			t.Fatalf("insert edge %s (%s): %v", e.edgeID, e.evidence, err)
		}
	}
	for _, e := range graphEdges {
		insertEdge(orgID, e)
	}
	// Another org's edges must not be counted.
	insertEdge(otherOrgID, graphEdge{"8108000000000000000000000000000000000000000000000000000000000099", "jira:X-1", "jira:X-2", "blocks", "issue", "v1", base})

	type dependency struct{ source, target, relationship string }
	insertDependency := func(org string, d dependency) {
		t.Helper()
		if err := admin.Exec(ctx, `
            INSERT INTO work_item_dependencies (
                source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id
            ) VALUES (?, ?, ?, ?, ?, ?)
        `, d.source, d.target, d.relationship, d.relationship, base, org); err != nil {
			t.Fatalf("insert dependency %s > %s: %v", d.source, d.target, err)
		}
	}
	for _, d := range []dependency{
		{"jira:I-1", "jira:I-2", "blocks"}, // also a work_graph_edges row
		{"jira:I-11", "jira:I-12", "blocks"},
		{"jira:I-13", "jira:I-14", "blocks"},
		{"jira:I-15", "jira:I-16", "relates"},
	} {
		insertDependency(orgID, d)
	}
	insertDependency(otherOrgID, dependency{"jira:X-3", "jira:X-4", "blocks"})

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	blocks := model.WorkGraphEdgeTypeInputBlocks

	t.Run("no filter: the count is every edge, each version once", func(t *testing.T) {
		filters := &model.WorkGraphEdgeFilterInput{Limit: 2}
		result, err := ResolveEdges(ctx, client, orgID, filters)
		if err != nil {
			t.Fatalf("ResolveEdges: %v", err)
		}
		if len(result.Edges) != 2 {
			t.Fatalf("edges = %d, want the 2 of the page", len(result.Edges))
		}
		if result.TotalCount != 6 {
			t.Fatalf("totalCount = %d, want 6 (5 blocks + 1 implements; the second version of one edge is not an edge)", result.TotalCount)
		}
		if !result.PageInfo.HasNextPage {
			t.Fatal("hasNextPage = false with 6 matching edges and a page of 2")
		}
	})

	t.Run("no filter: a page of exactly every edge has no next page", func(t *testing.T) {
		result, err := ResolveEdges(ctx, client, orgID, &model.WorkGraphEdgeFilterInput{Limit: 6})
		if err != nil {
			t.Fatalf("ResolveEdges: %v", err)
		}
		if len(result.Edges) != 6 || result.TotalCount != 6 {
			t.Fatalf("edges = %d, totalCount = %d, want 6 and 6", len(result.Edges), result.TotalCount)
		}
		if result.PageInfo.HasNextPage {
			t.Fatal("hasNextPage = true when the page holds every matching edge")
		}
	})

	t.Run("BLOCKS filter: an edge of both tables is counted once", func(t *testing.T) {
		filters := &model.WorkGraphEdgeFilterInput{EdgeType: &blocks, Limit: 2}
		result, err := ResolveEdges(ctx, client, orgID, filters)
		if err != nil {
			t.Fatalf("ResolveEdges: %v", err)
		}
		if len(result.Edges) != 2 {
			t.Fatalf("edges = %d, want the 2 of the page", len(result.Edges))
		}
		if result.TotalCount != 7 {
			t.Fatalf("totalCount = %d, want 7 (5 graph edges + 3 dependency rows - 1 in both)", result.TotalCount)
		}

		// The counted number is the number of edges a caller gets with a limit
		// that cuts nothing: the count and the rows cannot disagree.
		all, err := ResolveEdges(ctx, client, orgID, &model.WorkGraphEdgeFilterInput{EdgeType: &blocks, Limit: 100})
		if err != nil {
			t.Fatalf("ResolveEdges (no cut): %v", err)
		}
		if len(all.Edges) != result.TotalCount {
			t.Fatalf("the uncut read returns %d edges and the count says %d", len(all.Edges), result.TotalCount)
		}
		if all.TotalCount != len(all.Edges) || all.PageInfo.HasNextPage {
			t.Fatalf("uncut read: totalCount = %d, hasNextPage = %v, want %d and false", all.TotalCount, all.PageInfo.HasNextPage, len(all.Edges))
		}

		counted, err := countMatchingEdges(ctx, client, orgID, filters, newFilterScope(filters, nil))
		if err != nil {
			t.Fatalf("countMatchingEdges: %v", err)
		}
		if counted != 7 {
			t.Fatalf("countMatchingEdges = %d, want 7", counted)
		}
	})
}
