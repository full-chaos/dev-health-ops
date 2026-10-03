package workgraph

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-8108: WorkGraphEdgesResult.totalCount is the number of edges the
// filters match BEFORE the limit. It was the length of the returned page
// (`TotalCount: len(edges)`), so a caller that subtracts the page from the
// total ("N more edges") always got 0.

// countingClient answers the statements of ResolveEdges by what they are, not
// by their position: the row statement of the primary edges, the row statement
// of the dependency edges, the count statement, and (for everything else: the
// membership read) an empty result.
type countingClient struct {
	primary    [][]any
	dependency [][]any
	// count is the scripted answer of the count statement; countErr fails it.
	count    uint64
	countErr error

	statements []string
	bindings   [][]clickhouse.Binding
}

func (c *countingClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	switch {
	case strings.HasPrefix(statement, "SELECT count() FROM ("):
		if c.countErr != nil {
			return nil, c.countErr
		}
		return &fakeRowScanner{rows: [][]any{{c.count}}}, nil
	case strings.Contains(statement, "FROM work_item_dependencies"):
		return &fakeRowScanner{rows: c.dependency}, nil
	case strings.Contains(statement, "FROM work_graph_edges") && strings.Contains(statement, "ORDER BY confidence DESC, edge_id ASC"):
		return &fakeRowScanner{rows: c.primary}, nil
	}
	return &fakeRowScanner{}, nil
}

// countStatements returns the count statements the client received, with
// their bindings by name.
func (c *countingClient) countStatements() (statements []string, bindings []map[string]any) {
	for i, statement := range c.statements {
		if !strings.HasPrefix(statement, "SELECT count() FROM (") {
			continue
		}
		byName := map[string]any{}
		for _, b := range c.bindings[i] {
			byName[b.Name] = b.Value
		}
		statements = append(statements, statement)
		bindings = append(bindings, byName)
	}
	return statements, bindings
}

// issueEdge is one scripted row of either row statement: `source` blocks
// `target`, two issues. The ids are not of a form the display-name reads take,
// so no name read runs.
func issueEdge(edgeID, source, target string) []any {
	return []any{edgeID, "issue", source, "issue", target, "blocks", "", "", "native", 1.0, ""}
}

func TestResolveEdges_TotalCountIsTheMatchingEdgesNotThePage(t *testing.T) {
	client := &countingClient{
		primary: [][]any{issueEdge("e1", "A", "B"), issueEdge("e2", "C", "D")},
		count:   5,
	}
	filters := &model.WorkGraphEdgeFilterInput{Limit: 2}

	result, err := ResolveEdges(context.Background(), client, "org1", filters)
	if err != nil {
		t.Fatalf("ResolveEdges: %v", err)
	}
	if len(result.Edges) != 2 {
		t.Fatalf("edges = %d, want the 2 of the page", len(result.Edges))
	}
	if result.TotalCount != 5 {
		t.Fatalf("totalCount = %d, want 5: the edges that match before the limit, not the %d of the page", result.TotalCount, len(result.Edges))
	}
	if !result.PageInfo.HasNextPage {
		t.Fatal("hasNextPage = false with 5 matching edges and a page of 2")
	}

	// The count reads the SAME select the rows come from, with no order, no
	// limit and no limit binding. A count built from another text could drift
	// from the rows it counts.
	scope := newFilterScope(filters, nil)
	selectSQL, _ := dedupedEdgesSelect("org1", scope)
	statements, bindings := client.countStatements()
	if len(statements) != 1 {
		t.Fatalf("count statements = %d, want 1", len(statements))
	}
	if !strings.Contains(statements[0], selectSQL) {
		t.Fatalf("the count statement does not hold the select of the row statement:\n%s", statements[0])
	}
	if strings.Contains(statements[0], "LIMIT") || strings.Contains(statements[0], "ORDER BY confidence") {
		t.Fatalf("the count statement carries the order or the limit of the row statement:\n%s", statements[0])
	}
	if _, ok := bindings[0]["limit"]; ok {
		t.Fatal("the count statement was sent the limit binding")
	}
	if client.statements[0] != selectSQL+dedupedEdgesOrderAndLimit {
		t.Fatalf("the row statement is not the select plus its order and limit:\n%s", client.statements[0])
	}
}

// A page that is exactly as long as the limit, with nothing more behind it.
// The old rule (`hasNextPage = len(edges) == limit`) said "more" here.
func TestResolveEdges_AFullPageWithNothingMoreHasNoNextPage(t *testing.T) {
	client := &countingClient{
		primary: [][]any{issueEdge("e1", "A", "B"), issueEdge("e2", "C", "D")},
		count:   2,
	}
	result, err := ResolveEdges(context.Background(), client, "org1", &model.WorkGraphEdgeFilterInput{Limit: 2})
	if err != nil {
		t.Fatalf("ResolveEdges: %v", err)
	}
	if result.TotalCount != 2 {
		t.Fatalf("totalCount = %d, want 2", result.TotalCount)
	}
	if result.PageInfo.HasNextPage {
		t.Fatal("hasNextPage = true with 2 matching edges and a page of 2")
	}
}

// A page shorter than the limit is the full set: its length is the exact
// count, and no count statement is sent.
func TestResolveEdges_APageBelowTheLimitIsCountedWithoutARead(t *testing.T) {
	client := &countingClient{
		primary: [][]any{issueEdge("e1", "A", "B"), issueEdge("e2", "C", "D")},
		count:   99, // must not be read
	}
	result, err := ResolveEdges(context.Background(), client, "org1", &model.WorkGraphEdgeFilterInput{Limit: 5})
	if err != nil {
		t.Fatalf("ResolveEdges: %v", err)
	}
	if statements, _ := client.countStatements(); len(statements) != 0 {
		t.Fatalf("a count statement was sent for a page below the limit:\n%s", statements[0])
	}
	if result.TotalCount != 2 {
		t.Fatalf("totalCount = %d, want 2", result.TotalCount)
	}
	if result.PageInfo.HasNextPage {
		t.Fatal("hasNextPage = true for a page below the limit")
	}
}

// The count and the rows are two reads of a table that takes writes. The
// count is never reported below the number of edges returned.
func TestResolveEdges_TheCountNeverGoesBelowThePage(t *testing.T) {
	client := &countingClient{
		primary: [][]any{issueEdge("e1", "A", "B"), issueEdge("e2", "C", "D")},
		count:   1,
	}
	result, err := ResolveEdges(context.Background(), client, "org1", &model.WorkGraphEdgeFilterInput{Limit: 2})
	if err != nil {
		t.Fatalf("ResolveEdges: %v", err)
	}
	if result.TotalCount != 2 {
		t.Fatalf("totalCount = %d, want 2 (never below the page)", result.TotalCount)
	}
	if result.PageInfo.HasNextPage {
		t.Fatal("hasNextPage = true with a count that does not exceed the page")
	}
}

// A count that was not measured is not replaced by the page length: the
// request fails.
func TestResolveEdges_AFailedCountFailsTheRequest(t *testing.T) {
	boom := errors.New("clickhouse: count failed")
	client := &countingClient{
		primary:  [][]any{issueEdge("e1", "A", "B"), issueEdge("e2", "C", "D")},
		countErr: boom,
	}
	result, err := ResolveEdges(context.Background(), client, "org1", &model.WorkGraphEdgeFilterInput{Limit: 2})
	if err == nil {
		t.Fatalf("ResolveEdges answered totalCount = %d and no error after a failed count", result.TotalCount)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("ResolveEdges error = %v, want it to wrap the count failure", err)
	}
	if result != nil {
		t.Fatalf("ResolveEdges returned a result beside the error: %+v", result)
	}
}

// With an edge-type filter the dependency edges are spliced into the page. A
// dependency edge is often ALSO a work_graph_edges row, so the count is the
// UNION of the two selects by the splice identity, in one statement: adding
// two counts would count the overlap twice.
func TestResolveEdges_TheCountWithDependencyEdgesIsTheUnionOfBothSelects(t *testing.T) {
	client := &countingClient{
		primary: [][]any{issueEdge("e1", "A", "B")},
		// A full dependency page: more may be behind it, so the count is read.
		dependency: [][]any{issueEdge("wid:1", "A", "B"), issueEdge("wid:2", "C", "D")},
		count:      7,
	}
	filters := &model.WorkGraphEdgeFilterInput{
		EdgeTypes: []model.WorkGraphEdgeTypeInput{model.WorkGraphEdgeTypeInputBlocks, model.WorkGraphEdgeTypeInputImplements},
		Limit:     2,
	}

	result, err := ResolveEdges(context.Background(), client, "org1", filters)
	if err != nil {
		t.Fatalf("ResolveEdges: %v", err)
	}
	if len(result.Edges) != 2 {
		t.Fatalf("edges = %d, want 2 (A>B once, then C>D)", len(result.Edges))
	}
	if result.TotalCount != 7 {
		t.Fatalf("totalCount = %d, want the counted 7", result.TotalCount)
	}

	scope := newFilterScope(filters, nil)
	primarySelect, _ := dedupedEdgesSelect("org1", scope)
	dependencySelect, _, active := dependencyEdgesSelect("org1", filters, scope, dependencyCountEdgeTypesParam)
	if !active {
		t.Fatal("premise broken: the dependency select is not active for a BLOCKS filter")
	}
	statements, bindings := client.countStatements()
	if len(statements) != 1 {
		t.Fatalf("count statements = %d, want 1", len(statements))
	}
	statement := statements[0]
	if !strings.Contains(statement, primarySelect) || !strings.Contains(statement, dependencySelect) {
		t.Fatalf("the count statement does not hold both selects:\n%s", statement)
	}
	if !strings.Contains(statement, "UNION DISTINCT") {
		t.Fatalf("the count statement does not remove the overlap of the two selects:\n%s", statement)
	}
	if strings.Count(statement, edgeIdentityColumns) != 2 {
		t.Fatalf("the count statement does not compare the two selects by the splice identity:\n%s", statement)
	}

	// The two selects have an edge-type binding each, with DIFFERENT values:
	// every selected type on the primary side, only the dependency-eligible
	// ones on the dependency side. One name for both would count another
	// filter than the rows had.
	if got := bindings[0]["edge_types"]; !reflect.DeepEqual(got, []string{"blocks", "implements"}) {
		t.Fatalf("edge_types = %v, want every selected edge type", got)
	}
	if got := bindings[0][dependencyCountEdgeTypesParam]; !reflect.DeepEqual(got, []string{"blocks"}) {
		t.Fatalf("%s = %v, want the dependency-eligible edge types only", dependencyCountEdgeTypesParam, got)
	}
	if !strings.Contains(dependencySelect, "{"+dependencyCountEdgeTypesParam+":Array(String)}") {
		t.Fatalf("the dependency half of the count statement does not use its own edge-type binding:\n%s", dependencySelect)
	}
	if _, ok := bindings[0]["limit"]; ok {
		t.Fatal("the count statement was sent the limit binding")
	}
}

// Both reads below the limit and a splice that fits: the page is the full set
// and the overlap is counted once, with no count statement.
func TestResolveEdges_ACompleteSpliceIsCountedWithoutARead(t *testing.T) {
	client := &countingClient{
		primary:    [][]any{issueEdge("e1", "A", "B")},
		dependency: [][]any{issueEdge("wid:1", "A", "B"), issueEdge("wid:2", "C", "D")},
		count:      99, // must not be read
	}
	blocks := model.WorkGraphEdgeTypeInputBlocks
	result, err := ResolveEdges(context.Background(), client, "org1", &model.WorkGraphEdgeFilterInput{EdgeType: &blocks, Limit: 10})
	if err != nil {
		t.Fatalf("ResolveEdges: %v", err)
	}
	if statements, _ := client.countStatements(); len(statements) != 0 {
		t.Fatalf("a count statement was sent for a complete splice:\n%s", statements[0])
	}
	if result.TotalCount != 2 {
		t.Fatalf("totalCount = %d, want 2: A>B is one edge, in both reads", result.TotalCount)
	}
}

func TestSplicedEdgeCount_CountsAnEdgeOfBothReadsOnce(t *testing.T) {
	primary := []edgeRow{{edgeID: "p", sourceType: "issue", sourceID: "A", edgeType: "blocks", targetType: "issue", targetID: "B"}}
	dependency := []edgeRow{
		{edgeID: "wid:1", sourceType: "issue", sourceID: "A", edgeType: "blocks", targetType: "issue", targetID: "B"},
		{edgeID: "wid:2", sourceType: "issue", sourceID: "C", edgeType: "blocks", targetType: "issue", targetID: "D"},
		{edgeID: "wid:3", sourceType: "issue", sourceID: "E", edgeType: "blocks", targetType: "issue", targetID: "F"},
	}
	if got := splicedEdgeCount(primary, dependency); got != 3 {
		t.Fatalf("splicedEdgeCount = %d, want 3", got)
	}
	// It is what the splice returns when the limit cuts nothing.
	if got := len(spliceDependencyEdges(primary, dependency, 100)); got != 3 {
		t.Fatalf("spliceDependencyEdges with no cut = %d rows, want 3", got)
	}
}

func TestMergeCountBindings(t *testing.T) {
	t.Run("one name with one value is sent once", func(t *testing.T) {
		got, err := mergeCountBindings(
			[]clickhouse.Binding{{Name: "org_id", Value: "org1"}, {Name: "edge_types", Value: []string{"blocks", "implements"}}},
			[]clickhouse.Binding{{Name: "org_id", Value: "org1"}, {Name: "dependency_edge_types", Value: []string{"blocks"}}},
		)
		if err != nil {
			t.Fatalf("mergeCountBindings: %v", err)
		}
		var names []string
		for _, b := range got {
			names = append(names, b.Name)
		}
		if !reflect.DeepEqual(names, []string{"org_id", "edge_types", "dependency_edge_types"}) {
			t.Fatalf("names = %v", names)
		}
	})
	t.Run("one name with two values is an error", func(t *testing.T) {
		_, err := mergeCountBindings(
			[]clickhouse.Binding{{Name: "edge_types", Value: []string{"blocks", "implements"}}},
			[]clickhouse.Binding{{Name: "edge_types", Value: []string{"blocks"}}},
		)
		if err == nil {
			t.Fatal("two values for one binding name were merged")
		}
	})
}
