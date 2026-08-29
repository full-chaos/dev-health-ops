package workgraph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// --- fakes, same shape as hotspots_test.go / reviewedges_test.go ----------

type fakeRowScanner struct {
	rows   [][]any
	cursor int
	err    error
}

func (f *fakeRowScanner) Next() bool {
	if f.err != nil {
		return false
	}
	return f.cursor < len(f.rows)
}

func (f *fakeRowScanner) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return errors.New("workgraph test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *uint64:
			*ptr = row[i].(uint64)
		case *uint32:
			*ptr = row[i].(uint32)
		case *float64:
			*ptr = row[i].(float64)
		default:
			return errors.New("workgraph test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

// fakeClient dispatches responses by call ORDER (same convention as
// hotspots_test.go's fakeClient) -- responses[0] answers the first Query
// call, responses[1] the second, etc. errs[i], when non-nil, makes call i
// return that error instead of consuming a response.
type fakeClient struct {
	responses  []*fakeRowScanner
	errs       []error
	calls      int
	statements []string
	bindings   [][]clickhouse.Binding
}

func (f *fakeClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	i := f.calls
	f.calls++
	f.statements = append(f.statements, statement)
	f.bindings = append(f.bindings, bindings)
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	return f.responses[i], nil
}

func bindingValue(bindings []clickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

func strPtr(s string) *string { return &s }

// --- displayNameFor (work_graph.py:231-258) --------------------------------

func TestDisplayNameFor(t *testing.T) {
	resolved := map[string]string{
		"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa#pr7": "Fix the thing (#7)",
	}
	cases := []struct {
		name string
		id   string
		want *string
	}{
		{"empty", "", nil},
		{"lookup wins over pass-through", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa#pr7", strPtr("Fix the thing (#7)")},
		{"unresolved PR-format id -> nil (A8)", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb#pr9", nil},
		{"bare UUID -> nil (A8)", "cccccccc-cccc-cccc-cccc-cccccccccccc", nil},
		{"opaque hex -> nil (A8)", "0123456789abcdef01234567", nil},
		{"human-readable passthrough", "PROJ-123", strPtr("PROJ-123")},
		{"whitespace trimmed", "  PROJ-123  ", strPtr("PROJ-123")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := displayNameFor(tc.id, resolved)
			if (got == nil) != (tc.want == nil) {
				t.Fatalf("displayNameFor(%q) = %v, want %v", tc.id, got, tc.want)
			}
			if got != nil && *got != *tc.want {
				t.Fatalf("displayNameFor(%q) = %q, want %q", tc.id, *got, *tc.want)
			}
		})
	}
}

// --- subcategoryParentTheme / themeSubcategoryConflict ---------------------

func TestSubcategoryParentTheme(t *testing.T) {
	if got := subcategoryParentTheme("delivery.feature_work"); got == nil || *got != "delivery" {
		t.Fatalf("subcategoryParentTheme = %v, want delivery", got)
	}
	if got := subcategoryParentTheme(""); got != nil {
		t.Fatalf("subcategoryParentTheme(\"\") = %v, want nil", got)
	}
	// No dot at all: the whole string is the "prefix" (matches Python's
	// split(".", 1)[0] on a no-dot string, which returns the string itself).
	if got := subcategoryParentTheme("delivery"); got == nil || *got != "delivery" {
		t.Fatalf("subcategoryParentTheme(no dot) = %v, want delivery", got)
	}
}

func TestThemeSubcategoryConflict(t *testing.T) {
	if themeSubcategoryConflict(nil, nil) {
		t.Fatal("nil, nil must not conflict")
	}
	if themeSubcategoryConflict(strPtr("delivery"), nil) {
		t.Fatal("theme-only must not conflict")
	}
	if !themeSubcategoryConflict(strPtr("risk"), strPtr("delivery.feature_work")) {
		t.Fatal("cross-theme pair must conflict")
	}
	if themeSubcategoryConflict(strPtr("delivery"), strPtr("delivery.feature_work")) {
		t.Fatal("matching theme/subcategory must not conflict")
	}
}

// --- dependencyEdgeFilterValues (work_graph.py:213-228) --------------------

func TestDependencyEdgeFilterValues(t *testing.T) {
	blocks := model.WorkGraphEdgeTypeInputBlocks
	implements := model.WorkGraphEdgeTypeInputImplements // NOT in dependencyEdgeTypes

	t.Run("nil filters", func(t *testing.T) {
		if got := dependencyEdgeFilterValues(nil); got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})
	t.Run("neither set", func(t *testing.T) {
		if got := dependencyEdgeFilterValues(&model.WorkGraphEdgeFilterInput{}); got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})
	t.Run("singular only, dependency-eligible", func(t *testing.T) {
		got := dependencyEdgeFilterValues(&model.WorkGraphEdgeFilterInput{EdgeType: &blocks})
		if len(got) != 1 || got[0] != "blocks" {
			t.Fatalf("got %v, want [blocks]", got)
		}
	})
	t.Run("singular only, NOT dependency-eligible -> empty", func(t *testing.T) {
		got := dependencyEdgeFilterValues(&model.WorkGraphEdgeFilterInput{EdgeType: &implements})
		if len(got) != 0 {
			t.Fatalf("got %v, want empty (implements is not a dependency edge type)", got)
		}
	})
	t.Run("plural only", func(t *testing.T) {
		got := dependencyEdgeFilterValues(&model.WorkGraphEdgeFilterInput{
			EdgeTypes: []model.WorkGraphEdgeTypeInput{blocks, implements},
		})
		if len(got) != 1 || got[0] != "blocks" {
			t.Fatalf("got %v, want [blocks] (implements filtered out)", got)
		}
	})
	t.Run("both singular and plural intersect", func(t *testing.T) {
		isBlockedBy := model.WorkGraphEdgeTypeInputIsBlockedBy
		got := dependencyEdgeFilterValues(&model.WorkGraphEdgeFilterInput{
			EdgeType:  &blocks,
			EdgeTypes: []model.WorkGraphEdgeTypeInput{isBlockedBy},
		})
		if len(got) != 0 {
			t.Fatalf("got %v, want empty (blocks not in edgeTypes=[is_blocked_by])", got)
		}
	})
}

// --- dependencyEdgeTypeSQL: self-verifying against the source map ---------

func TestDependencyEdgeTypeSQL_CoversEveryMapEntry(t *testing.T) {
	sql := dependencyEdgeTypeSQL()
	if !strings.HasPrefix(sql, "multiIf(") || !strings.HasSuffix(sql, ", 'relates')") {
		t.Fatalf("unexpected shape: %s", sql)
	}
	for k, v := range dependencyRelationshipTypeMap {
		want := "relationship_type = '" + k + "', '" + v + "'"
		if !strings.Contains(sql, want) {
			t.Fatalf("dependencyEdgeTypeSQL missing case for %q: %s", k, want)
		}
	}
	// Regression guard for the easy-to-miss 15th entry.
	if !strings.Contains(sql, "relationship_type = 'is_parent_of', 'parent_of'") {
		t.Fatal("missing is_parent_of -> parent_of translation")
	}
}

// --- clampEdgesLimit ---------------------------------------------------

func TestClampEdgesLimit(t *testing.T) {
	if got := clampEdgesLimit(0); got != 1 {
		t.Fatalf("clampEdgesLimit(0) = %d, want 1", got)
	}
	if got := clampEdgesLimit(-5); got != 1 {
		t.Fatalf("clampEdgesLimit(-5) = %d, want 1", got)
	}
	if got := clampEdgesLimit(50); got != 50 {
		t.Fatalf("clampEdgesLimit(50) = %d, want 50", got)
	}
	if got := clampEdgesLimit(MaxEdgesLimit + 1); got != MaxEdgesLimit {
		t.Fatalf("clampEdgesLimit(over) = %d, want %d", got, MaxEdgesLimit)
	}
}

// --- isMissingMembershipTableError (work_graph.py:854-877) -----------------

// operationErrorDouble mirrors dev-health-go/clickhouse's UNEXPORTED
// operationError shape exactly for test purposes: Error() returns a
// FIXED generic string, unrelated to cause's real text, and Unwrap()
// exposes cause -- so a caller that reads only Error() sees nothing
// useful, and only errors.As (walking Unwrap at every level) reaches the
// real *chproto.Exception. operationError itself cannot be constructed
// from this package (unexported, external module), so this double
// stands in for it -- structurally, not textually: the type it wraps is
// the REAL clickhouse-go/v2/lib/proto.Exception, not a string.
type operationErrorDouble struct {
	outer string
	cause error
}

func (e *operationErrorDouble) Error() string { return e.outer }
func (e *operationErrorDouble) Unwrap() error { return e.cause }

func TestIsMissingMembershipTableError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			"membership table, code 60, real Exception type",
			&chproto.Exception{Code: 60, Message: "Unknown table expression identifier 'work_unit_membership' in scope SELECT ..."},
			true,
		},
		{
			"membership runs table, real Exception type",
			&chproto.Exception{Code: 60, Message: "DB::Exception: UNKNOWN_TABLE. Unknown table 'work_unit_membership_runs'"},
			true,
		},
		{
			"different missing table, same code 60 -- must NOT swallow",
			&chproto.Exception{Code: 60, Message: "Unknown table expression identifier 'work_graph_edges' in scope SELECT ..."},
			false,
		},
		{
			"unrelated error, no Exception in the chain at all",
			errors.New("connection reset by peer"),
			false,
		},
		{
			// The echoed SQL in a real ClickHouse error mentions every
			// table in the query -- a naive substring search on
			// "work_unit_membership" would false-positive here. The
			// identifier clause names work_graph_edges, not membership.
			"echoed SQL mentions membership table but identifier clause does not",
			&chproto.Exception{Code: 60, Message: "Unknown table expression identifier 'work_graph_edges' in scope SELECT ... FROM work_unit_membership AS m ..."},
			false,
		},
		{
			// THE CODEX-FOUND BUG (2026-08-29, gpt-5.6-terra xhigh round),
			// regression-pinned here: dev-health-go/clickhouse's real
			// Client.Query wraps every driver error as
			// &operationError{operation, cause}, whose OWN Error() method
			// returns ONLY "ClickHouse query failed" -- never the driver's
			// real message or code. Every call site in this package wraps
			// AGAIN with fmt.Errorf("workgraph: ...: %w", err). errors.As
			// walks Unwrap() at every level automatically, so this
			// operationErrorDouble double (mirroring the real wrap shape)
			// plus the extra fmt.Errorf layer must still classify.
			"real client's two-level wrap (operationError + workgraph fmt.Errorf) -- must still classify",
			fmt.Errorf("workgraph: batch resolve membership: %w",
				&operationErrorDouble{
					outer: "ClickHouse query failed",
					cause: &chproto.Exception{Code: 60, Message: "Unknown table expression identifier 'work_unit_membership' in scope SELECT ..."},
				}),
			true,
		},
		{
			// THE FOLLOW-UP RULING'S OWN REGRESSION CASE (chris/orchestrator,
			// 2026-08-29, same day): a code-60-SHAPED error whose text says
			// "code: 60" and names work_unit_membership, but is NOT a real
			// *chproto.Exception in the chain (e.g. some other layer's
			// error happens to render similar text). The STRUCTURED check
			// must say false here -- a text-substring version of this
			// function (the version this replaced) would have said true,
			// which is exactly the failure mode the follow-up ruling
			// diagnosed: matching a format string is not matching a type,
			// and an adversarial/incidental text collision must not swallow
			// a real, different error.
			"text LOOKS like a code-60 membership-table error but carries no real Exception type -- must NOT swallow",
			errors.New("code: 60, message: Unknown table expression identifier 'work_unit_membership' in scope SELECT ..."),
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMissingMembershipTableError(tc.err); got != tc.want {
				t.Fatalf("isMissingMembershipTableError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// --- mapNodeType / mapEdgeType / mapProvenance (pure case transform) -------

func TestEnumMapping(t *testing.T) {
	if got := mapNodeType("feature_flag"); got != model.WorkGraphNodeTypeFeatureFlag {
		t.Fatalf("mapNodeType = %v, want %v", got, model.WorkGraphNodeTypeFeatureFlag)
	}
	if got := mapEdgeType("is_blocked_by"); got != model.WorkGraphEdgeTypeIsBlockedBy {
		t.Fatalf("mapEdgeType = %v, want %v", got, model.WorkGraphEdgeTypeIsBlockedBy)
	}
	if got := mapProvenance("explicit_text"); got != model.WorkGraphProvenanceExplicitText {
		t.Fatalf("mapProvenance = %v, want %v", got, model.WorkGraphProvenanceExplicitText)
	}
}

// --- resolveRepoScope (work_graph.py:126-150 / api/queries/scopes.py) ------

func TestResolveRepoScope_EmptyRefs(t *testing.T) {
	ids, shortCircuit, err := resolveRepoScope(context.Background(), &fakeClient{}, "org1", nil)
	if err != nil || shortCircuit || ids != nil {
		t.Fatalf("got (%v, %v, %v), want (nil, false, nil)", ids, shortCircuit, err)
	}
}

func TestResolveRepoScope_NoneResolve_ShortCircuits(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}}
	ids, shortCircuit, err := resolveRepoScope(context.Background(), client, "org1", []string{"nope/nope"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !shortCircuit {
		t.Fatal("want shortCircuit=true when refs given but none resolve")
	}
	if len(ids) != 0 {
		t.Fatalf("got ids=%v, want empty", ids)
	}
}

func TestResolveRepoScope_MixedUUIDAndSlugRefs_OneBatchedQuery(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{
			rows: [][]any{
				{"11111111-1111-1111-1111-111111111111"},
				{"22222222-2222-2222-2222-222222222222"},
			},
		}},
	}
	ids, shortCircuit, err := resolveRepoScope(context.Background(), client, "org1",
		[]string{"11111111-1111-1111-1111-111111111111", "acme/repo"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if shortCircuit {
		t.Fatal("must not short-circuit when refs resolved")
	}
	if client.calls != 1 {
		t.Fatalf("resolveRepoScope must issue exactly ONE query (no N+1), got %d calls", client.calls)
	}
	if len(ids) != 2 {
		t.Fatalf("got %v, want 2 resolved ids", ids)
	}
	if uuidRefs, ok := bindingValue(client.bindings[0], "uuid_refs"); !ok {
		t.Fatal("expected uuid_refs binding")
	} else if v := uuidRefs.([]string); len(v) != 1 || v[0] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("uuid_refs = %v", v)
	}
	if nameRefs, ok := bindingValue(client.bindings[0], "name_refs"); !ok {
		t.Fatal("expected name_refs binding")
	} else if v := nameRefs.([]string); len(v) != 1 || v[0] != "acme/repo" {
		t.Fatalf("name_refs = %v", v)
	}
}

// --- buildWorkGraphWhere (work_graph.py:930-1009) --------------------------

func TestBuildWorkGraphWhere_IncludeEdgeFiltersGatesEdgeOnlyClauses(t *testing.T) {
	sourceType := model.WorkGraphNodeTypeInputIssue
	filters := &model.WorkGraphEdgeFilterInput{SourceType: &sourceType, Limit: 1000}
	scope := newFilterScope(filters, nil)

	withEdgeFilters := buildWorkGraphWhere("org1", scope, true)
	if !strings.Contains(withEdgeFilters.sql, "source_type = {source_type:String}") {
		t.Fatalf("expected source_type clause when includeEdgeFilters=true: %s", withEdgeFilters.sql)
	}

	withoutEdgeFilters := buildWorkGraphWhere("org1", scope, false)
	if strings.Contains(withoutEdgeFilters.sql, "source_type = {source_type:String}") {
		t.Fatalf("source_type clause must NOT appear when includeEdgeFilters=false (aggregates apply graph-wide filters only): %s", withoutEdgeFilters.sql)
	}
	// org_id must always be present regardless.
	if !strings.Contains(withoutEdgeFilters.sql, "org_id = {org_id:String}") {
		t.Fatal("org_id clause missing")
	}
}

func TestBuildWorkGraphWhere_ThemeFilterAddsExistsClause(t *testing.T) {
	filters := &model.WorkGraphEdgeFilterInput{Theme: strPtr("delivery"), Limit: 1000}
	scope := newFilterScope(filters, nil)
	where := buildWorkGraphWhere("org1", scope, true)
	if !strings.Contains(where.sql, "EXISTS (") {
		t.Fatalf("expected correlated EXISTS clause for active theme filter: %s", where.sql)
	}
	if v, ok := bindingValue(where.bindings, "theme"); !ok || v.(string) != "delivery" {
		t.Fatalf("expected theme binding = delivery, got %v", v)
	}
	if v, ok := bindingValue(where.bindings, "wanted_count"); !ok || v.(int) != 1 {
		t.Fatalf("expected wanted_count=1 for theme-only filter, got %v", v)
	}
}

func TestBuildWorkGraphWhere_RepoIDsClause(t *testing.T) {
	filters := &model.WorkGraphEdgeFilterInput{Limit: 1000}
	scope := newFilterScope(filters, []string{"11111111-1111-1111-1111-111111111111"})
	where := buildWorkGraphWhere("org1", scope, true)
	if !strings.Contains(where.sql, "repo_id IN {repo_ids:Array(String)}") {
		t.Fatalf("expected repo_id IN clause: %s", where.sql)
	}
}

// --- fetchDedupedEdgeRows: the CHAOS-4515 fix's SQL SHAPE (not its live
// effect -- that needs a real ClickHouse and is the red-on-baseline proof
// owed under an orchestrator-granted container slot). This is a smoke test
// that the dedup collapse is actually IN the rendered query text, so a
// future edit that accidentally removes it fails a fast unit test rather
// than only a slow live one. --------------------------------------------

func TestFetchDedupedEdgeRows_QueryShapeHasArgMaxCollapse(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}}
	scope := newFilterScope(&model.WorkGraphEdgeFilterInput{Limit: 1000}, nil)
	_, err := fetchDedupedEdgeRows(context.Background(), client, "org1", scope, 1000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("want exactly 1 query, got %d", client.calls)
	}
	sql := client.statements[0]
	for _, want := range []string{
		"argMax(repo_id, last_synced)",
		"argMax(provider, last_synced)",
		"argMax(provenance, last_synced)",
		"toFloat64(argMax(confidence, last_synced))",
		"argMax(evidence, last_synced)",
		"any(edge_id)",
		"GROUP BY org_id, source_type, source_id, edge_type, target_type, target_id",
		"ORDER BY confidence DESC, edge_id ASC",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("dedup query missing %q:\n%s", want, sql)
		}
	}
	// The raw un-deduped Python shape (`FROM work_graph_edges\n        {where_sql}\n        {order_by_sql}`
	// with NO GROUP BY at all) must not be what this port sends -- that
	// would silently drop the CHAOS-4515 fix.
	if !strings.Contains(sql, "GROUP BY") {
		t.Fatal("dedup query must contain a GROUP BY -- the fix is a collapse, not a raw read")
	}
}

// --- spliceDependencyEdges: THE SPLICE TRAP (BRIEF.md, work_graph.py:1204-1229) --

func mkEdge(id string) edgeRow {
	return edgeRow{
		edgeID: id, sourceType: "issue", sourceID: "s-" + id,
		targetType: "issue", targetID: "t-" + id, edgeType: "blocks",
	}
}

// mkEdgeConfidence is mkEdge with an explicit confidence -- needed by
// TestSpliceDependencyEdges_NoResort so the seeded rows are NOT all tied
// at confidence 0 (mkEdge's default). A mutation test caught this the
// hard way (orchestrator 08-29 mutation-kill round): the original
// NoResort test used plain mkEdge for every row, so a "re-sort the
// merged slice by confidence DESC" regression was a no-op stable sort
// over all-equal keys and left the test GREEN -- a mask, not a proof.
// Real dependency-derived rows always carry confidence=1.0
// (work_graph.py:1076 / edges.go's queryDependencyEdges), strictly
// HIGHER than any real primary row's confidence, so that is the ratio
// this helper mirrors: primary rows below 1.0, dependency rows at 1.0.
func mkEdgeConfidence(id string, confidence float64) edgeRow {
	e := mkEdge(id)
	e.confidence = confidence
	return e
}

func TestSpliceDependencyEdges_NoResort(t *testing.T) {
	// Primary rows are already ORDER BY confidence DESC, edge_id ASC --
	// p1/p2 carry LOWER confidence than the dependency rows (which mirror
	// queryDependencyEdges's real constant confidence=1.0), the SAME
	// order-discriminating shape
	// test_dual_run_edges_edge_type_filtered_splice_matches uses in the
	// live dual-run proof. This is deliberate, not incidental: a "re-sort
	// the merged slice by confidence DESC" regression would place the
	// confidence=1.0 dependency rows FIRST, visibly reordering the
	// result -- confirmed by an executed mutation kill (sort.SliceStable
	// by confidence inserted into spliceDependencyEdges; this test went
	// RED; restored, verified by diff+sha256, re-ran GREEN). An earlier
	// version of this test used equal-confidence rows throughout
	// (mkEdge's default 0 for every row) and STAYED GREEN under that same
	// mutation -- a stable sort over all-equal keys is a no-op, so the
	// mutation was invisible to it. That version is what this replaces.
	primary := []edgeRow{mkEdgeConfidence("p1", 0.9), mkEdgeConfidence("p2", 0.1)}
	dependency := []edgeRow{mkEdgeConfidence("d2", 1.0), mkEdgeConfidence("d1", 1.0)} // deliberately NOT edge_id-sorted, confidence=1.0 like the real dependency query

	got := spliceDependencyEdges(primary, dependency, 10)
	want := []string{"p1", "p2", "d2", "d1"}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i, id := range want {
		if got[i].edgeID != id {
			t.Fatalf("position %d: got edgeID=%q, want %q (splice must preserve each query's own order, never re-sort -- a re-sort-by-confidence regression would put d2/d1 FIRST since they carry confidence=1.0 > p1's 0.9 and p2's 0.1)", i, got[i].edgeID, id)
		}
	}
}

func TestSpliceDependencyEdges_DedupesByIdentityNotEdgeID(t *testing.T) {
	// A dependency row sharing a primary row's IDENTITY (source/edge/target
	// tuple) must be excluded even though queryDependencyEdges computes its
	// own synthetic "wid:..." edge_id (never equal to work_graph_edges' own
	// edge_id) -- work_graph.py:1205-1214 keys existing_edge_keys on the
	// 5-tuple identity, NOT edge_id.
	primary := []edgeRow{{edgeID: "primary-hash", sourceType: "issue", sourceID: "A", edgeType: "blocks", targetType: "issue", targetID: "B"}}
	dependency := []edgeRow{{edgeID: "wid:deadbeef", sourceType: "issue", sourceID: "A", edgeType: "blocks", targetType: "issue", targetID: "B"}}

	got := spliceDependencyEdges(primary, dependency, 10)
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1 (dependency row with matching identity must be excluded)", len(got))
	}
}

func TestSpliceDependencyEdges_TruncatesToLimit(t *testing.T) {
	primary := []edgeRow{mkEdge("p1")}
	dependency := []edgeRow{mkEdge("d1"), mkEdge("d2"), mkEdge("d3")}
	got := spliceDependencyEdges(primary, dependency, 2)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2 (truncated to limit)", len(got))
	}
	if got[0].edgeID != "p1" || got[1].edgeID != "d1" {
		t.Fatalf("got %v, want [p1 d1] (primary first, then dependency in its own order, truncated)", got)
	}
}

func TestSpliceDependencyEdges_EmptyDependency_ReturnsePrimaryUnmodified(t *testing.T) {
	primary := []edgeRow{mkEdge("p1"), mkEdge("p2")}
	got := spliceDependencyEdges(primary, nil, 10)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
}

// --- queryDependencyEdges early exits (work_graph.py:1020-1024) ------------

func TestQueryDependencyEdges_NoEdgeTypeFilter_NeverQueries(t *testing.T) {
	client := &fakeClient{}
	scope := newFilterScope(&model.WorkGraphEdgeFilterInput{Limit: 1000}, nil)
	rows, err := queryDependencyEdges(context.Background(), client, "org1", &model.WorkGraphEdgeFilterInput{Limit: 1000}, scope, 1000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if rows != nil {
		t.Fatalf("got %v, want nil (unfiltered path never exercises the splice)", rows)
	}
	if client.calls != 0 {
		t.Fatalf("unfiltered path must not query ClickHouse at all, got %d calls", client.calls)
	}
}

func TestQueryDependencyEdges_RepoScopeActive_SkipsEntirely(t *testing.T) {
	blocks := model.WorkGraphEdgeTypeInputBlocks
	filters := &model.WorkGraphEdgeFilterInput{EdgeType: &blocks, Limit: 1000}
	client := &fakeClient{}
	// scope.repoIDs non-empty simulates a resolved repo filter -- Python
	// checks filters.repo_ids (POST-resolution) truthiness here.
	scope := newFilterScope(filters, []string{"11111111-1111-1111-1111-111111111111"})
	rows, err := queryDependencyEdges(context.Background(), client, "org1", filters, scope, 1000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if rows != nil || client.calls != 0 {
		t.Fatalf("repo-scoped request must skip dependency edges entirely: rows=%v calls=%d", rows, client.calls)
	}
}

func TestQueryDependencyEdges_EdgeTypeFilterActive_Queries(t *testing.T) {
	blocks := model.WorkGraphEdgeTypeInputBlocks
	filters := &model.WorkGraphEdgeFilterInput{EdgeType: &blocks, Limit: 1000}
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}}
	scope := newFilterScope(filters, nil)
	_, err := queryDependencyEdges(context.Background(), client, "org1", filters, scope, 1000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("edge_type-filtered path must query exactly once, got %d", client.calls)
	}
	sql := client.statements[0]
	if !strings.Contains(sql, "ORDER BY last_synced DESC, edge_id ASC") {
		t.Fatalf("dependency query must use its OWN order key (last_synced DESC, edge_id ASC), distinct from the primary query's confidence-based order: %s", sql)
	}
}

// --- ResolveEdges: repo-scope short circuit / theme conflict --------------

func TestResolveEdges_RepoScopeNoneResolved_ShortCircuitsWithNoDegradedReason(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: nil}}} // repo resolution query returns nothing
	filters := &model.WorkGraphEdgeFilterInput{RepoIds: []string{"nope/nope"}, Limit: 1000}
	result, err := ResolveEdges(context.Background(), client, "org1", filters)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(result.Edges) != 0 || result.DegradedReason != nil {
		t.Fatalf("got %+v, want empty result with no degraded reason", result)
	}
	if client.calls != 1 {
		t.Fatalf("short-circuit must not run the edges/dependency/membership/display-name queries, got %d calls", client.calls)
	}
}

func TestResolveEdges_ThemeSubcategoryConflict_ShortCircuits(t *testing.T) {
	client := &fakeClient{}
	filters := &model.WorkGraphEdgeFilterInput{Theme: strPtr("risk"), Subcategory: strPtr("delivery.feature_work"), Limit: 1000}
	result, err := ResolveEdges(context.Background(), client, "org1", filters)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(result.Edges) != 0 {
		t.Fatalf("cross-theme conflict must return an empty result, got %+v", result)
	}
	if client.calls != 0 {
		t.Fatalf("theme conflict must short-circuit BEFORE any query (no repo filter here), got %d calls", client.calls)
	}
}

func TestResolveEdges_UnfilteredPath_NeverCallsDependencyQuery(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: nil}, // fetchDedupedEdgeRows
		},
	}
	filters := &model.WorkGraphEdgeFilterInput{Limit: 1000}
	_, err := ResolveEdges(context.Background(), client, "org1", filters)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// Exactly 1 call: the deduped edges query. No dependency query (no
	// edge_type filter), no display-name/membership queries (zero rows).
	if client.calls != 1 {
		t.Fatalf("got %d calls, want 1 (unfiltered, zero-row path)", client.calls)
	}
}
