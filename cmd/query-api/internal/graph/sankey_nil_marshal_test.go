package graph

// CHAOS-4701: executable proof that a nil SankeyNode.Value / SankeyEdge.Value
// marshals to `{..., value: null}`, not a collapsed `null` for the whole
// item -- the CHAOS-4658 gqlgen-generated-code trap, applied to this
// ticket's two fields.
//
// CHAOS-4658's own P1 (codex round 2, gpt-5.6-terra, xhigh,
// chaos-4658-round2-20260831T183514) found what a static read of only
// the per-field marshal function (_BreakdownItem_value) could not show:
// the PARENT object marshaler this repo's earlier `gqlgen generate` run
// produced still carried the `if out.Values[i] == graphql.Null {
// out.Invalids++ }` check gqlgen only emits for a NON-NULL field. That
// check predates a value-nullability widening -- nothing regenerates
// this file (deliberate: regeneration would also revert every other
// hand-edit in it). Widening the SDL and the field marshaler alone
// leaves this parent check as a silent leftover: a genuinely-nil value
// flips Invalids, and _SankeyNode/_SankeyEdge return graphql.Null for
// the WHOLE item instead of `{..., value: null}` -- then
// marshalNSankeyNode2ᚕ.../marshalNSankeyEdge2ᚕ...'s `[SankeyNode!]!` /
// `[SankeyEdge!]!` non-null-list contract would collapse the ENTIRE
// nodes/edges list to null on top of that, since a list element came
// back null. Two swallowed nulls, not the one this ticket exists to
// produce. CHAOS-4701's own ticket text names this exact hazard
// explicitly, quoting CHAOS-4658's finding, so this test is written
// FIRST (before trusting the hand-edit) rather than assumed safe by
// analogy.
//
// This test calls the REAL generated _SankeyNode/_SankeyEdge functions
// -- the same code gqlgen's handler calls on a live /query request --
// rather than re-deriving their behavior from reading the source, per
// AGENTS.md's "diagnoses need executed repro" and this package's own
// breakdownitem_nil_marshal_test.go / flowmatrix_nil_marshal_test.go
// precedent (same newTestExecutionContext helper, same
// call-the-generated-code-directly method, same hand-built
// ast.SelectionSet technique -- gqlgen's graphql.CollectFields only
// reads sel.Name/Directives off each *ast.Field, so a bare
// {Name: "id"|"value"|...} triple is a faithful stand-in for a real
// parsed query).
import (
	"testing"

	gqlgen "github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func sankeyNodeSelectionSet() ast.SelectionSet {
	// Alias, not just Name, must be set -- FieldSet.MarshalGQL writes
	// field.Alias as the JSON key (fieldset.go); see
	// breakdownitem_nil_marshal_test.go's breakdownItemSelectionSet for
	// the same discipline and why it matters.
	return ast.SelectionSet{
		&ast.Field{Name: "id", Alias: "id"},
		&ast.Field{Name: "label", Alias: "label"},
		&ast.Field{Name: "dimension", Alias: "dimension"},
		&ast.Field{Name: "value", Alias: "value"},
	}
}

func sankeyEdgeSelectionSet() ast.SelectionSet {
	return ast.SelectionSet{
		&ast.Field{Name: "source", Alias: "source"},
		&ast.Field{Name: "target", Alias: "target"},
		&ast.Field{Name: "value", Alias: "value"},
	}
}

// TestSankeyNode_NilValue_MarshalsFieldNull_NotWholeItemNull proves BOTH
// directions in one item, the same discipline CHAOS-4657's timeseries
// test and CHAOS-4658's breakdown-marshal test apply: a nil Value must
// marshal to `null` for THAT FIELD, while id/label/dimension -- present
// on the SAME object -- must still marshal to their real values. A test
// that only asserted "the whole object isn't null" could pass on a
// change that dropped value from the selection set entirely; asserting
// all four fields' literal wire output rules that out.
func TestSankeyNode_NilValue_MarshalsFieldNull_NotWholeItemNull(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	obj := &model.SankeyNode{ID: "TEAM:team-null", Label: "team-null", Dimension: "TEAM", Value: nil}

	result := ec._SankeyNode(ctx, sankeyNodeSelectionSet(), obj)
	if result == gqlgen.Null {
		t.Fatalf("_SankeyNode: got graphql.Null for the WHOLE item from a nil value field -- " +
			"CHAOS-4658's P1 class is CONFIRMED REAL here too: a legitimately-nullable value field is " +
			"still flipping the parent's Invalids counter and collapsing the entire item")
	}

	got := marshalToString(t, result)
	want := `{"id":"TEAM:team-null","label":"team-null","dimension":"TEAM","value":null}`
	if got != want {
		t.Fatalf("_SankeyNode marshal mismatch:\n got:  %s\n want: %s", got, want)
	}
}

// TestSankeyNode_PopulatedValue_StillMarshalsRealNumber is the OTHER
// direction in a separate call: a fix that made every value marshal to
// null -- e.g. deleting the "value" case from the switch entirely --
// would still pass the nil-value test above but must fail this one.
func TestSankeyNode_PopulatedValue_StillMarshalsRealNumber(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	v := 9.5
	obj := &model.SankeyNode{ID: "TEAM:team-real", Label: "team-real", Dimension: "TEAM", Value: &v}

	result := ec._SankeyNode(ctx, sankeyNodeSelectionSet(), obj)
	if result == gqlgen.Null {
		t.Fatalf("_SankeyNode: got graphql.Null for a fully-populated item")
	}

	got := marshalToString(t, result)
	want := `{"id":"TEAM:team-real","label":"team-real","dimension":"TEAM","value":9.5}`
	if got != want {
		t.Fatalf("_SankeyNode marshal mismatch:\n got:  %s\n want: %s", got, want)
	}
}

// TestSankeyEdge_NilValue_MarshalsFieldNull_NotWholeItemNull is
// SankeyNode's proof above, applied to SankeyEdge -- CHAOS-4701 widened
// BOTH types' value field in the same commit, and each has its own
// gqlgen-generated parent marshaler with its own Invalids++ leftover to
// check independently; one type marshaling correctly does not imply the
// other does.
func TestSankeyEdge_NilValue_MarshalsFieldNull_NotWholeItemNull(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	obj := &model.SankeyEdge{Source: "TEAM:team-a", Target: "TEAM:team-null", Value: nil}

	result := ec._SankeyEdge(ctx, sankeyEdgeSelectionSet(), obj)
	if result == gqlgen.Null {
		t.Fatalf("_SankeyEdge: got graphql.Null for the WHOLE item from a nil value field -- " +
			"CHAOS-4658's P1 class is CONFIRMED REAL here too")
	}

	got := marshalToString(t, result)
	want := `{"source":"TEAM:team-a","target":"TEAM:team-null","value":null}`
	if got != want {
		t.Fatalf("_SankeyEdge marshal mismatch:\n got:  %s\n want: %s", got, want)
	}
}

// TestSankeyEdge_PopulatedValue_StillMarshalsRealNumber is SankeyEdge's
// other-direction proof, mirroring TestSankeyNode_PopulatedValue_StillMarshalsRealNumber.
func TestSankeyEdge_PopulatedValue_StillMarshalsRealNumber(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	v := 4.25
	obj := &model.SankeyEdge{Source: "TEAM:team-a", Target: "TEAM:team-real", Value: &v}

	result := ec._SankeyEdge(ctx, sankeyEdgeSelectionSet(), obj)
	if result == gqlgen.Null {
		t.Fatalf("_SankeyEdge: got graphql.Null for a fully-populated item")
	}

	got := marshalToString(t, result)
	want := `{"source":"TEAM:team-a","target":"TEAM:team-real","value":4.25}`
	if got != want {
		t.Fatalf("_SankeyEdge marshal mismatch:\n got:  %s\n want: %s", got, want)
	}
}
