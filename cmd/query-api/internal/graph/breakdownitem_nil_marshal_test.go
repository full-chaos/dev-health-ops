package graph

// CHAOS-4658: executable proof that a nil BreakdownItem.Value marshals
// to `{key, value: null, label}`, not a collapsed `null` for the whole
// item.
//
// codex round 2 (gpt-5.6-terra, xhigh, chaos-4658-round2-20260831T183514)
// found what a static read of only the per-field marshal function
// (_BreakdownItem_value, hand-edited under CHAOS-4650/4658 to return
// graphql.Null for a nil pointer instead of panicking) could not show:
// the PARENT object marshaler this repo's earlier `gqlgen generate` run
// produced, _BreakdownItem (generated.go), still carried the
// `if out.Values[i] == graphql.Null { out.Invalids++ }` check gqlgen
// only emits for a NON-NULL field. That check predates CHAOS-4650 --
// value was Float! then -- and nothing regenerates this file (deliberate:
// regeneration would also revert every other hand-edit in it). Widening
// the SDL and the field marshaler alone left this parent check as a
// silent leftover: a genuinely-nil value would flip Invalids, and
// _BreakdownItem would return graphql.Null for the WHOLE item instead of
// `{key, value: null, label}` -- then marshalNBreakdownItem2ᚕ...'s
// `[BreakdownItem!]!` non-null-list contract would collapse the ENTIRE
// items list to null on top of that, since a list element came back
// null. Two swallowed nulls, not the one this ticket exists to produce.
//
// This test calls the REAL generated _BreakdownItem function -- the same
// code gqlgen's handler calls on a live /query request -- rather than
// re-deriving its behavior from reading the source, per AGENTS.md's
// "diagnoses need executed repro" and this package's own
// flowmatrix_nil_marshal_test.go precedent (same newTestExecutionContext
// helper, same call-the-generated-code-directly method). The
// ast.SelectionSet below is hand-built rather than parsed from a query
// string: gqlgen's graphql.CollectFields only reads sel.Name/Directives
// off each *ast.Field (executable_schema.go's collectFields), so a bare
// {Name: "key"|"value"|"label"} triple is a faithful stand-in for what a
// real parsed query for `{ key value label }` produces.
import (
	"testing"

	gqlgen "github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

func breakdownItemSelectionSet() ast.SelectionSet {
	// Alias, not just Name, must be set: FieldSet.MarshalGQL writes
	// field.Alias as the JSON key (fieldset.go), and CollectedField's
	// embedded *ast.Field carries no default -- an unset Alias marshaled
	// as the literal empty string key for all three fields on the first
	// attempt at this test (caught by this test itself, not asserted
	// away).
	return ast.SelectionSet{
		&ast.Field{Name: "key", Alias: "key"},
		&ast.Field{Name: "value", Alias: "value"},
		&ast.Field{Name: "label", Alias: "label"},
	}
}

// TestBreakdownItem_NilValue_MarshalsFieldNull_NotWholeItemNull proves
// BOTH directions in one item, the same discipline CHAOS-4657's
// timeseries test applies: a nil Value must marshal to `null` for THAT
// FIELD, while key and label -- present on the SAME object -- must still
// marshal to their real values. A test that only asserted "the whole
// object isn't null" could pass on a change that dropped value from the
// selection set entirely; asserting all three fields' literal wire
// output rules that out.
func TestBreakdownItem_NilValue_MarshalsFieldNull_NotWholeItemNull(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	label := "Real Label"
	obj := &model.BreakdownItem{Key: "repo-null", Value: nil, Label: &label}

	result := ec._BreakdownItem(ctx, breakdownItemSelectionSet(), obj)
	if result == gqlgen.Null {
		t.Fatalf("_BreakdownItem: got graphql.Null for the WHOLE item from a nil value field -- " +
			"codex's P1 is CONFIRMED REAL: a legitimately-nullable value field is still " +
			"flipping the parent's Invalids counter and collapsing the entire item")
	}

	got := marshalToString(t, result)
	want := `{"key":"repo-null","value":null,"label":"Real Label"}`
	if got != want {
		t.Fatalf("_BreakdownItem marshal mismatch:\n got:  %s\n want: %s", got, want)
	}
}

// TestBreakdownItem_PopulatedValue_StillMarshalsRealNumber is the OTHER
// direction in a separate call (mirroring, at the marshal layer, the
// same both-directions discipline CHAOS-4657's ExecuteTimeseries test
// applies at the scan layer): a fix that made every value marshal to
// null -- e.g. deleting the "value" case from the switch entirely --
// would still pass the nil-value test above but must fail this one.
func TestBreakdownItem_PopulatedValue_StillMarshalsRealNumber(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	v := 42.5
	obj := &model.BreakdownItem{Key: "repo-real", Value: &v, Label: nil}

	result := ec._BreakdownItem(ctx, breakdownItemSelectionSet(), obj)
	if result == gqlgen.Null {
		t.Fatalf("_BreakdownItem: got graphql.Null for a fully-populated item")
	}

	got := marshalToString(t, result)
	want := `{"key":"repo-real","value":42.5,"label":null}`
	if got != want {
		t.Fatalf("_BreakdownItem marshal mismatch:\n got:  %s\n want: %s", got, want)
	}
}
