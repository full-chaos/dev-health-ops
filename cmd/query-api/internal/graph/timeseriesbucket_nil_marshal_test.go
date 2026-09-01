package graph

// CHAOS-4703: executable proof that a nil TimeseriesBucket.Value marshals
// to `{date, value: null}`, not a collapsed `null` for the whole bucket
// (and, one level up, not a collapsed `null` for the enclosing non-null
// `[TimeseriesBucket!]!` list).
//
// Same trap CHAOS-4658 found on BreakdownItem and CHAOS-4701 found on
// SankeyNode/SankeyEdge: gqlgen's generated _TimeseriesBucket object
// marshaler (generated.go) carried an `if out.Values[i] == graphql.Null
// { out.Invalids++ }` check for the "value" case -- a check gqlgen only
// emits for a NON-NULL field. That check predates this ticket: value was
// Float! until this commit widened the SDL to Float. The per-field
// marshaler (_TimeseriesBucket_value) was ALREADY hand-edited to a
// nullable marshal back under CHAOS-4657 (Go type went to *float64 then,
// SDL deliberately left alone) -- so a nil Value legitimately marshals
// that ONE field to `graphql.Null`. Left uncorrected, the parent's stale
// Invalids++ would still fire on that legitimate null and collapse the
// WHOLE TimeseriesBucket to graphql.Null, and then
// marshalNTimeseriesBucket2ᚕ...'s `[TimeseriesBucket!]!` non-null-list
// contract would collapse the ENTIRE buckets list to null on top of
// that -- strictly worse than the bug CHAOS-4703 sets out to fix.
//
// This test calls the REAL generated _TimeseriesBucket function -- the
// same code gqlgen's handler calls on a live /query request -- per
// AGENTS.md's "diagnoses need executed repro" and this package's
// breakdownitem_nil_marshal_test.go / sankey_nil_marshal_test.go
// precedent (same newTestExecutionContext helper, same
// call-the-generated-code-directly method). RED-FIRST: run this test
// against the pre-fix tree (SDL still `Float!`, parent Invalids++ still
// present) and TestTimeseriesBucket_NilValue_MarshalsFieldNull_NotWholeBucketNull
// fails with "got graphql.Null for the WHOLE bucket".
import (
	"testing"
	"time"

	gqlgen "github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

func timeseriesBucketSelectionSet() ast.SelectionSet {
	// Alias must be set -- see breakdownItemSelectionSet's identical note:
	// FieldSet.MarshalGQL writes field.Alias as the JSON key and
	// CollectedField carries no default for an unset Alias.
	return ast.SelectionSet{
		&ast.Field{Name: "date", Alias: "date"},
		&ast.Field{Name: "value", Alias: "value"},
	}
}

// TestTimeseriesBucket_NilValue_MarshalsFieldNull_NotWholeBucketNull proves
// BOTH directions in one bucket, the same discipline CHAOS-4657's
// ExecuteTimeseries scan-level test and CHAOS-4658/4701's marshal-level
// tests apply: a nil Value must marshal to `null` for THAT FIELD, while
// date -- present on the SAME object -- must still marshal to its real
// value. A test that only asserted "the whole bucket isn't null" could
// pass on a change that dropped value from the selection set entirely;
// asserting both fields' literal wire output rules that out.
func TestTimeseriesBucket_NilValue_MarshalsFieldNull_NotWholeBucketNull(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	d := graphqldate.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	obj := &model.TimeseriesBucket{Date: d, Value: nil}

	result := ec._TimeseriesBucket(ctx, timeseriesBucketSelectionSet(), obj)
	if result == gqlgen.Null {
		t.Fatalf("_TimeseriesBucket: got graphql.Null for the WHOLE bucket from a nil value field -- " +
			"a legitimately-nullable value field is still flipping the parent's Invalids counter and " +
			"collapsing the entire bucket (and, one layer up, the enclosing non-null buckets list)")
	}

	got := marshalToString(t, result)
	want := `{"date":"2026-01-01","value":null}`
	if got != want {
		t.Fatalf("_TimeseriesBucket marshal mismatch:\n got:  %s\n want: %s", got, want)
	}
}

// TestTimeseriesBucket_PopulatedValue_StillMarshalsRealNumber is the
// OTHER direction in a separate call (mirroring, at the marshal layer,
// the same both-directions discipline CHAOS-4657's ExecuteTimeseries
// test applies at the scan layer): a fix that made every value marshal
// to null -- e.g. deleting the "value" case from the switch entirely --
// would still pass the nil-value test above but must fail this one.
func TestTimeseriesBucket_PopulatedValue_StillMarshalsRealNumber(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	d := graphqldate.New(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	v := 42.5
	obj := &model.TimeseriesBucket{Date: d, Value: &v}

	result := ec._TimeseriesBucket(ctx, timeseriesBucketSelectionSet(), obj)
	if result == gqlgen.Null {
		t.Fatalf("_TimeseriesBucket: got graphql.Null for a fully-populated bucket")
	}

	got := marshalToString(t, result)
	want := `{"date":"2026-01-02","value":42.5}`
	if got != want {
		t.Fatalf("_TimeseriesBucket marshal mismatch:\n got:  %s\n want: %s", got, want)
	}
}
