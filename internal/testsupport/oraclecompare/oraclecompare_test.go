// Clause-coverage unit tests for the pure comparison core. They moved here
// with the functions they cover: a comparator's own logic is exactly the code
// that must not be trusted on the strength of the comparisons it reports
// passing.
package oraclecompare

import (
	"reflect"
	"strings"
	"testing"
)

// TestDiffRowsClauseCoverage is a fast, synthetic-data unit test of
// DiffRows's own logic -- the core comparator every pair (Python-shellout
// or ClickHouse-readback alike) reuses -- isolating each of its clauses
// (matching value, differing value, present-in-Python-only,
// present-in-Go-only, the two vacuity guards added for codex finding #6)
// plus its two independent exclusion mechanisms (Python-declared,
// Go-only-declared) as its own case, so a mutation to any one clause is
// caught by a case that exercises ONLY that clause, not by coincidence
// from a case exercising several at once. This is the mutation harness's
// own "mutate compound predicates clause by clause" rule applied to the
// framework code itself, not just to a pair's row-construction logic.
func TestDiffRowsClauseCoverage(t *testing.T) {
	tests := []struct {
		name           string
		pythonRow      map[string]any
		goRow          map[string]any
		pythonExcluded map[string]string
		goOnlyFields   map[string]string
		wantMessages   int
		// wantSubstring, when non-empty, must appear in messages[0] -- this
		// is what actually distinguishes "the present-in-Python-only clause
		// fired" from "the value-mismatch clause ALSO fired, coincidentally
		// producing one message, because a missing map key decodes as a nil
		// value that fails reflect.DeepEqual against anything". A
		// mutation-harness run against an earlier version of this test
		// (which only asserted len(messages)) proved that gap: disabling
		// the present-in-Python-only/present-in-Go-only clauses SURVIVED,
		// because the fallthrough value-mismatch clause still produced
		// exactly one message for the same two cases, for the wrong reason.
		wantSubstring string
	}{
		{
			name:         "identical single field: no divergence",
			pythonRow:    map[string]any{"state": "open"},
			goRow:        map[string]any{"state": "open"},
			wantMessages: 0,
		},
		{
			name:          "differing value: exactly one divergence",
			pythonRow:     map[string]any{"state": "open"},
			goRow:         map[string]any{"state": "closed"},
			wantMessages:  1,
			wantSubstring: `python="open" go="closed"`,
		},
		{
			name:          "present in Python only: exactly one divergence",
			pythonRow:     map[string]any{"state": "open"},
			goRow:         map[string]any{},
			wantMessages:  1,
			wantSubstring: "present in Python's row (open) but absent from Go's",
		},
		{
			name:          "present in Go only: exactly one divergence",
			pythonRow:     map[string]any{},
			goRow:         map[string]any{"state": "open"},
			wantMessages:  1,
			wantSubstring: "present in Go's row (open) but absent from Python's",
		},
		{
			name:           "declared Python-side exclusion suppresses a value mismatch",
			pythonRow:      map[string]any{"reviews_count": float64(1), "state": "open"},
			goRow:          map[string]any{"reviews_count": float64(0), "state": "open"},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   0,
		},
		{
			name:           "declared Python-side exclusion suppresses a present-or-absent divergence",
			pythonRow:      map[string]any{"reviews_count": float64(1), "state": "open"},
			goRow:          map[string]any{"state": "open"},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   0,
		},
		{
			name:         "declared Go-only exclusion suppresses a present-or-absent divergence",
			pythonRow:    map[string]any{"state": "open"},
			goRow:        map[string]any{"org_id": "org-acme", "state": "open"},
			goOnlyFields: map[string]string{"org_id": "Go-side tenant bookkeeping"},
			wantMessages: 0,
		},
		{
			name: "one excluded field plus one real divergence: only the real one is reported",
			pythonRow: map[string]any{
				"reviews_count": float64(1), "state": "open",
			},
			goRow: map[string]any{
				"reviews_count": float64(0), "state": "closed",
			},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   1,
			wantSubstring:  `python="open" go="closed"`,
		},
		{
			name:          "both rows empty: fails as a setup error, not a silent pass",
			pythonRow:     map[string]any{},
			goRow:         map[string]any{},
			wantMessages:  1,
			wantSubstring: "both rows are empty",
		},
		{
			name:           "every field excluded: fails as a setup error, not a silent pass",
			pythonRow:      map[string]any{"reviews_count": float64(1)},
			goRow:          map[string]any{"reviews_count": float64(1)},
			pythonExcluded: map[string]string{"reviews_count": "owned by github/pr-reviews"},
			wantMessages:   1,
			wantSubstring:  "every field in both rows is declared excluded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			messages := DiffRows("case", tt.pythonRow, tt.goRow, tt.pythonExcluded, tt.goOnlyFields)
			if len(messages) != tt.wantMessages {
				t.Fatalf("DiffRows() = %d message(s) %v, want %d", len(messages), messages, tt.wantMessages)
			}
			if tt.wantSubstring != "" {
				if len(messages) == 0 || !strings.Contains(messages[0], tt.wantSubstring) {
					t.Fatalf("DiffRows() = %v, want a message containing %q", messages, tt.wantSubstring)
				}
			}
		})
	}
}

// TestCheckExclusionIntegrityClauseCoverage is a fast, synthetic-data unit
// test of CheckExclusionIntegrity's own logic (codex finding, third
// review): isolating "a goOnlyFields key actually appears on the Python
// side" and "a declared exclusion never matches anything in the batch"
// (for both exclusion maps) as their own cases, plus a clean case proving
// neither check fires on legitimate, currently-effective declarations.
func TestCheckExclusionIntegrityClauseCoverage(t *testing.T) {
	tests := []struct {
		name           string
		pythonRows     map[string]map[string]any
		goRows         map[string]map[string]any
		excludedFields map[string]string
		goOnlyFields   map[string]string
		wantMessages   int
		wantSubstring  string
	}{
		{
			name:           "legitimate exclusions, both used: no violation",
			pythonRows:     map[string]map[string]any{"c1": {"state": "open"}},
			goRows:         map[string]map[string]any{"c1": {"state": "open", "reviews_count": 0, "org_id": "x"}},
			excludedFields: map[string]string{"reviews_count": "owned by pr-reviews"},
			goOnlyFields:   map[string]string{"org_id": "Go-side bookkeeping"},
			wantMessages:   0,
		},
		{
			name:          "goOnlyFields key actually present on the Python side: violation",
			pythonRows:    map[string]map[string]any{"c1": {"state": "open", "org_id": "leaked"}},
			goRows:        map[string]map[string]any{"c1": {"state": "open", "org_id": "x"}},
			goOnlyFields:  map[string]string{"org_id": "Go-side bookkeeping"},
			wantMessages:  1,
			wantSubstring: "claims this field never appears on the Python side",
		},
		{
			name:           "excluded_fields entry never matches anything: stale",
			pythonRows:     map[string]map[string]any{"c1": {"state": "open"}},
			goRows:         map[string]map[string]any{"c1": {"state": "open"}},
			excludedFields: map[string]string{"never_seen": "supposedly owned elsewhere"},
			wantMessages:   1,
			wantSubstring:  `excluded_fields["never_seen"]`,
		},
		{
			name:          "goOnlyFields entry never matches anything: stale",
			pythonRows:    map[string]map[string]any{"c1": {"state": "open"}},
			goRows:        map[string]map[string]any{"c1": {"state": "open"}},
			goOnlyFields:  map[string]string{"never_seen": "supposedly Go-only"},
			wantMessages:  1,
			wantSubstring: `goOnlyFields["never_seen"]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			messages := CheckExclusionIntegrity(tt.pythonRows, tt.goRows, tt.excludedFields, tt.goOnlyFields)
			if len(messages) != tt.wantMessages {
				t.Fatalf("CheckExclusionIntegrity() = %d message(s) %v, want %d",
					len(messages), messages, tt.wantMessages)
			}
			if tt.wantSubstring != "" {
				if len(messages) == 0 || !strings.Contains(messages[0], tt.wantSubstring) {
					t.Fatalf("CheckExclusionIntegrity() = %v, want a message containing %q",
						messages, tt.wantSubstring)
				}
			}
		})
	}
}

// TestTypedValuesEqualCanonicalizesFloatAndDatetimeText is codex's fourth-
// round finding: the SAME IEEE754 double or the SAME instant can encode to
// DIFFERENT text on the Python and Go sides (Python's repr(5.0) is "5.0";
// Go's strconv.FormatFloat(5.0, 'g', -1, 64) is "5" -- same value, and
// Python's isoformat() shows a fixed 6-digit microsecond field while Go's
// RFC3339Nano strips trailing zero fractional digits). A bare
// reflect.DeepEqual on the tagged {"t","v"} envelope would report these as
// DIVERGENT even though the underlying values are equal -- exactly the
// false-positive noise that would make a real pair with float/sub-second
// fields untrustworthy. Also proves the inverse: genuinely different
// values still compare unequal, and non-float/datetime tags (str/int/bool)
// are untouched by the new parse-and-compare path.
func TestTypedValuesEqualCanonicalizesFloatAndDatetimeText(t *testing.T) {
	tagged := func(tag, value string) map[string]any {
		return map[string]any{"t": tag, "v": value}
	}
	tests := []struct {
		name      string
		a, b      any
		wantEqual bool
	}{
		{
			name:      "equal floats, different text (Python repr vs Go FormatFloat)",
			a:         tagged("float", "5.0"),
			b:         tagged("float", "5"),
			wantEqual: true,
		},
		{
			name:      "equal instants, different fractional-second width",
			a:         tagged("datetime", "2026-07-10T09:00:00.123000Z"),
			b:         tagged("datetime", "2026-07-10T09:00:00.123Z"),
			wantEqual: true,
		},
		{
			name:      "genuinely different floats stay unequal",
			a:         tagged("float", "5.0"),
			b:         tagged("float", "5.1"),
			wantEqual: false,
		},
		{
			name:      "genuinely different instants stay unequal",
			a:         tagged("datetime", "2026-07-10T09:00:00Z"),
			b:         tagged("datetime", "2026-07-10T09:00:01Z"),
			wantEqual: false,
		},
		{
			name:      "str tag: exact text comparison, untouched by the float/datetime path",
			a:         tagged("str", "open"),
			b:         tagged("str", "open"),
			wantEqual: true,
		},
		{
			name:      "int tag: exact text comparison, untouched by the float/datetime path",
			a:         tagged("int", "5"),
			b:         tagged("int", "5.0"), // an int side must never accept float-shaped text
			wantEqual: false,
		},
		{
			name:      "column list of equal floats with different text",
			a:         []any{tagged("float", "5.0"), tagged("float", "0.0")},
			b:         []any{tagged("float", "5"), tagged("float", "0")},
			wantEqual: true,
		},
		{
			name:      "column list where one element genuinely differs",
			a:         []any{tagged("float", "5.0"), tagged("float", "1.0")},
			b:         []any{tagged("float", "5"), tagged("float", "2")},
			wantEqual: false,
		},
		{
			name:      "column lists of different length are never equal",
			a:         []any{tagged("float", "5.0")},
			b:         []any{tagged("float", "5"), tagged("float", "5")},
			wantEqual: false,
		},
		{
			name:      "column list holding nulls stays position-sensitive",
			a:         []any{nil, tagged("float", "5.0")},
			b:         []any{tagged("float", "5"), nil},
			wantEqual: false,
		},
		{
			name:      "nested object canonicalizes its float leaves",
			a:         map[string]any{"hours": tagged("float", "5.0"), "name": tagged("str", "a")},
			b:         map[string]any{"hours": tagged("float", "5"), "name": tagged("str", "a")},
			wantEqual: true,
		},
		{
			name:      "nested object missing a key is not equal",
			a:         map[string]any{"hours": tagged("float", "5.0"), "name": tagged("str", "a")},
			b:         map[string]any{"hours": tagged("float", "5")},
			wantEqual: false,
		},
		{
			name:      "differing tags on the same text are never equal",
			a:         tagged("date", "2026-08-04"),
			b:         tagged("str", "2026-08-04"),
			wantEqual: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TypedValuesEqual(tt.a, tt.b)
			if got != tt.wantEqual {
				t.Fatalf("TypedValuesEqual(%#v, %#v) = %v, want %v", tt.a, tt.b, got, tt.wantEqual)
			}
		})
	}
}

// oracleDay is a day type that opts into the date tag, exactly as a
// providersync row-column type does.
type oracleDay string

func (d oracleDay) OracleDate() string { return string(d) }

// plainDay looks identical on the wire but does NOT implement DateValued.
type plainDay string

// TestDateValuedTypesTagAsDate is the regression guard for the one behaviour
// this extraction actually broke.
//
// The opt-in used to be an anonymous `interface{ oracleDate() string }`. A Go
// interface whose method name is UNEXPORTED can only be satisfied inside the
// package that declares it, so moving the encoder here silently stopped every
// providersync day type from matching: nothing failed to compile, and the type
// assertion just fell through to the reflect.String branch. The live-oracle
// lane caught it as ~22 pairs reporting python={"t":"date"} against
// go={"t":"str"} for the same text. A compile-time-invisible break needs a
// test that names it, not a comment.
func TestDateValuedTypesTagAsDate(t *testing.T) {
	encoded := TypedEncode(t, reflect.ValueOf(oracleDay("2026-08-04")))
	want := map[string]any{"t": "date", "v": "2026-08-04"}
	if !reflect.DeepEqual(encoded, want) {
		t.Fatalf("a DateValued type must tag as date: got %#v want %#v", encoded, want)
	}
}

// TestNonDateValuedStringTagsAsStrAndNeverEqualsADate is the red control for
// the test above: it pins the WRONG behaviour that the bug produced, and
// proves the comparator still rejects it. Without this, the test above could
// pass while a date and a same-looking string compared equal anyway.
func TestNonDateValuedStringTagsAsStrAndNeverEqualsADate(t *testing.T) {
	asString := TypedEncode(t, reflect.ValueOf(plainDay("2026-08-04")))
	want := map[string]any{"t": "str", "v": "2026-08-04"}
	if !reflect.DeepEqual(asString, want) {
		t.Fatalf("a plain string type must tag as str: got %#v", asString)
	}
	asDate := TypedEncode(t, reflect.ValueOf(oracleDay("2026-08-04")))
	if TypedValuesEqual(asDate, asString) {
		t.Fatal("a date and a same-looking plain string must NOT compare equal -- " +
			"this is precisely the collapse the type tag exists to prevent")
	}
}

// TestTypedValuesEqualRejectsCrossTagAndNonFinite verifies the two coercion
// hazards carried over from the P0 review rounds against THIS core, rather
// than assuming they were handled.
//
// Both already hold, and for a better reason than a value-level check: tags
// are compared before values, so an int and an integral float never reach a
// numeric comparison at all.
func TestTypedValuesEqualRejectsCrossTagAndNonFinite(t *testing.T) {
	tests := []struct {
		name         string
		python, goal any
		wantEqual    bool
	}{
		{
			name:      "int and integral float never collapse",
			python:    map[string]any{"t": "int", "v": "5"},
			goal:      map[string]any{"t": "float", "v": "5"},
			wantEqual: false,
		},
		{
			name:      "int and same-digit string never collapse",
			python:    map[string]any{"t": "int", "v": "5"},
			goal:      map[string]any{"t": "str", "v": "5"},
			wantEqual: false,
		},
		{
			name:      "integers beyond float64 precision stay distinct",
			python:    map[string]any{"t": "int", "v": "9007199254740992"},
			goal:      map[string]any{"t": "int", "v": "9007199254740993"},
			wantEqual: false,
		},
		{
			name:   "NaN never equals NaN, so a garbage pair reports as a divergence",
			python: map[string]any{"t": "float", "v": "NaN"},
			goal:   map[string]any{"t": "float", "v": "NaN"},
			// Fail-closed: two unusable values must not read as a match.
			wantEqual: false,
		},
		{
			name:      "differing float text for the same double still matches",
			python:    map[string]any{"t": "float", "v": "5.0"},
			goal:      map[string]any{"t": "float", "v": "5"},
			wantEqual: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if got := TypedValuesEqual(test.python, test.goal); got != test.wantEqual {
				t.Fatalf("TypedValuesEqual(%#v, %#v) = %v, want %v",
					test.python, test.goal, got, test.wantEqual)
			}
		})
	}
}
