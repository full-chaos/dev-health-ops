package teamsidentity

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// TestQueryBoolDefaultTrueMatchesFastAPI pins queryBoolDefaultTrue against
// FastAPI's own `active_only: bool = True` query-parameter behaviour,
// captured live against a real FastAPI app: an ABSENT key uses the default
// without validation; a PRESENT key (including present-but-empty) is
// checked against pydantic's fixed case-insensitive bool vocabulary and
// anything outside it is a "bool_parsing" 422, never a silent default.
func TestQueryBoolDefaultTrueMatchesFastAPI(t *testing.T) {
	cases := []struct {
		query       string // "" means the key is entirely absent
		wantValue   bool
		wantPresent bool // whether an error is expected
	}{
		{query: "", wantValue: true},
		{query: "?active_only=true", wantValue: true},
		{query: "?active_only=True", wantValue: true},
		{query: "?active_only=TRUE", wantValue: true},
		{query: "?active_only=1", wantValue: true},
		{query: "?active_only=yes", wantValue: true},
		{query: "?active_only=y", wantValue: true},
		{query: "?active_only=on", wantValue: true},
		{query: "?active_only=t", wantValue: true},
		{query: "?active_only=false", wantValue: false},
		{query: "?active_only=0", wantValue: false},
		{query: "?active_only=no", wantValue: false},
		{query: "?active_only=n", wantValue: false},
		{query: "?active_only=off", wantValue: false},
		{query: "?active_only=f", wantValue: false},
		{query: "?active_only=not-a-bool", wantPresent: true},
		{query: "?active_only=2", wantPresent: true},
		{query: "?active_only=-1", wantPresent: true},
		{query: "?active_only=01", wantPresent: true},
		{query: "?active_only=1.0", wantPresent: true},
		{query: "?active_only=", wantPresent: true}, // present, empty -- still 422, unlike an absent key
		{query: "?active_only=%20true%20", wantPresent: true},
		{query: "?active_only=tru", wantPresent: true},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "/x"+tc.query, nil)
		value, badBool := queryBoolDefaultTrue(req, "active_only")
		if tc.wantPresent {
			if badBool == nil {
				t.Errorf("query %q: want a bool_parsing error, got none", tc.query)
				continue
			}
			if badBool.Type != "bool_parsing" || badBool.Msg != "Input should be a valid boolean, unable to interpret input" {
				t.Errorf("query %q: got %+v", tc.query, badBool)
			}
			continue
		}
		if badBool != nil {
			t.Errorf("query %q: unexpected error %+v", tc.query, badBool)
			continue
		}
		if value != tc.wantValue {
			t.Errorf("query %q: got %v, want %v", tc.query, value, tc.wantValue)
		}
	}
}

// TestUnknownTeamIDsDetailSortsAndQuotes proves the 404 detail sorts unique
// ids and single-quotes each, matching
// f"Unknown team_id(s): {sorted(set(missing))}" for the common case, AND
// (r2, CHAOS-6310 r1 finding #6, fixed) that a value holding an apostrophe
// switches to Python repr()'s double-quote form via the shared
// pythonparity.StrRepr encoder -- this was the live, reproducible gap the
// r2 codex round caught (a P1: Go single-quoted unconditionally, Python
// double-quotes here).
func TestUnknownTeamIDsDetailSortsAndQuotes(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want string
	}{
		{"common case, no apostrophe", []string{"b", "a", "b"}, `Unknown team_id(s): ['a', 'b']`},
		{"apostrophe switches to double quotes", []string{"doesn't-exist"}, `Unknown team_id(s): ["doesn't-exist"]`},
		{"mixed: only the apostrophe-holding element switches", []string{"plain", "doesn't-exist"}, `Unknown team_id(s): ["doesn't-exist", 'plain']`},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got := unknownTeamIDsDetail(test.in)
			if got != test.want {
				t.Errorf("got %s, want %s", got, test.want)
			}
		})
	}
}

// TestPythonProviderIdentitiesJSONMatchesJSONDumpsSpacing pins
// pythonProviderIdentitiesJSON (built on the shared pyjson.Dumps, not a
// route-local encoder) against Python's bare `json.dumps` default
// separators (", ", ": "), captured live -- pyjson.Marshal's compact
// Starlette-response form omits both spaces. Also proves (r2, CHAOS-6310
// finding #3) that keys render in the dict's OWN order, not sorted: the
// last case's insertion order ("b" then "a") is deliberately
// non-alphabetical, matching Python's dict, which never reorders.
func TestPythonProviderIdentitiesJSONMatchesJSONDumpsSpacing(t *testing.T) {
	build := func(pairs ...struct {
		key    string
		values []string
	}) *pybody.OrderedStringListDict {
		d := pybody.NewOrderedStringListDict()
		for _, p := range pairs {
			d.Set(p.key, p.values)
		}
		return d
	}
	pair := func(key string, values ...string) struct {
		key    string
		values []string
	} {
		return struct {
			key    string
			values []string
		}{key, values}
	}
	cases := []struct {
		name string
		in   *pybody.OrderedStringListDict
		want string
	}{
		{"nil dict", nil, `{}`},
		{"empty dict", pybody.NewOrderedStringListDict(), `{}`},
		{"single key", build(pair("order-provider-0", "order-value-0")), `{"order-provider-0": ["order-value-0"]}`},
		{"empty value list", build(pair("a")), `{"a": []}`},
		{"insertion order preserved, not sorted", build(pair("b", "x", "y"), pair("a", "z")), `{"b": ["x", "y"], "a": ["z"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pythonProviderIdentitiesJSON(tc.in)
			if err != nil {
				t.Fatalf("pythonProviderIdentitiesJSON: %v", err)
			}
			if got != tc.want {
				t.Errorf("pythonProviderIdentitiesJSON = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestNaiveDatetimeMatchesPydanticNaive pins the wire form of the naive
// created_at/updated_at columns: pydantic-core's naive datetime, six
// microsecond digits when non-zero (trailing zeros kept), no fraction when
// zero, never a zone. The live venue (adminstampvenue) compares the same
// rendering against the real Python api.
func TestNaiveDatetimeMatchesPydanticNaive(t *testing.T) {
	for _, tc := range []struct {
		micro int
		want  string
	}{
		{895620, "2026-09-21T11:37:05.895620"},
		{895335, "2026-09-21T11:37:05.895335"},
		{1, "2026-09-21T11:37:05.000001"},
		{100000, "2026-09-21T11:37:05.100000"},
		{0, "2026-09-21T11:37:05"},
	} {
		at := time.Date(2026, 9, 21, 11, 37, 5, tc.micro*1000, time.UTC)
		if got := naiveDatetime(at); got != tc.want {
			t.Errorf("micro %d: got %q want %q", tc.micro, got, tc.want)
		}
	}
}

// TestNaiveDatetimeInServerZone pins the non-UTC server-zone form: the
// driver reports the stored instant in the server zone and the Python api
// prints that wall clock with its offset (observed live: a 18:37:05Z
// instant read from a America/Los_Angeles server prints
// 2026-09-21T11:37:05.895620-07:00). The live venue runs both zones.
func TestNaiveDatetimeInServerZone(t *testing.T) {
	pacific := time.FixedZone("PDT", -7*3600)
	at := time.Date(2026, 9, 21, 18, 37, 5, 895620000, time.UTC).In(pacific)
	if got, want := naiveDatetime(at), "2026-09-21T11:37:05.895620-07:00"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
	whole := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).In(time.FixedZone("IST", 5*3600+1800))
	if got, want := naiveDatetime(whole), "2026-01-02T08:34:05+05:30"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
	// A zero-offset zone that is not UTC is aware, so "Z".
	london := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).In(time.FixedZone("GMT", 0))
	if got, want := naiveDatetime(london), "2026-01-02T03:04:05Z"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
