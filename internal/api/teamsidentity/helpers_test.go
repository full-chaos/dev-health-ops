package teamsidentity

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
// f"Unknown team_id(s): {sorted(set(missing))}" for the common case. The
// apostrophe-escaping rule (Python's repr() switches quote style for a
// value holding a single quote and no double quote) is a KNOWN, documented
// gap pending a shared pythonparity repr encoder -- see
// unknownTeamIDsDetail's doc comment.
func TestUnknownTeamIDsDetailSortsAndQuotes(t *testing.T) {
	got := unknownTeamIDsDetail([]string{"b", "a", "b"})
	want := `Unknown team_id(s): ['a', 'b']`
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// TestSortedKeysIsDeterministic proves sortedKeys (used to order the
// provider-conflict check, r1 finding #5) is alphabetical and stable,
// independent of Go's randomized map iteration.
func TestSortedKeysIsDeterministic(t *testing.T) {
	m := map[string][]string{"z-provider": {"v"}, "a-provider": {"v"}, "m-provider": {"v"}}
	want := []string{"a-provider", "m-provider", "z-provider"}
	for i := 0; i < 5; i++ {
		got := sortedKeys(m)
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	}
}

// TestPythonProviderIdentitiesJSONMatchesJSONDumpsSpacing pins
// pythonProviderIdentitiesJSON (built on the shared pyjson.Dumps, not a
// route-local encoder) against Python's bare `json.dumps` default
// separators (", ", ": "), captured live -- pyjson.Marshal's compact
// Starlette-response form omits both spaces.
func TestPythonProviderIdentitiesJSONMatchesJSONDumpsSpacing(t *testing.T) {
	cases := []struct {
		in   map[string][]string
		want string
	}{
		{in: map[string][]string{}, want: `{}`},
		{in: map[string][]string{"order-provider-0": {"order-value-0"}}, want: `{"order-provider-0": ["order-value-0"]}`},
		{in: map[string][]string{"a": {}}, want: `{"a": []}`},
		{in: map[string][]string{"b": {"x", "y"}, "a": {"z"}}, want: `{"a": ["z"], "b": ["x", "y"]}`},
	}
	for _, tc := range cases {
		got, err := pythonProviderIdentitiesJSON(tc.in)
		if err != nil {
			t.Fatalf("pythonProviderIdentitiesJSON(%v): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("pythonProviderIdentitiesJSON(%v) = %s, want %s", tc.in, got, tc.want)
		}
	}
}
