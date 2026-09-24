package teamsidentity

import (
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestNaiveISOTime(t *testing.T) {
	whole := time.Date(2026, 9, 1, 15, 0, 0, 0, time.UTC)
	if got := naiveISOTime(whole); got != "2026-09-01T15:00:00" {
		t.Errorf("whole seconds = %q", got)
	}
	// isoformat keeps all six digits, trailing zeros included.
	frac := time.Date(2026, 9, 1, 10, 0, 0, 123400000, time.UTC)
	if got := naiveISOTime(frac); got != "2026-09-01T10:00:00.123400" {
		t.Errorf("fractional = %q", got)
	}
}

func TestLoadsJSONValue(t *testing.T) {
	if loadsJSONValue("") != nil {
		t.Error("empty string must be nil")
	}
	if got := loadsJSONValue("{bad"); got != "{bad" {
		t.Errorf("invalid JSON must stay raw, got %v", got)
	}
	if got := loadsJSONValue(`"x"`); got != "x" {
		t.Errorf("valid JSON string = %v", got)
	}
}

func TestJSONListValue(t *testing.T) {
	cases := []struct {
		in   pyjson.Value
		want []string
	}{
		{[]pyjson.Value{"a", nil, "b"}, []string{"a", "b"}},
		{`["x","y"]`, []string{"x", "y"}},
		{`"[\"x\"]"`, []string{}},
		{"plain", []string{}},
		{nil, []string{}},
		{pyjson.NewObject(), []string{}},
	}
	for _, c := range cases {
		if got := jsonListValue(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("jsonListValue(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestTruthyAndStrHelpers(t *testing.T) {
	for _, falsy := range []pyjson.Value{nil, false, "", []pyjson.Value{}, pyjson.NewObject()} {
		if truthy(falsy) {
			t.Errorf("%v must be falsy", falsy)
		}
	}
	if !truthy("x") || !truthy(true) || !truthy([]pyjson.Value{nil}) {
		t.Error("non-empty values must be truthy")
	}
	if got := firstTruthyString("", "fb"); got != "fb" {
		t.Errorf("firstTruthyString falls back, got %q", got)
	}
	if got := firstTruthyString("a", "fb"); got != "a" {
		t.Errorf("firstTruthyString keeps first, got %q", got)
	}
	if strOrEmpty(nil) != "" || strOrEmpty("v") != "v" {
		t.Error("strOrEmpty")
	}
}

func TestPythonIntOrZero(t *testing.T) {
	if v, err := pythonIntOrZero(nil); err != nil || v != 0 {
		t.Errorf("nil = %d, %v", v, err)
	}
	if v, err := pythonIntOrZero(" 7 "); err != nil || v != 7 {
		t.Errorf("padded string = %d, %v", v, err)
	}
	if _, err := pythonIntOrZero("x"); err == nil {
		t.Error("unparseable string must fail")
	}
	if _, err := pythonIntOrZero([]pyjson.Value{"a"}); err == nil {
		t.Error("list must fail")
	}
	if v, err := pythonIntOrZero(true); err != nil || v != 1 {
		t.Errorf("true = %d, %v", v, err)
	}
}

func TestDatetimeColumn(t *testing.T) {
	if got, err := datetimeColumn(nil); err != nil || got != nil {
		t.Errorf("nil = %v, %v", got, err)
	}
	aware, err := datetimeColumn("2026-09-01 00:00:00+00:00")
	if err != nil || aware == nil || !aware.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("aware = %v, %v", aware, err)
	}
	naive, err := datetimeColumn("2026-08-01 00:00:00.5")
	if err != nil || naive == nil || naive.Nanosecond() != 500000000 {
		t.Errorf("naive = %v, %v", naive, err)
	}
	if _, err := datetimeColumn("yesterday"); err == nil {
		t.Error("garbage must fail")
	}
	if _, err := datetimeColumn(int64(3)); err == nil {
		t.Error("non-string must fail")
	}
}

func TestMembershipFacets(t *testing.T) {
	row := pyjson.NewObject()
	row.Set("member_id", "alice")
	row.Set("raw_provider_user_id", "")
	row.Set("raw_email", "a@x")
	got := sortedKeysOfSet(membershipFacets(row))
	if !reflect.DeepEqual(got, []string{"a@x", "alice"}) {
		t.Errorf("facets = %v", got)
	}
}
