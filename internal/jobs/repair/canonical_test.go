package repair

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// The expected values below were produced by running the Python
// _canonical_json and _repair_id on the same inputs.
func TestCanonicalJSON_MatchesPython(t *testing.T) {
	for _, c := range []struct{ input, want string }{
		{`{"b": 2, "a": {"z": 1.5, "y": "é"}}`, `{"a":{"y":"@u00e9","z":1.5},"b":2}`},
		{`{"n": 1e-7, "m": 1e16, "k": 100.0, "j": 1.0, "i": -0.0, "h": 123456789012345678}`, `{"h":123456789012345678,"i":-0.0,"j":1.0,"k":100.0,"m":1e+16,"n":1e-07}`},
		{`{"arr": [3, 1, {"b": null, "a": true}], "s": "@u2028 \"q\" \\ /"}`, `{"arr":[3,1,{"a":true,"b":null}],"s":"@u2028 \"q\" \\ /"}`},
		{`{"unicode": "日本語😀", "nested": {"": 1, "A": 2, "a": 3}}`, `{"nested":{"":1,"A":2,"a":3},"unicode":"@u65e5@u672c@u8a9e@ud83d@ude00"}`},
		{`{"exp": 1E5, "neg": -1e-10, "big": 1.7976931348623157e308}`, `{"big":1.7976931348623157e+308,"exp":100000.0,"neg":-1e-10}`},
	} {
		c.input = strings.ReplaceAll(c.input, "@u", "\\u")
		c.want = strings.ReplaceAll(c.want, "@u", "\\u")
		decoder := json.NewDecoder(strings.NewReader(c.input))
		decoder.UseNumber()
		var value map[string]any
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		got, err := canonicalJSON(value)
		if err != nil || got != c.want {
			t.Errorf("%s\n got %s (%v)\nwant %s", c.input, got, err, c.want)
		}
	}
}

func TestCanonicalJSON_RefusesAnIntegerBeyondSixtyFourBits(t *testing.T) {
	decoder := json.NewDecoder(strings.NewReader(`{"n": 9223372036854775808}`))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	if _, err := canonicalJSON(value); err == nil {
		t.Fatal("an integer beyond 64 bits must be refused, not rounded")
	}
}

func TestRepairID_MatchesPython(t *testing.T) {
	for _, c := range []struct {
		execution, state, resolution, want string
		attempt                            int
	}{
		{"11111111-1111-1111-1111-111111111111", "ambiguous", "retry_safe", "3a5dbf0b-0e16-5ad1-8a87-49e707d51581", 1},
		{"22222222-2222-2222-2222-222222222222", "executing", "confirm_succeeded", "44b31dc0-6518-5b0b-9ab3-5d15abffb990", 7},
		{"33333333-3333-3333-3333-333333333333", "ambiguous", "retry_safe", "5e2f6b5d-4b1d-5494-bd8a-acbce157e1a7", 12},
	} {
		got, err := repairID(uuid.MustParse(c.execution), MetricRequest{ExpectedState: c.state, ExpectedAttemptCount: c.attempt, Resolution: c.resolution})
		if err != nil || got.String() != c.want {
			t.Errorf("%s/%s/%d/%s = %s (%v), want %s", c.execution, c.state, c.attempt, c.resolution, got, err, c.want)
		}
	}
}
