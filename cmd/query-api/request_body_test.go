package main

import (
	"math"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// TestValidatedIntsKeepTheirValue pins what a route uses after validation:
// an int FastAPI accepts keeps its exact value through legacyJSON (2^53+1
// is not rounded), an int beyond Go's int saturates instead of becoming 0,
// and a midnight unix timestamp given as an int is that date.
func TestValidatedIntsKeepTheirValue(t *testing.T) {
	body, _, detail := decodeRequestBody([]any{"body"}, []byte(`{"filters": {"time": {"range_days": 9007199254740993, "start_date": 86400}}}`))
	if detail != nil {
		t.Fatalf("decode: %+v", detail)
	}
	filters, _ := legacyJSON(body).(map[string]any)["filters"].(map[string]any)
	timeFilter, _ := filters["time"].(map[string]any)
	if got := intFromAny(timeFilter["range_days"], 14); got != 9007199254740993 {
		t.Errorf("range_days = %d, want 9007199254740993", got)
	}
	if date, ok := dateFromAny(timeFilter["start_date"]); !ok || date.Format("2006-01-02") != "1970-01-02" {
		t.Errorf("start_date = %v %v, want 1970-01-02", date, ok)
	}
	for text, want := range map[string]int{`9223372036854775808`: math.MaxInt, `-9223372036854775809`: math.MinInt, `12`: 12, `"7"`: 7} {
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatal(err)
		}
		if got, detail := coerceIntBodyField([]any{"body", "top_n_repos"}, value); detail != nil || got != want {
			t.Errorf("%s: %d %+v, want %d", text, got, detail, want)
		}
		if got := intFromAny(legacyJSON(value), 14); got != want {
			t.Errorf("intFromAny(%s) = %d, want %d", text, got, want)
		}
	}
}

// TestValidatedIntsRenderAsPythonIntsInTheCacheKey pins the investment
// explain cache key over a validated body: its ints are Python ints in
// the key's json.dumps text (7, not 7.0), a huge int stays exact, and the
// key builds without error.
func TestValidatedIntsRenderAsPythonIntsInTheCacheKey(t *testing.T) {
	body, _, detail := decodeRequestBody([]any{"body"}, []byte(`{"filters": {"time": {"range_days": 7, "compare_days": 123456789012345678901234567890}}}`))
	if detail != nil {
		t.Fatalf("decode: %+v", detail)
	}
	filters := legacyJSON(body).(map[string]any)["filters"]
	text, err := pythonparity.MarshalPythonJSONSorted(filters)
	if err != nil {
		t.Fatalf("cache key JSON: %v", err)
	}
	if want := `{"time": {"compare_days": 123456789012345678901234567890, "range_days": 7}}`; string(text) != want {
		t.Errorf("cache key JSON = %s, want %s", text, want)
	}
	if _, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{Filters: filters.(map[string]any), OrgID: "org-1"}); err != nil {
		t.Errorf("ComputeCacheKey: %v", err)
	}
}
