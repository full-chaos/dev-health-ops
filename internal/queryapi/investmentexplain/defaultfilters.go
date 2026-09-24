package investmentexplain

// DefaultMetricFilterForCacheKey is the JSON shape of Python's
// MetricFilter().model_dump(mode="json") -- the value FastAPI/Pydantic
// materializes for a request whose "filters" key is entirely absent
// (MetricFilter's own default_factory on every nested field). Confirmed
// against a live `uv run python3 -c 'MetricFilter().model_dump(mode="json")'`
// run, not assumed from the field declarations alone.
//
// A caller with no request-level filters object (the Go REST route's
// investmentExplainRequestBody.Filters is nil when the JSON body omits
// "filters") must use THIS value for ExplainInvestmentMixOptions.
// FiltersForCacheKey, not nil -- a nil FiltersForCacheKey serializes to
// the bare JSON literal `null` (pythonparity.MarshalPythonJSONSorted(nil)),
// which is a different cache key than Python's for the same "no filters
// supplied" request, and also a different key than Python would compute
// for a request that explicitly sent `{"filters": null}` (Pydantic
// rejects that outright as a validation error -- out of scope here, see
// investment_explain_route.go's own documented narrower-than-full-
// Pydantic-validation boundary). Caught by codex round 1 (P1).
func DefaultMetricFilterForCacheKey() map[string]any {
	return map[string]any{
		"time": map[string]any{
			"range_days":   14,
			"compare_days": 14,
			"start_date":   nil,
			"end_date":     nil,
		},
		"scope": map[string]any{
			"level": "org",
			"ids":   []any{},
		},
		"who": map[string]any{
			"developers": nil,
			"roles":      nil,
		},
		"what": map[string]any{
			"repos":     nil,
			"services":  nil,
			"artifacts": nil,
		},
		"why": map[string]any{
			"work_category": nil,
			"issue_type":    nil,
			"initiative":    nil,
		},
		"how": map[string]any{
			"flow_stage": nil,
			"blocked":    nil,
			"wip_state":  nil,
		},
	}
}
