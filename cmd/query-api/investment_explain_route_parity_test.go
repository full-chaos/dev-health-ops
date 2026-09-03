package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
)

// unusedQueryClient panics if Query is ever called -- used where a
// *investmentexplain.Reader is required by a function signature but the
// specific request under test (empty scope, no repos) resolves to zero
// ClickHouse calls, so any call at all is itself a test failure.
type unusedQueryClient struct{}

func (unusedQueryClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	panic("unusedQueryClient.Query: no ClickHouse call expected for this request shape")
}

// emptyRowScanner and emptyRowsQueryClient answer every ClickHouse query
// with zero rows -- used where a request shape genuinely does reach the
// full ExplainInvestmentMix flow (unlike unusedQueryClient's request
// shapes above) but this test only cares about the CODE PATH taken, not
// the resulting data.
type emptyRowScanner struct{}

func (emptyRowScanner) Next() bool        { return false }
func (emptyRowScanner) Scan(...any) error { return nil }
func (emptyRowScanner) Err() error        { return nil }
func (emptyRowScanner) Close() error      { return nil }

type emptyRowsQueryClient struct{}

func (emptyRowsQueryClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowScanner{}, nil
}

// TestIntFromAnyAcceptsNumericString regresses codex round 1's P1: a
// JSON body with `"range_days": "2"` (a numeric STRING, not a bare
// number) is a normal Pydantic-coerced request
// (TimeFilter(range_days="2") resolves to 2, confirmed against a live
// `uv run python3` construction of the real model, not assumed) -- a
// first draft's intFromAny only handled float64 and silently fell back
// to the 14-day default for this exact shape.
func TestIntFromAnyAcceptsNumericString(t *testing.T) {
	cases := []struct {
		name     string
		value    any
		fallback int
		want     int
	}{
		{"float64", float64(2), 14, 2},
		{"numeric string", "2", 14, 2},
		{"numeric string with whitespace", " 2 ", 14, 2},
		{"non-numeric string falls back", "abc", 14, 14},
		{"nil falls back", nil, 14, 14},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := intFromAny(tc.value, tc.fallback)
			if got != tc.want {
				t.Fatalf("intFromAny(%#v, %d) = %d, want %d", tc.value, tc.fallback, got, tc.want)
			}
		})
	}
}

// TestTimeWindowHonorsStringRangeDays is the end-to-end version of the
// above: a request body shaped like `{"time": {"range_days": "2"}}` must
// produce the SAME start/end window as `{"time": {"range_days": 2}}`,
// not the 14-day default.
func TestTimeWindowHonorsStringRangeDays(t *testing.T) {
	numeric := map[string]any{"time": map[string]any{"range_days": float64(2), "end_date": "2026-09-01"}}
	stringly := map[string]any{"time": map[string]any{"range_days": "2", "end_date": "2026-09-01"}}

	numStart, numEnd := timeWindow(numeric)
	strStart, strEnd := timeWindow(stringly)

	if !numStart.Equal(strStart) || !numEnd.Equal(strEnd) {
		t.Fatalf("timeWindow diverges on numeric vs string range_days: numeric=(%s,%s) string=(%s,%s)",
			numStart, numEnd, strStart, strEnd)
	}
	wantStart := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	if !strStart.Equal(wantStart) {
		t.Fatalf("start = %s, want %s (2-day range ending 2026-09-01)", strStart, wantStart)
	}
}

// TestBuildExplainOptionsDefaultsFiltersForCacheKeyWhenOmitted regresses
// codex round 1's P1: an omitted "filters" key must hash as Python's
// fully-materialized MetricFilter() default, not the bare JSON literal
// `null` a nil FiltersForCacheKey would produce.
func TestBuildExplainOptionsDefaultsFiltersForCacheKeyWhenOmitted(t *testing.T) {
	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	opts, err := buildExplainOptions(context.Background(), reader, "org-1", investmentExplainRequestBody{Filters: nil}, "auto", false)
	if err != nil {
		t.Fatalf("buildExplainOptions: %v", err)
	}

	got, ok := opts.FiltersForCacheKey.(map[string]any)
	if !ok {
		t.Fatalf("FiltersForCacheKey = %#v (%T), want the default MetricFilter map", opts.FiltersForCacheKey, opts.FiltersForCacheKey)
	}
	want := investmentexplain.DefaultMetricFilterForCacheKey()

	gotKey, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{Filters: got, OrgID: "org-1"})
	if err != nil {
		t.Fatalf("compute cache key (got): %v", err)
	}
	wantKey, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{Filters: want, OrgID: "org-1"})
	if err != nil {
		t.Fatalf("compute cache key (want): %v", err)
	}
	if gotKey != wantKey {
		t.Fatalf("cache key for omitted filters = %s, want %s (DefaultMetricFilterForCacheKey)", gotKey, wantKey)
	}

	// A nil FiltersForCacheKey (the pre-fix behavior) must NOT match --
	// this is the discriminating half of the proof, not just a
	// self-consistency check against the same helper the fix itself uses.
	nullKey, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{Filters: nil, OrgID: "org-1"})
	if err != nil {
		t.Fatalf("compute cache key (null): %v", err)
	}
	if gotKey == nullKey {
		t.Fatalf("cache key for omitted filters unexpectedly matches the null-filters key -- the fix is not discriminating")
	}
}

// TestBuildExplainOptionsDefaultsFiltersForCacheKeyWhenExplicitlyEmpty
// regresses codex round 2's P1: a request that sends "filters" as an
// EXPLICIT empty object ({"filters": {}}, body.Filters a non-nil empty
// map) must ALSO hash against Python's fully-materialized default, not
// the literal `{}` -- Pydantic's default_factory fires on every nested
// field regardless of whether the outer "filters" key was present-but-
// empty or absent entirely. The round 1 fix only checked
// body.Filters == nil, which is false for this shape.
func TestBuildExplainOptionsDefaultsFiltersForCacheKeyWhenExplicitlyEmpty(t *testing.T) {
	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	opts, err := buildExplainOptions(context.Background(), reader, "org-1", investmentExplainRequestBody{Filters: map[string]any{}}, "auto", false)
	if err != nil {
		t.Fatalf("buildExplainOptions: %v", err)
	}

	gotKey, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{Filters: opts.FiltersForCacheKey, OrgID: "org-1"})
	if err != nil {
		t.Fatalf("compute cache key (got): %v", err)
	}
	wantKey, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{
		Filters: investmentexplain.DefaultMetricFilterForCacheKey(), OrgID: "org-1",
	})
	if err != nil {
		t.Fatalf("compute cache key (want): %v", err)
	}
	if gotKey != wantKey {
		t.Fatalf("cache key for explicit-empty filters = %s, want %s (DefaultMetricFilterForCacheKey)", gotKey, wantKey)
	}

	// The literal {} (the pre-fix behavior for this shape) must NOT
	// match -- the discriminating half of the proof.
	literalEmptyKey, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{Filters: map[string]any{}, OrgID: "org-1"})
	if err != nil {
		t.Fatalf("compute cache key (literal empty): %v", err)
	}
	if gotKey == literalEmptyKey {
		t.Fatalf("cache key for explicit-empty filters unexpectedly matches the literal-{} key -- the fix is not discriminating")
	}
}

// TestWriteKeepAliveJSONErrorBodyMatchesPythonByteForByte regresses
// codex round 1's P1: Go's error-path JSON bodies must match Python's
// exact `json.dumps({"error": ..., "detail": ...})` bytes -- key order
// (error before detail) and default ", "/": " spacing -- not
// encoding/json's alphabetically-sorted, compact map marshal. The exact
// expected strings were confirmed against a live
// `python3 -c 'json.dumps(...)'` run, not hand-typed from memory.
func TestWriteKeepAliveJSONErrorBodyMatchesPythonByteForByte(t *testing.T) {
	rec := httptest.NewRecorder()
	writeKeepAliveJSON(context.Background(), rec, func(context.Context) ([]byte, error) {
		return nil, errBoom
	})

	got := rec.Body.String()
	want := `{"error": "Streaming error", "detail": "An internal error has occurred."}` +
		`{"error": "Streaming error", "detail": "An internal streaming error occurred."}`
	if got != want {
		t.Fatalf("error body = %q, want %q", got, want)
	}
}

type boomError struct{}

func (boomError) Error() string { return "boom" }

var errBoom = boomError{}

// TestInvestmentExplainWorkHandlerRejectsUnsupportedProviderPreStream
// regresses codex round 1's #5, ruled by team-lead as a required fix, not
// a RISK-NOTES-only item: a request for a provider Python genuinely
// supports (anthropic/gemini/qwen/ollama/lmstudio) but this Go port
// cannot construct a client for must get a plain 501 BEFORE any
// streaming begins -- not the normal streamed llm_unavailable body --
// so the Python REST forwarder's own non-200 fallback routes the
// request to Python's real completion instead of a wrong Go answer.
func TestInvestmentExplainWorkHandlerRejectsUnsupportedProviderPreStream(t *testing.T) {
	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	handler := newInvestmentExplainWorkHandler(reader, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain?llm_provider=anthropic", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
	body, _ := io.ReadAll(rec.Body)
	want := `{"error": "unsupported_provider"}`
	if string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

// TestInvestmentExplainWorkHandlerSupportedProviderStillStreams proves
// the discriminating half: a provider this port DOES implement (or an
// unresolvable one, which is the ordinary llm_unavailable case, not this
// one) must NOT hit the pre-stream 501 -- it falls through to the normal
// streaming path. Uses the mock provider, which resolves and streams
// fast enough that no ClickHouse call ever fires before the LLM-
// availability gate inside ExplainInvestmentMix short-circuits.
func TestInvestmentExplainWorkHandlerSupportedProviderStillStreams(t *testing.T) {
	reader, err := investmentexplain.NewReader(emptyRowsQueryClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	handler := newInvestmentExplainWorkHandler(reader, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain?llm_provider=mock", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code == http.StatusNotImplemented {
		t.Fatalf("mock provider incorrectly hit the pre-stream 501 path")
	}
}
