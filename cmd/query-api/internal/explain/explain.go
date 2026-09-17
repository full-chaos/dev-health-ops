// Package explain is the Go port of the FastAPI "explain" REST pair
// (api/main.py's GET+POST /api/v1/explain, backed by
// api/services/explain.py's build_explain_response and the query helpers
// it calls: api/queries/metrics.py's fetch_metric_value and
// api/queries/explain.py's fetch_metric_contributors/
// fetch_metric_driver_delta).
//
// CLIENT CONVENTION: a package-local QueryClient interface over
// github.com/full-chaos/dev-health-go/clickhouse's Binding/RowScanner,
// matching drilldown/investmentexplain/quadrant's own convention in this
// binary (repeat, don't couple, for an interface this narrow) rather than
// importing one of those packages' own Reader.
//
// CACHE (a declared, documented no-op divergence -- see RISK-NOTES):
// Python wraps build_explain_response in EXPLAIN_CACHE, an epoch-scoped,
// 120s in-process TTLCache (api/main.py:162, api/services/filtering.py's
// epoch_cache_key). It is a pure perf/staleness-window optimization:
// cache.get is validated back through ExplainResponse.model_validate and
// cache.set stores response.model_dump(mode="json"), so a cache hit and a
// live compute return the SAME data for the SAME underlying rows -- the
// epoch key (bumped by the Go finalize job on every write) means a hit is
// never more than 120s stale, and never serves a DIFFERENT shape. This
// port carries no equivalent cache: query-api has no shared TTL-cache
// infrastructure anywhere yet (grepped cmd/query-api/internal -- none),
// the same class of documented gap every sibling REST route in this
// binary already carries for rate limiting. No response-content change,
// latency only.
package explain

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the narrow ClickHouse read capability this package needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// ErrUnavailable is returned when a Reader is constructed without a client.
var ErrUnavailable = errors.New("explain: clickhouse client unavailable")

// queryTimeoutSecs matches drilldown/investmentexplain/quadrant's own copy
// of this constant -- see drilldown.go's doc comment for why the trailing
// SETTINGS clause must carry a literal integer, never a bound parameter
// (ClickHouse 26.6.1.1193, the digest-pinned integration-test image,
// fails to parse a bound parameter inside SETTINGS; 26.7.5.10 does not).
const queryTimeoutSecs = 30

func settingsMaxExecutionTime() string {
	return fmt.Sprintf("SETTINGS max_execution_time = %d", queryTimeoutSecs)
}

// Reader reads the ClickHouse data the explain route needs.
type Reader struct {
	client QueryClient
}

// NewReader returns a Reader over the given query client.
func NewReader(client QueryClient) (*Reader, error) {
	if client == nil {
		return nil, ErrUnavailable
	}
	return &Reader{client: client}, nil
}

// Params is the already-resolved request shape both GET and POST
// /api/v1/explain reduce to before calling BuildExplainResponse -- the
// route file does the HTTP-shape-specific work (query-param defaults for
// GET, JSON-body parsing for POST) and always hands this package a
// fully-resolved value, matching drilldown.PRParams/investmentexplain's
// own division of labor between the route file and this package.
type Params struct {
	// Metric is the raw request value verbatim -- NOT validated against
	// metricConfigs' key set (api/services/explain.py:146's own
	// `_METRIC_CONFIG.get(metric, _METRIC_CONFIG["cycle_time"])`: an
	// unrecognised metric string silently borrows cycle_time's table/
	// column/label/unit/etc, but the RESPONSE's own "metric" field still
	// echoes this original string back, not "cycle_time" -- confirmed by
	// that same line only substituting the CONFIG, never the request
	// value used to build the response below).
	Metric string
	// StartDay/EndDay/CompareStart/CompareEnd are the four values
	// time_window (api/services/filtering.py:78-92) returns -- the route
	// file owns that computation (explainTimeWindow, cmd/query-api's own
	// route file: the package-level `timeWindow` investment_explain_route.go
	// and drilldown_prs_route.go already share only returns two of these
	// four values, since neither of those routes needs a comparison
	// window).
	StartDay     time.Time
	EndDay       time.Time
	CompareStart time.Time
	CompareEnd   time.Time
	// ScopeLevel/ScopeIDs/WhatRepos are filters.scope.level, filters.scope.ids
	// and filters.what.repos verbatim -- fed to scopeFilterForMetric.
	ScopeLevel string
	ScopeIDs   []string
	WhatRepos  []string
}

// Contributor ports api/models/schemas.py's Contributor model.
type Contributor struct {
	ID           string  `json:"id"`
	Label        string  `json:"label"`
	Value        float64 `json:"value"`
	DeltaPct     float64 `json:"delta_pct"`
	EvidenceLink string  `json:"evidence_link"`
	DisplayName  *string `json:"display_name"`
}

// Response ports api/models/schemas.py's ExplainResponse model.
type Response struct {
	Metric         string            `json:"metric"`
	Label          string            `json:"label"`
	Unit           string            `json:"unit"`
	Value          float64           `json:"value"`
	DeltaPct       float64           `json:"delta_pct"`
	Drivers        []Contributor     `json:"drivers"`
	Contributors   []Contributor     `json:"contributors"`
	DrilldownLinks map[string]string `json:"drilldown_links"`
}

// safeFloat ports api/utils/numeric.py's safe_float for an already-float64
// Go value: NaN/Inf degrade to 0.0, matching math.isfinite's own check.
// The None/TypeError branch (a value that isn't even numeric) has no Go
// equivalent to port -- every caller in this package already has a
// float64 (or a nullable *float64 already denulled to 0.0 by
// floatOrZero) by the time safeFloat runs, matching where Python's own
// safe_float(None) collapses to the SAME 0.0 default this package's
// floatOrZero already provides.
func safeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0.0
	}
	return v
}

// floatOrZero denulls a nullable scan destination -- the Go-side
// equivalent of Python's `float(value or 0.0)` / `safe_float(row.get(...))`
// None-handling for a ClickHouse aggregate that can legitimately return
// NULL (avg()/sum() over an all-NULL group, or an unmatched LEFT JOIN
// side).
func floatOrZero(v *float64) float64 {
	if v == nil {
		return 0.0
	}
	return *v
}

// safeTransform ports api/utils/numeric.py's safe_transform.
func safeTransform(transform func(float64) float64, value float64) float64 {
	return safeFloat(transform(value))
}

// deltaPct ports api/utils/numeric.py's delta_pct.
func deltaPct(current, previous float64) float64 {
	if previous == 0 {
		return 0.0
	}
	return (current - previous) / previous * 100.0
}

// primaryScopeID ports explain.py's _primary_scope_id.
func primaryScopeID(scopeIDs []string) string {
	if len(scopeIDs) > 0 {
		return scopeIDs[0]
	}
	return ""
}

// shortToken ports explain.py's _short_token -- the controlled fallback
// label for an id resolveScopeDisplayNames could not resolve to a human
// name (A8: never a bare UUID as a label).
func shortToken(scopeID string) string {
	token := strings.ReplaceAll(scopeID, "-", "")
	if len(token) > 8 {
		token = token[:8]
	}
	if token == "" {
		return "Unknown"
	}
	return "#" + token
}
