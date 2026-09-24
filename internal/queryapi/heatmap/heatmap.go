// Package heatmap is the Go port of GET /api/v1/heatmap
// (src/dev_health_ops/api/services/heatmap.py's build_heatmap_response,
// api/queries/heatmap.py's seven ClickHouse readers, and the identity/
// scope-filter helpers it calls: api/services/people_identity.py,
// api/utils/identity_aliases.py, api/services/filtering.py's
// scope_filter_for_metric/resolve_repo_filter_ids, and
// api/queries/scopes.py's resolve_repo_id/resolve_repo_ids/
// resolve_repo_ids_for_teams).
//
// SCOPE: all four HEATMAP_METRICS entries (review_wait_density,
// repo_touchpoints, hotspot_risk, active_hours) are ported, including the
// evidence sub-fetch each of the first, third and fourth carries and the
// genuine Python quirk in the hotspot_risk branch: it calls
// fetch_hotspot_evidence but assigns the result to "_" (heatmap.py's own
// build_heatmap_response), so the evidence it fetches is never attached
// to the response -- evidence stays null for hotspot_risk regardless of
// x/y. This port reproduces that exactly (see queries.go's
// fetchHotspotEvidence call site in BuildResponse): it is Python's real,
// shipped behaviour, not a bug to fix silently.
//
// DEDUP (class ruling, this service): repos (ReplacingMergeTree(last_synced),
// 000_raw_tables.sql), git_pull_requests (same engine/version column) and
// git_commits (same) are each read by the reference queries (api/queries/
// heatmap.py) with no FINAL or argMax dedup at all -- confirmed reading
// every one of the seven queries there. file_metrics_daily is
// ReplacingMergeTree(computed_at) since migration 096
// (src/dev_health_ops/migrations/clickhouse/096_daily_family_tables_
// replacing_merge_tree.py); the reference dedups it correctly via
// clickhouse_dedup.dedup_from's "ORDER BY computed_at DESC LIMIT 1 BY
// <key>" subquery (functionally equivalent to FINAL for a table whose
// sorting key already IS the reader key -- no divergence there), but this
// package's own class ruling is one dedup shape throughout ("never LIMIT
// 1 BY alone"), so queries.go reads file_metrics_daily FINAL too. Every
// repos join across all four metrics reads FINAL; the reference's own
// unfixed repos/git_pull_requests/git_commits reads are declared
// Python-plane defects (internal/goapiproof/restcorpus.go's
// heatmapDedupParity), citing the same class ticket already covering
// this exact mechanism (a ReplacingMergeTree(last_synced) raw-tables
// read next to repos, in a sibling REST route -- drilldown/prs) for the
// identical tables.
//
// DATETIME (class ruling, this service): git_pull_requests.created_at/
// first_review_at and git_commits.author_when are ClickHouse
// DateTime64(3, 'UTC'); Python's clickhouse_connect driver returns them
// naive (isoformat with no offset) inside the evidence dict rows, where
// this port's typed evidence rows (response.go) use time.Time,
// marshaling via encoding/json's RFC 3339 (explicit UTC offset) -- the
// same declared defect class every other DateTime64 column in this
// schema carries. Go is correct.
package heatmap

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the narrow ClickHouse read capability this package
// needs -- same shape/convention as quadrant.QueryClient,
// explain.QueryClient, drilldown's own package-local interface.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// ErrUnavailable is returned when BuildResponse is called with a nil client.
var ErrUnavailable = errors.New("heatmap: clickhouse client unavailable")

// queryTimeoutSecs/settingsMaxExecutionTime mirror explain/drilldown's own
// copies -- see explain.go's doc comment for why the trailing SETTINGS
// clause must carry a literal integer, never a bound parameter.
const queryTimeoutSecs = 30

func settingsMaxExecutionTime() string {
	return fmt.Sprintf("SETTINGS max_execution_time = %d", queryTimeoutSecs)
}

// weekdayLabels ports WEEKDAY_LABELS (services/heatmap.py:32).
var weekdayLabels = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}

// metricDefinition ports HeatmapMetric (services/heatmap.py:44-53).
type metricDefinition struct {
	Type   string
	Metric string
	Unit   string
	Scale  string
	XAxis  string
	YAxis  string
	XField string
	YField string
	Scope  string
}

// heatmapMetrics ports HEATMAP_METRICS (services/heatmap.py:56-95)
// verbatim, same declaration order.
var heatmapMetrics = []metricDefinition{
	{Type: "temporal_load", Metric: "review_wait_density", Unit: "hours", Scale: "linear", XAxis: "hour", YAxis: "weekday", XField: "hour", YField: "weekday", Scope: "repo"},
	{Type: "context_switch", Metric: "repo_touchpoints", Unit: "commits", Scale: "linear", XAxis: "day", YAxis: "repo", XField: "day", YField: "repo", Scope: "repo"},
	{Type: "risk", Metric: "hotspot_risk", Unit: "hotspot score", Scale: "log", XAxis: "week", YAxis: "file", XField: "week", YField: "file_key", Scope: "repo"},
	{Type: "individual", Metric: "active_hours", Unit: "commits", Scale: "linear", XAxis: "hour", YAxis: "weekday", XField: "hour", YField: "weekday", Scope: "developer"},
}

// metricFor ports _metric_for (services/heatmap.py:98-102).
func metricFor(typeValue, metricValue string) (metricDefinition, bool) {
	for _, m := range heatmapMetrics {
		if m.Type == typeValue && m.Metric == metricValue {
			return m, true
		}
	}
	return metricDefinition{}, false
}

// validScopeLevels ports ScopeFilter.level's Literal set (api/models/
// filters.py:16-18) -- the constructor-time validation
// `ScopeFilter(level=scope_type, ...)` performs inside build_heatmap_
// response's own try/except (services/heatmap.py:243-251), caught and
// reraised as 400 "Invalid scope filter".
var validScopeLevels = map[string]bool{
	"org": true, "team": true, "repo": true, "service": true, "developer": true,
}

// normalizeRangeDays ports _normalize_range_days (services/heatmap.py:
// 202-203): `max(1, min(int(range_days or 14), 180))`. Python's `or 14`
// substitutes ONLY when range_days is exactly the falsy int 0 -- a
// negative value passes through unchanged into the clamp below.
func normalizeRangeDays(rangeDays int) int {
	if rangeDays == 0 {
		rangeDays = 14
	}
	if rangeDays < 1 {
		return 1
	}
	if rangeDays > 180 {
		return 180
	}
	return rangeDays
}

// timeWindow ports time_window (services/filtering.py:78-92), restricted
// to the (start_day, end_day) pair build_heatmap_response actually reads
// -- same restriction quadrant.go's own copy of this Python function
// applies, for the identical reason (compare_start/compare_end are never
// read by this route either).
func timeWindow(rangeDays int, startDate, endDate *time.Time) (startDay, endDay time.Time) {
	endDay = time.Now().UTC().Truncate(24 * time.Hour)
	if endDate != nil {
		endDay = *endDate
	}
	endDay = endDay.AddDate(0, 0, 1)

	if startDate != nil {
		startDay = *startDate
		if !startDay.Before(endDay) {
			startDay = endDay.AddDate(0, 0, -1)
		}
		return startDay, endDay
	}
	return endDay.AddDate(0, 0, -rangeDays), endDay
}

// clampLimit ports the plain `min(max(limit, 1), 200)` clamp
// build_heatmap_response applies a second time at each evidence-fetch
// call site (services/heatmap.py:294,375,423) -- a no-op in practice
// given Params.Limit already comes in bounded to [1,200] by the route's
// own _bounded_limit_param(limit, 200) call (heatmap_route.go's
// boundedLimitParamHeatmap), ported anyway for a byte-faithful copy of
// the reference's own double clamp.
func clampLimit(limit, lo, hi int) int {
	if limit < lo {
		return lo
	}
	if limit > hi {
		return hi
	}
	return limit
}

// RequestError carries an HTTP status the way Python's HTTPException does
// -- same shape/purpose as quadrant.RequestError.
type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }

func badRequest(msg string) error { return &RequestError{Status: 400, Message: msg} }
func notFound(msg string) error   { return &RequestError{Status: 404, Message: msg} }

// AsRequestError extracts a *RequestError's status/message, or reports
// ok=false for any other error -- the route layer's 503 fallback.
func AsRequestError(err error) (*RequestError, bool) {
	var reqErr *RequestError
	if errors.As(err, &reqErr) {
		return reqErr, true
	}
	return nil, false
}

// Params is BuildResponse's input -- the already-validated/defaulted
// query params GET /api/v1/heatmap takes (main.py:569-585). The route
// file owns type-level validation (422) and the comparative-params
// reject (400); this package owns everything build_heatmap_response
// itself decides.
type Params struct {
	Type      string
	Metric    string
	ScopeType string // default "org"
	ScopeID   string
	RangeDays int
	StartDate *time.Time
	EndDate   *time.Time
	X         string // "" means Python's None
	Y         string // "" means Python's None
	Limit     int    // already run through boundedLimitParam(limit, 200) by the caller
}

// BuildResponse ports build_heatmap_response (services/heatmap.py:
// 224-297) plus the route-level x/y-or-None normalization (main.py's own
// `x=x or None, y=y or None`, folded into the X/Y=="" checks below).
func BuildResponse(ctx context.Context, client QueryClient, orgID string, params Params) (*Response, error) {
	if client == nil {
		return nil, ErrUnavailable
	}

	definition, ok := metricFor(params.Type, params.Metric)
	if !ok {
		return nil, notFound("Unknown heatmap metric")
	}

	if definition.Type == "individual" && params.ScopeType != "developer" {
		return nil, badRequest("Individual heatmaps require developer scope")
	}
	if params.ScopeType == "developer" && definition.Type != "individual" {
		return nil, badRequest("Developer scope is only supported for individual heatmaps")
	}
	if definition.Type == "individual" && params.ScopeID == "" {
		return nil, badRequest("Individual heatmaps require a person id")
	}
	// Unreachable given heatmapMetrics' own four entries (none pairs
	// XAxis=="person" with YAxis=="person") -- ported verbatim anyway,
	// matching Python's own dead branch (services/heatmap.py:262-265).
	if definition.XAxis == "person" && definition.YAxis == "person" {
		return nil, badRequest("Person comparisons are not supported")
	}

	scopeType := params.ScopeType
	if scopeType == "" {
		scopeType = "org"
	}
	if !validScopeLevels[scopeType] {
		return nil, badRequest("Invalid scope filter")
	}

	rangeDays := normalizeRangeDays(params.RangeDays)
	startDay, endDay := timeWindow(rangeDays, params.StartDate, params.EndDate)
	startTS := startDay
	endTS := endDay

	var identities []string
	if definition.Type == "individual" {
		var err error
		identities, err = resolveIdentityVariants(ctx, client, params.ScopeID, orgID)
		if err != nil {
			return nil, err
		}
		if len(identities) == 0 {
			return nil, notFound("Individual not found")
		}
	}

	var rows []metricRow
	var evidence any // nil (Python's None) unless a branch below sets it

	// One instant for the whole response: the branches below build several
	// statements from one scope, so they must resolve one team membership.
	asOf := time.Now().UTC()

	switch definition.Metric {
	case "review_wait_density":
		scopeFilterSQL, scopeBindings, err := scopeFilterForMetric(ctx, client, "repo", scopeType, []string{params.ScopeID}, nil, orgID, asOf)
		if err != nil {
			return nil, err
		}
		reviewRows, err := fetchReviewWaitDensity(ctx, client, startTS, endTS, scopeFilterSQL, scopeBindings, orgID)
		if err != nil {
			return nil, err
		}
		rows = reviewWaitDensityToMetricRows(reviewRows)

		if params.X != "" && params.Y != "" {
			hour, weekday, valid := parseHourWeekday(params.X, params.Y)
			if valid {
				limit := clampLimit(params.Limit, 1, 200)
				evidenceRows, err := fetchReviewWaitEvidence(ctx, client, startTS, endTS, weekday, hour, scopeFilterSQL, scopeBindings, limit, orgID)
				if err != nil {
					return nil, err
				}
				evidence = evidenceRows
			}
		}

	case "repo_touchpoints":
		scopeFilterSQL, scopeBindings, err := scopeFilterForMetric(ctx, client, "repo", scopeType, []string{params.ScopeID}, nil, orgID, asOf)
		if err != nil {
			return nil, err
		}
		touchRows, err := fetchRepoTouchpoints(ctx, client, startTS, endTS, scopeFilterSQL, scopeBindings, 20, orgID)
		if err != nil {
			return nil, err
		}
		rows = repoTouchpointsToMetricRows(touchRows)

	case "hotspot_risk":
		scopeFilterSQL, scopeBindings, err := scopeFilterForMetric(ctx, client, "repo", scopeType, []string{params.ScopeID}, nil, orgID, asOf)
		if err != nil {
			return nil, err
		}
		hotspotRows, err := fetchHotspotRisk(ctx, client, startDay, endDay, scopeFilterSQL, scopeBindings, 20, orgID)
		if err != nil {
			return nil, err
		}
		rows = hotspotRiskToMetricRows(hotspotRows)

		if params.X != "" && params.Y != "" {
			weekStart, weekEnd, valid := parseWeekRange(params.X)
			if valid {
				limit := clampLimit(params.Limit, 1, 200)
				// Python assigns this call's result to "_" -- the fetch
				// still runs (and its error, if any, still propagates),
				// but the rows it returns are discarded, never attached
				// to the response. See package doc comment.
				if _, err := fetchHotspotEvidence(ctx, client, weekStart, weekEnd, params.Y, scopeFilterSQL, scopeBindings, limit, orgID); err != nil {
					return nil, err
				}
			}
		}

	case "active_hours":
		activeRows, err := fetchIndividualActiveHours(ctx, client, startTS, endTS, identities, orgID)
		if err != nil {
			return nil, err
		}
		rows = individualActiveHoursToMetricRows(activeRows)

		if params.X != "" && params.Y != "" {
			hour, weekday, valid := parseHourWeekday(params.X, params.Y)
			if valid {
				limit := clampLimit(params.Limit, 1, 200)
				evidenceRows, err := fetchIndividualActiveEvidence(ctx, client, startTS, endTS, weekday, hour, identities, limit, orgID)
				if err != nil {
					return nil, err
				}
				evidence = evidenceRows
			}
		}

	default:
		rows = nil
	}

	xAxis := axisValues(rows, true, definition.XAxis)
	yAxis := axisValues(rows, false, definition.YAxis)
	cells := cellsFromRows(rows, definition.XAxis, definition.YAxis)

	return &Response{
		Axes:     Axes{X: xAxis, Y: yAxis},
		Cells:    cells,
		Legend:   Legend{Unit: definition.Unit, Scale: definition.Scale},
		Evidence: evidence,
	}, nil
}

// parseHourWeekday ports the try/except int(x)/WEEKDAY_LABELS.index(y)+1
// block shared by the review_wait_density and active_hours branches
// (services/heatmap.py:277-286, 407-414): valid is false (hour=weekday=-1
// in Python) on any parse failure, and the caller additionally checks the
// resulting range.
func parseHourWeekday(x, y string) (hour, weekday int, valid bool) {
	h, err := parseIntStrict(x)
	if err != nil {
		return 0, 0, false
	}
	idx := -1
	for i, label := range weekdayLabels {
		if label == y {
			idx = i
			break
		}
	}
	if idx == -1 {
		return 0, 0, false
	}
	weekday = idx + 1
	hour = h
	if hour < 0 || hour > 23 || weekday < 1 || weekday > 7 {
		return 0, 0, false
	}
	return hour, weekday, true
}

// parseWeekRange ports the hotspot_risk branch's try/except
// date.fromisoformat(x) block (services/heatmap.py:317-323).
func parseWeekRange(x string) (weekStart, weekEnd time.Time, valid bool) {
	start, err := time.Parse("2006-01-02", x)
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	return start, start.AddDate(0, 0, 7), true
}

// parseIntStrict ports Python's int(str) for the plain-digit hour strings
// this route's x query param carries in practice (including a
// zero-padded "03" -- this route's own x-axis formatting produces
// exactly that shape, and strconv.Atoi accepts a leading zero as decimal,
// same as Python's int()).
func parseIntStrict(s string) (int, error) {
	return strconv.Atoi(s)
}
