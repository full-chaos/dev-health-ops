// Package recommendations serves the GraphQL `recommendations` field
// (CHAOS-7065): the latest persisted, rule-based recommendations for one
// team within a lookback window.
//
// This is a straight port of resolve_recommendations
// (api/graphql/resolvers/recommendations.py) -- the SQL and its two-stage
// argMax dedup are already proven in Go against a real ClickHouse
// container by internal/queryapi/home's own fetchRecommendationSignals
// (queries_signals.go), which reads the SAME recommendations_daily table
// with the SAME dedup shape for HOME's signals panel. This package's own
// query differs only in its WHERE clause (one team, not team_id IN (...))
// and its result shape (every fired rule in the window, not a top-10
// severity-ranked slice) -- the field GraphQL exposes is the engine's
// own answer, not a derived summary.
package recommendations

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every sibling operation package
// declares independently (home.QueryClient, quadrant.QueryClient).
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// recommendationsSQL ports _RECOMMENDATIONS_SQL
// (api/graphql/resolvers/recommendations.py) verbatim: the inner argMax
// collapses re-runs of the SAME window_end by computed_at; the outer
// argMax then keeps only the LATEST window_end (as-of day) per
// (org, team, rule), so a rule that later stops firing is superseded by
// its own tombstone rather than lingering (CHAOS-2373). Final HAVING
// keeps only currently-fired rules.
const recommendationsSQL = `
SELECT
    team_id,
    org_id,
    rule_id,
    argMax(latest_fired,             window_end) AS latest_fired,
    argMax(latest_severity,          window_end) AS latest_severity,
    argMax(latest_title,             window_end) AS latest_title,
    argMax(latest_rationale,         window_end) AS latest_rationale,
    argMax(latest_success_criterion, window_end) AS latest_success_criterion,
    argMax(latest_evidence_json,     window_end) AS latest_evidence_json,
    argMax(latest_window_start,      window_end) AS latest_window_start,
    max(window_end)                              AS latest_window_end,
    argMax(latest_computed_at,       window_end) AS latest_computed_at
FROM (
    SELECT
        team_id,
        org_id,
        rule_id,
        window_end,
        argMax(fired,               computed_at) AS latest_fired,
        argMax(severity,            computed_at) AS latest_severity,
        argMax(title,               computed_at) AS latest_title,
        argMax(rationale,           computed_at) AS latest_rationale,
        argMax(success_criterion,   computed_at) AS latest_success_criterion,
        argMax(evidence_json,       computed_at) AS latest_evidence_json,
        argMax(window_start,        computed_at) AS latest_window_start,
        max(computed_at)                         AS latest_computed_at
    FROM recommendations_daily
    WHERE team_id  = {team_id:String}
      AND org_id   = {org_id:String}
      AND window_end >= {window_start:Date}
      AND window_end <= {window_end:Date}
    GROUP BY org_id, team_id, rule_id, window_end
)
GROUP BY org_id, team_id, rule_id
HAVING latest_fired = true
ORDER BY latest_window_end DESC, rule_id
`

// windowToDates ports _window_to_dates (resolvers/recommendations.py)
// exactly. window_end is "today + 1" -- the scheduled writer persists
// each run's rows at window_end == as_of_day + 1 (queries_signals.go's
// own doc comment notes the same convention), so capping the read at
// today+1 includes today's finalized recommendations same-day rather
// than leaving them one finalize-day stale (CHAOS-2373). window_start is
// anchored to today (not the bumped cap), so the lookback span itself is
// unchanged. A cycle is 14 days (two-week sprint), matching Python.
func windowToDates(now time.Time, window model.WindowInput) (start, end time.Time) {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	end = today.AddDate(0, 0, 1)
	days := window.Value * 7
	switch window.Unit {
	case model.WindowUnitDay:
		days = window.Value
	case model.WindowUnitWeek:
		days = window.Value * 7
	case model.WindowUnitCycle:
		days = window.Value * 14
	}
	start = today.AddDate(0, 0, -days)
	return start, end
}

// parseEvidence ports _parse_evidence (recommendations.py) with Python's own
// value semantics, not Go's struct decoding: the column is decoded as
// json.loads does (pyjson), every non-object entry is skipped, str() and
// float() are applied to each field as Python applies them, and an entry
// whose value Python cannot convert is skipped. So a JSON null in any
// position is never turned into a zero value: a null entry is skipped, a
// null value skips the entry (float(None) raises), a null team_id is the
// string "None" (str(None)).
//
// The row outcome follows Python's own exceptions: evidence_json that decodes
// to a bare number, boolean or null (iterating it raises TypeError, which
// _row_to_recommendation catches) drops the row; a value too large for a float
// raises OverflowError, which nothing on the path catches, so the whole field
// errors. An evidence column that decodes to a string or an object iterates
// characters or keys, none of which is an object: no evidence.
func parseEvidence(ctx context.Context, raw string) (refs []model.EvidenceRef, outcome rowOutcome) {
	if raw == "" {
		return nil, rowKept
	}
	decoded, err := pyjson.DecodeString(raw)
	if err != nil {
		slog.WarnContext(ctx, "query-api: recommendations evidence_json unparseable, skipping",
			"operation", "recommendations", "error", err)
		return nil, rowKept
	}
	var items []pyjson.Value
	switch typed := decoded.(type) {
	case []pyjson.Value:
		items = typed
	case string, *pyjson.Object:
		return nil, rowKept
	default:
		slog.WarnContext(ctx, "query-api: recommendations evidence_json is not iterable, skipping the row",
			"operation", "recommendations")
		return nil, rowDropped
	}
	for _, item := range items {
		entry, ok := item.(*pyjson.Object)
		if !ok {
			continue
		}
		ref, verdict := evidenceRefFromObject(entry)
		switch verdict {
		case entryKept:
			refs = append(refs, ref)
		case entrySkipped:
			slog.WarnContext(ctx, "query-api: recommendations evidence entry malformed, skipping",
				"operation", "recommendations")
		case entryOverflows:
			slog.ErrorContext(ctx, "query-api: recommendations evidence value overflows a float, failing the field",
				"operation", "recommendations")
			return nil, rowOverflows
		}
	}
	return refs, rowKept
}

// rowOutcome is what an evidence column does to its row.
type rowOutcome int

const (
	rowKept rowOutcome = iota
	rowDropped
	rowOverflows
)

// errEvidenceOverflow is Python's uncaught OverflowError ("int too large to
// convert to float") for an evidence value.
var errEvidenceOverflow = errors.New("recommendations: an evidence value is too large to convert to a float")

type entryVerdict int

const (
	entryKept entryVerdict = iota
	entrySkipped
	entryOverflows
)

func evidenceRefFromObject(entry *pyjson.Object) (model.EvidenceRef, entryVerdict) {
	get := func(key string, fallback pyjson.Value) pyjson.Value {
		if value, present := entry.Get(key); present {
			return value
		}
		return fallback
	}
	date := func(key string) (graphqldate.Date, bool) {
		value := get(key, "")
		if !pyjson.Truthy(value) {
			return graphqldate.Date{}, true
		}
		parsed, err := graphqldate.Parse(pyjson.Str(value))
		return parsed, err == nil
	}
	windowStart, ok := date("window_start")
	if !ok {
		return model.EvidenceRef{}, entrySkipped
	}
	windowEnd, ok := date("window_end")
	if !ok {
		return model.EvidenceRef{}, entrySkipped
	}
	value, verdict := pyFloat(get("value", pyjson.Float(0)))
	if verdict != entryKept {
		return model.EvidenceRef{}, verdict
	}
	return model.EvidenceRef{
		TeamID:      pyjson.Str(get("team_id", "")),
		MetricTable: pyjson.Str(get("metric_table", "")),
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		Field:       pyjson.Str(get("field", "")),
		Value:       value,
	}, entryKept
}

// pyNumberText is the text float() accepts for a finite decimal: optional
// sign, digits with single underscores between digits, an optional fraction
// and exponent.
var pyNumberText = regexp.MustCompile(`^[+-]?(?:\d(?:_?\d)*(?:\.(?:\d(?:_?\d)*)?)?|\.\d(?:_?\d)*)(?:[eE][+-]?\d(?:_?\d)*)?$`)

// pyFloat is float(value) for a decoded JSON value: entrySkipped where Python
// raises TypeError or ValueError (caught per entry), entryOverflows where it
// raises OverflowError (not caught anywhere on the path).
func pyFloat(value pyjson.Value) (float64, entryVerdict) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return 1, entryKept
		}
		return 0, entryKept
	case pyjson.Float:
		return float64(typed), entryKept
	case pyjson.Int:
		if typed.Int == nil {
			return 0, entryKept
		}
		f, _ := new(big.Float).SetInt(typed.Int).Float64()
		if math.IsInf(f, 0) {
			return 0, entryOverflows
		}
		return f, entryKept
	case string:
		text := strings.TrimSpace(typed)
		switch strings.ToLower(strings.TrimLeft(text, "+-")) {
		case "inf", "infinity":
			if strings.HasPrefix(text, "-") {
				return math.Inf(-1), entryKept
			}
			return math.Inf(1), entryKept
		case "nan":
			return math.NaN(), entryKept
		}
		if !pyNumberText.MatchString(text) {
			return 0, entrySkipped
		}
		parsed, err := strconv.ParseFloat(strings.ReplaceAll(text, "_", ""), 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return 0, entrySkipped
		}
		return parsed, entryKept
	default:
		// None, list, dict: float() raises TypeError.
		return 0, entrySkipped
	}
}

// severityFromRaw ports the Severity(raw_sev) / ValueError fallback in
// _row_to_recommendation exactly: an out-of-vocabulary value degrades to
// WARNING, it never fails the row.
func severityFromRaw(raw string) model.Severity {
	// The ClickHouse column and Python's Severity enum both hold the
	// lowercase form ("warning"/"critical"); gqlgen's generated Severity
	// constants are UPPERCASE ("WARNING"/"CRITICAL"). Upper-case before
	// comparing, or every real row falls through to the WARNING default
	// -- caught by TestResolve_FieldMapping, which fixture-asserts
	// CRITICAL for a row whose DB column literally says "critical".
	switch model.Severity(strings.ToUpper(raw)) {
	case model.SeverityWarning:
		return model.SeverityWarning
	case model.SeverityCritical:
		return model.SeverityCritical
	default:
		return model.SeverityWarning
	}
}

// row is one scanned recommendations_daily result row.
type row struct {
	teamID       string
	orgID        string
	ruleID       string
	severity     string
	title        string
	rationale    string
	successCrit  string
	evidenceJSON string
	windowStart  time.Time
	windowEnd    time.Time
	computedAt   time.Time
}

// Resolve answers the recommendations field. A query failure is logged
// and answered as an empty list, matching Python's own
// `except Exception: logger.exception(...); return []` -- a transient
// ClickHouse read failure never surfaces as a GraphQL field error here,
// same as upstream.
func Resolve(ctx context.Context, client QueryClient, orgID, team string, window model.WindowInput, now time.Time) ([]model.Recommendation, error) {
	start, end := windowToDates(now, window)
	bindings := []dhclickhouse.Binding{
		{Name: "team_id", Value: team},
		{Name: "org_id", Value: orgID},
		{Name: "window_start", Value: start.Format("2006-01-02")},
		{Name: "window_end", Value: end.Format("2006-01-02")},
	}
	rs, err := client.Query(ctx, recommendationsSQL, bindings)
	if err != nil {
		slog.ErrorContext(ctx, "query-api: recommendations query failed, answering empty",
			"operation", "recommendations", "error", err)
		return []model.Recommendation{}, nil
	}
	defer rs.Close()

	out := []model.Recommendation{}
	for rs.Next() {
		var r row
		var fired bool
		if err := rs.Scan(&r.teamID, &r.orgID, &r.ruleID, &fired, &r.severity, &r.title,
			&r.rationale, &r.successCrit, &r.evidenceJSON, &r.windowStart, &r.windowEnd,
			&r.computedAt); err != nil {
			slog.WarnContext(ctx, "query-api: recommendations row malformed, skipping",
				"operation", "recommendations", "error", err)
			continue
		}
		evidence, outcome := parseEvidence(ctx, r.evidenceJSON)
		if outcome == rowOverflows {
			return nil, errEvidenceOverflow
		}
		if outcome == rowDropped {
			continue
		}
		out = append(out, model.Recommendation{
			RuleID:           r.ruleID,
			TeamID:           r.teamID,
			OrgID:            r.orgID,
			ComputedAt:       r.computedAt,
			WindowStart:      graphqldate.New(r.windowStart),
			WindowEnd:        graphqldate.New(r.windowEnd),
			Severity:         severityFromRaw(r.severity),
			Title:            r.title,
			Rationale:        r.rationale,
			SuccessCriterion: r.successCrit,
			Evidence:         evidence,
		})
	}
	if err := rs.Err(); err != nil {
		// Python's query_dicts either returns every row or raises, and a raise
		// is answered as an empty list: a stream that broke half-way must not
		// be answered as a complete, shorter list.
		slog.ErrorContext(ctx, "query-api: recommendations result iteration failed, answering empty",
			"operation", "recommendations", "error", err)
		return []model.Recommendation{}, nil
	}
	return out, nil
}
