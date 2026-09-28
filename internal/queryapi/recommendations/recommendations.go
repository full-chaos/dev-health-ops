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
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

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

// evidenceItem is the wire shape of one entry of the evidence_json
// column -- the engine serialises it with these exact snake_case keys
// (recommendations.py's _parse_evidence doc comment), matching
// model.EvidenceRef field-for-field.
type evidenceItem struct {
	TeamID      string  `json:"team_id"`
	MetricTable string  `json:"metric_table"`
	WindowStart string  `json:"window_start"`
	WindowEnd   string  `json:"window_end"`
	Field       string  `json:"field"`
	Value       float64 `json:"value"`
}

// parseEvidence ports _parse_evidence exactly, including its per-entry
// tolerance: a malformed evidence_json payload, or one malformed entry
// inside an otherwise-valid array, is skipped (logged), never fails the
// whole recommendation.
func parseEvidence(ctx context.Context, raw string) []model.EvidenceRef {
	if raw == "" {
		return nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		slog.WarnContext(ctx, "query-api: recommendations evidence_json unparseable, skipping",
			"operation", "recommendations", "error", err)
		return nil
	}
	var out []model.EvidenceRef
	for _, raw := range items {
		// json.Unmarshal of a JSON null into a struct succeeds with zero
		// values; Python skips every non-dict entry, so a null must be
		// skipped here too, never surfaced as a zero-valued evidence row.
		if string(bytes.TrimSpace(raw)) == "null" {
			slog.WarnContext(ctx, "query-api: recommendations evidence entry is null, skipping",
				"operation", "recommendations")
			continue
		}
		var item evidenceItem
		if err := json.Unmarshal(raw, &item); err != nil {
			slog.WarnContext(ctx, "query-api: recommendations evidence entry malformed, skipping",
				"operation", "recommendations", "error", err)
			continue
		}
		windowStart := graphqldate.Date{}
		if item.WindowStart != "" {
			parsed, err := graphqldate.Parse(item.WindowStart)
			if err != nil {
				slog.WarnContext(ctx, "query-api: recommendations evidence entry malformed, skipping",
					"operation", "recommendations", "field", "window_start", "error", err)
				continue
			}
			windowStart = parsed
		}
		windowEnd := graphqldate.Date{}
		if item.WindowEnd != "" {
			parsed, err := graphqldate.Parse(item.WindowEnd)
			if err != nil {
				slog.WarnContext(ctx, "query-api: recommendations evidence entry malformed, skipping",
					"operation", "recommendations", "field", "window_end", "error", err)
				continue
			}
			windowEnd = parsed
		}
		out = append(out, model.EvidenceRef{
			TeamID:      item.TeamID,
			MetricTable: item.MetricTable,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			Field:       item.Field,
			Value:       item.Value,
		})
	}
	return out
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
func Resolve(ctx context.Context, client QueryClient, orgID, team string, window model.WindowInput, now time.Time) []model.Recommendation {
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
		return []model.Recommendation{}
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
			Evidence:         parseEvidence(ctx, r.evidenceJSON),
		})
	}
	if err := rs.Err(); err != nil {
		slog.ErrorContext(ctx, "query-api: recommendations result iteration failed, answering what was read so far",
			"operation", "recommendations", "error", err)
	}
	return out
}
