// Package producttelemetry serves the productTelemetryDashboard and
// productTelemetryPlatformDashboard GraphQL fields: usage rollups over the
// product_telemetry_events table for one org (keyed by the SHA-256 hex of the
// org id) or across every org.
//
// Both dashboards run the same section queries over a half-open day range
// (start inclusive, end exclusive). The org dashboard adds an org-hash
// predicate to every section; the platform dashboard adds none, plus a totals
// query and a top-organisations query whose rows are named from the
// organizations table in Postgres.
//
// The section queries carry no FINAL, so the counts follow the table's
// replacing-merge state, and ORDER BY carries no tie-break, so equal counts may
// appear in either order; the quantile is the approximate `quantile` function.
// All three are the behaviour of the reference implementation and are kept.
package producttelemetry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// QueryClient is the narrow ClickHouse boundary this package needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// OrgHash is the value the event table stores for an org.
func OrgHash(orgID string) string {
	sum := sha256.Sum256([]byte(orgID))
	return hex.EncodeToString(sum[:])
}

// Range is the half-open day range of a dashboard.
type Range struct{ Start, End time.Time }

// ErrRange is returned for a range whose start is after its end.
var ErrRange = fmt.Errorf("start_date must be before or equal to end_date")

// CheckRange refuses a range whose start is after its end.
func CheckRange(start, end time.Time) error {
	if start.After(end) {
		return ErrRange
	}
	return nil
}

func dateValue(t time.Time) string {
	y, m, d := t.Date()
	return fmt.Sprintf("%04d-%02d-%02d", y, int(m), d)
}

// scoped builds a WHERE clause: the org predicate when orgScoped, the event
// name when name is not empty, and always the range.
func scoped(orgScoped bool, name string) string {
	var conds []string
	if orgScoped {
		conds = append(conds, "org_id_hash = {org_id_hash:String}")
	}
	if name != "" {
		conds = append(conds, "name = '"+name+"'")
	}
	conds = append(conds, "occurred_at >= {start:Date}", "occurred_at < {end:Date}")
	return "WHERE " + strings.Join(conds, "\n  AND ")
}

func dailySQL(org bool) string {
	return `SELECT
    toDate(occurred_at) AS day,
    uniqExact(anonymous_user_id) AS active_anonymous_users
FROM product_telemetry_events
` + scoped(org, "") + `
GROUP BY day
ORDER BY day`
}

func routesSQL(org bool) string {
	return `SELECT
    route_pattern,
    count() AS events,
    uniqExact(session_id) AS sessions,
    uniqExact(anonymous_user_id) AS anonymous_users
FROM product_telemetry_events
` + scoped(org, "page_viewed") + `
GROUP BY route_pattern
ORDER BY events DESC
LIMIT 25`
}

func featureViewsSQL(org bool) string {
	return `SELECT
    JSONExtractString(payload_json, 'feature') AS feature,
    JSONExtractString(payload_json, 'surface') AS surface,
    count() AS views,
    uniqExact(anonymous_user_id) AS anonymous_users
FROM product_telemetry_events
` + scoped(org, "feature_viewed") + `
GROUP BY feature, surface
ORDER BY views DESC`
}

func filterChangesSQL(org bool) string {
	return `SELECT
    JSONExtractString(payload_json, 'view') AS view,
    JSONExtractString(payload_json, 'filterKey') AS filter_key,
    count() AS changes,
    avg(JSONExtractInt(payload_json, 'valueCount')) AS avg_value_count
FROM product_telemetry_events
` + scoped(org, "filter_changed") + `
GROUP BY view, filter_key
ORDER BY changes DESC`
}

func chartInteractionsSQL(org bool) string {
	return `SELECT
    JSONExtractString(payload_json, 'chart') AS chart,
    JSONExtractString(payload_json, 'action') AS action,
    JSONExtractString(payload_json, 'surface') AS surface,
    count() AS interactions,
    uniqExact(session_id) AS sessions
FROM product_telemetry_events
` + scoped(org, "chart_interacted") + `
GROUP BY chart, action, surface
ORDER BY interactions DESC`
}

func clientErrorsSQL(org bool) string {
	return `SELECT
    route_pattern,
    JSONExtractString(payload_json, 'boundary') AS boundary,
    JSONExtractString(payload_json, 'errorClass') AS error_class,
    count() AS errors,
    uniqExact(anonymous_user_id) AS affected_anonymous_users
FROM product_telemetry_events
` + scoped(org, "client_error") + `
GROUP BY route_pattern, boundary, error_class
ORDER BY errors DESC`
}

func sessionSummarySQL(org bool) string {
	return `SELECT
    quantile(0.5)(JSONExtractInt(payload_json, 'durationMs')) AS p50_duration_ms,
    quantile(0.75)(JSONExtractInt(payload_json, 'durationMs')) AS p75_duration_ms,
    quantile(0.9)(JSONExtractInt(payload_json, 'durationMs')) AS p90_duration_ms,
    quantile(0.95)(JSONExtractInt(payload_json, 'durationMs')) AS p95_duration_ms,
    avg(JSONExtractInt(payload_json, 'pagesViewed')) AS avg_pages_viewed,
    avg(JSONExtractInt(payload_json, 'interactions')) AS avg_interactions
FROM product_telemetry_events
` + scoped(org, "session_ended")
}

func totalsSQL() string {
	return `SELECT
    uniqExact(org_id_hash) AS active_orgs,
    uniqExact(anonymous_user_id) AS anonymous_users,
    uniqExact(session_id) AS sessions,
    count() AS events
FROM product_telemetry_events
` + scoped(false, "")
}

func topOrgsSQL() string {
	return `SELECT
    org_id_hash,
    count() AS events,
    uniqExact(session_id) AS sessions,
    uniqExact(anonymous_user_id) AS anonymous_users
FROM product_telemetry_events
` + scoped(false, "") + `
GROUP BY org_id_hash
ORDER BY events DESC
LIMIT 50`
}

// Reader runs the section queries.
type Reader struct {
	ClickHouse QueryClient
}

func (r *Reader) bindings(orgHash string, orgScoped bool, rg Range) []clickhouse.Binding {
	b := []clickhouse.Binding{
		{Name: "start", Value: dateValue(rg.Start)},
		{Name: "end", Value: dateValue(rg.End)},
	}
	if orgScoped {
		b = append(b, clickhouse.Binding{Name: "org_id_hash", Value: orgHash})
	}
	return b
}

// sections is every section both dashboards share.
type sections struct {
	Daily    []model.ProductTelemetryDailyActiveUsersType
	Routes   []model.ProductTelemetryRouteUsageType
	Features []model.ProductTelemetryFeatureViewType
	Filters  []model.ProductTelemetryFilterChangeType
	Charts   []model.ProductTelemetryChartInteractionType
	Errors   []model.ProductTelemetryClientErrorType
	Summary  *model.ProductTelemetrySessionSummaryType
}

func (r *Reader) sections(ctx context.Context, orgHash string, orgScoped bool, rg Range) (*sections, error) {
	b := r.bindings(orgHash, orgScoped, rg)
	s := &sections{
		Daily:    []model.ProductTelemetryDailyActiveUsersType{},
		Routes:   []model.ProductTelemetryRouteUsageType{},
		Features: []model.ProductTelemetryFeatureViewType{},
		Filters:  []model.ProductTelemetryFilterChangeType{},
		Charts:   []model.ProductTelemetryChartInteractionType{},
		Errors:   []model.ProductTelemetryClientErrorType{},
	}
	steps := []struct {
		name string
		sql  string
		scan func(rows clickhouse.RowScanner) error
	}{
		{"daily active users", dailySQL(orgScoped), func(rows clickhouse.RowScanner) error {
			var day time.Time
			var n uint64
			if err := rows.Scan(&day, &n); err != nil {
				return err
			}
			s.Daily = append(s.Daily, model.ProductTelemetryDailyActiveUsersType{Day: graphqldate.New(day), ActiveAnonymousUsers: int(n)})
			return nil
		}},
		{"top routes", routesSQL(orgScoped), func(rows clickhouse.RowScanner) error {
			var route *string
			var events, sessions, users uint64
			if err := rows.Scan(&route, &events, &sessions, &users); err != nil {
				return err
			}
			s.Routes = append(s.Routes, model.ProductTelemetryRouteUsageType{RoutePattern: deref(route), Events: int(events), Sessions: int(sessions), AnonymousUsers: int(users)})
			return nil
		}},
		{"feature views", featureViewsSQL(orgScoped), func(rows clickhouse.RowScanner) error {
			var feature, surface string
			var views, users uint64
			if err := rows.Scan(&feature, &surface, &views, &users); err != nil {
				return err
			}
			s.Features = append(s.Features, model.ProductTelemetryFeatureViewType{Feature: feature, Surface: surface, Views: int(views), AnonymousUsers: int(users)})
			return nil
		}},
		{"filter changes", filterChangesSQL(orgScoped), func(rows clickhouse.RowScanner) error {
			var view, key string
			var changes uint64
			var avg float64
			if err := rows.Scan(&view, &key, &changes, &avg); err != nil {
				return err
			}
			s.Filters = append(s.Filters, model.ProductTelemetryFilterChangeType{View: view, FilterKey: key, Changes: int(changes), AvgValueCount: optionalFloat(avg)})
			return nil
		}},
		{"chart interactions", chartInteractionsSQL(orgScoped), func(rows clickhouse.RowScanner) error {
			var chart, action, surface string
			var interactions, sessions uint64
			if err := rows.Scan(&chart, &action, &surface, &interactions, &sessions); err != nil {
				return err
			}
			s.Charts = append(s.Charts, model.ProductTelemetryChartInteractionType{Chart: chart, Action: action, Surface: surface, Interactions: int(interactions), Sessions: int(sessions)})
			return nil
		}},
		{"client errors", clientErrorsSQL(orgScoped), func(rows clickhouse.RowScanner) error {
			var route *string
			var boundary, class string
			var errs, users uint64
			if err := rows.Scan(&route, &boundary, &class, &errs, &users); err != nil {
				return err
			}
			s.Errors = append(s.Errors, model.ProductTelemetryClientErrorType{RoutePattern: deref(route), Boundary: boundary, ErrorClass: class, Errors: int(errs), AffectedAnonymousUsers: int(users)})
			return nil
		}},
		{"session summary", sessionSummarySQL(orgScoped), func(rows clickhouse.RowScanner) error {
			var p50, p75, p90, p95, pages, inter float64
			if err := rows.Scan(&p50, &p75, &p90, &p95, &pages, &inter); err != nil {
				return err
			}
			s.Summary = &model.ProductTelemetrySessionSummaryType{
				P50DurationMs: optionalInt(p50), P75DurationMs: optionalInt(p75),
				P90DurationMs: optionalInt(p90), P95DurationMs: optionalInt(p95),
				AvgPagesViewed: optionalFloat(pages), AvgInteractions: optionalFloat(inter),
			}
			return nil
		}},
	}
	for _, step := range steps {
		if err := r.run(ctx, step.name, step.sql, b, step.scan); err != nil {
			return nil, err
		}
	}
	if s.Summary == nil {
		s.Summary = &model.ProductTelemetrySessionSummaryType{}
	}
	return s, nil
}

func (r *Reader) run(ctx context.Context, name, sql string, b []clickhouse.Binding, scan func(clickhouse.RowScanner) error) error {
	rows, err := r.ClickHouse.Query(ctx, sql, b)
	if err != nil {
		return fmt.Errorf("producttelemetry: %s query: %w", name, err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return fmt.Errorf("producttelemetry: %s scan: %w", name, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("producttelemetry: %s rows: %w", name, err)
	}
	return nil
}

// Org answers the org dashboard for one org.
func (r *Reader) Org(ctx context.Context, orgID string, rg Range) (*model.ProductTelemetryDashboardType, error) {
	s, err := r.sections(ctx, OrgHash(orgID), true, rg)
	if err != nil {
		return nil, err
	}
	return &model.ProductTelemetryDashboardType{
		DailyActiveUsers: s.Daily, TopRoutes: s.Routes, FeatureViews: s.Features,
		FilterChanges: s.Filters, ChartInteractions: s.Charts, ClientErrors: s.Errors,
		SessionSummary: s.Summary,
	}, nil
}

// TopOrgRow is one top-organisations rollup row before its names are attached.
type TopOrgRow struct {
	Hash                    string
	Events, Sessions, Users int
}

// Platform is the cross-org answer before the top organisations are named.
type Platform struct {
	Totals *model.ProductTelemetryPlatformTotalsType
	Shared *sections
	Top    []TopOrgRow
}

// PlatformSections reads the cross-org sections, totals and top organisations.
func (r *Reader) PlatformSections(ctx context.Context, rg Range) (*Platform, error) {
	s, err := r.sections(ctx, "", false, rg)
	if err != nil {
		return nil, err
	}
	b := r.bindings("", false, rg)
	p := &Platform{Shared: s, Totals: &model.ProductTelemetryPlatformTotalsType{}, Top: []TopOrgRow{}}
	if err := r.run(ctx, "totals", totalsSQL(), b, func(rows clickhouse.RowScanner) error {
		var orgs, users, sessions, events uint64
		if err := rows.Scan(&orgs, &users, &sessions, &events); err != nil {
			return err
		}
		p.Totals = &model.ProductTelemetryPlatformTotalsType{ActiveOrgs: int(orgs), AnonymousUsers: int(users), Sessions: int(sessions), Events: int(events)}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := r.run(ctx, "top orgs", topOrgsSQL(), b, func(rows clickhouse.RowScanner) error {
		var hash string
		var events, sessions, users uint64
		if err := rows.Scan(&hash, &events, &sessions, &users); err != nil {
			return err
		}
		p.Top = append(p.Top, TopOrgRow{Hash: hash, Events: int(events), Sessions: int(sessions), Users: int(users)})
		return nil
	}); err != nil {
		return nil, err
	}
	return p, nil
}

// OrgName is one row of the organizations table.
type OrgName struct{ ID, Slug, Name string }

// Assemble names the top organisations from the organizations table and
// returns the platform dashboard. orgs may be nil when there are no top
// organisations.
func (p *Platform) Assemble(orgs []OrgName) *model.ProductTelemetryPlatformDashboardType {
	index := map[string]OrgName{}
	for _, o := range orgs {
		index[OrgHash(o.ID)] = o
	}
	top := make([]model.ProductTelemetryTopOrgType, 0, len(p.Top))
	for _, row := range p.Top {
		t := model.ProductTelemetryTopOrgType{OrgIDHash: row.Hash, Events: row.Events, Sessions: row.Sessions, AnonymousUsers: row.Users}
		if match, ok := index[row.Hash]; ok {
			id := match.ID
			t.OrgID = &id
			if match.Slug != "" {
				slug := match.Slug
				t.OrgSlug = &slug
			}
			if match.Name != "" {
				name := match.Name
				t.OrgName = &name
			}
		}
		top = append(top, t)
	}
	s := p.Shared
	return &model.ProductTelemetryPlatformDashboardType{
		Totals: p.Totals, DailyActiveUsers: s.Daily, TopRoutes: s.Routes, FeatureViews: s.Features,
		FilterChanges: s.Filters, ChartInteractions: s.Charts, ClientErrors: s.Errors,
		SessionSummary: s.Summary, TopOrgs: top,
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// optionalFloat is null for a value that is not finite (an aggregate over no
// rows).
func optionalFloat(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

// optionalInt truncates toward zero, and is null for a value that is not
// finite.
func optionalInt(v float64) *int {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	i := int(v)
	return &i
}
