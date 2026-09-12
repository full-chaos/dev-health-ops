// Package quadrant is the Go port of GET /api/v1/quadrant
// (src/dev_health_ops/api/services/quadrant.py's build_quadrant_response,
// api/queries/quadrant.py's fetch_quadrant_metric/
// fetch_work_item_team_quadrant_metric, and the team-label lookup inside
// _resolve_team_labels), CHAOS-5550.
//
// Python source read at this branch's base (origin/main f2715536f):
//   - api/services/quadrant.py -- QUADRANT_DEFINITIONS (4 fixed-axis types),
//     TEAM_METRICS/REPO_METRICS/PERSON_METRICS, and build_quadrant_response.
//   - api/queries/quadrant.py -- the two ClickHouse readers.
//   - api/models/schemas.py -- QuadrantResponse/QuadrantAxes/QuadrantAxis/
//     QuadrantPoint/QuadrantPointTrajectory/QuadrantAnnotation, the wire shape.
//
// PARITY, AS-IS (ruling R99): this port matches Python's behaviour exactly,
// including the per-chart attribution quirk it does not try to fix -- the
// cycle_throughput quadrant's x (cycle_time) and y (throughput) axes read
// team attribution via the primary-team-attribution join
// (work_item_team_attributions, is_primary=1), while every other quadrant
// definition (churn_throughput, wip_throughput, review_load_latency) reads
// the stored team_id column directly off the daily rollup tables. Any
// change to that split is a product decision, not a porting one.
//
// SCOPE, deliberately narrower than the full Python endpoint, in ONE
// documented way:
//
//  1. Only "team" and "repo" group scope are ported (scope_type
//     org/team/service -> team grain, scope_type repo -> repo grain --
//     _group_scope's own mapping). scope_type developer/person is NOT
//     ported: Python's person branch pulls in a second subsystem this PR
//     does not touch -- identity alias resolution (people_identity.py's
//     load_identity_aliases/identity_variants/person_id_for_identity,
//     backed by its own data file) and two more ClickHouse readers
//     (queries/people.py's resolve_person_identity/fetch_person_team_id) --
//     none of which the ticket's scope list names (resolver, the four
//     quadrant definitions, the route, the migration-matrix row). This
//     route answers 501 for scope_type in {developer, person}, naming the
//     gap explicitly rather than silently mis-answering, and is
//     independently reversible: the person branch can be added later
//     without changing this file's team/repo code paths or the wire
//     contract for the requests it already serves. PERSON_METRICS is
//     therefore not ported either -- it exists only to serve that branch.
//
// Every other behaviour Python has for scope_type in {org, team, repo,
// service} -- including the churn_throughput forced-repo-grain override
// (CHAOS-2079) and the cycle_throughput attribution quirk above -- is
// ported verbatim.
//
// Data-layer note: ClickHouse reads follow src/dev_health_ops/
// clickhouse_dedup.py's dedup_from exactly (dedupFrom below is the
// subset of its table registry the two ported metric tables and their
// query-side dependency actually use): work_item_metrics_daily is
// ReplacingMergeTree(computed_at), deduplicated with FINAL; user_metrics_daily
// and repo_metrics_daily are legacy append-only MergeTree, deduplicated with
// the same "latest computed_at per natural key" LIMIT-1-BY subquery Python's
// dedup_from returns for them. teams is read with FINAL directly (not
// through dedup_from), matching _resolve_team_labels's own query.
//
// Go-side-only adaptation (does not change any computed value): every
// value_expr is wrapped in toFloat64(...) in the outer SELECT so the Go
// driver has one fixed, static scan destination regardless of whether the
// underlying ClickHouse aggregate (sum/avg/uniqExact/division) would
// otherwise yield UInt64 or Float64 -- a numeric CAST, not a computation
// change. Python's client has no such static-typing constraint (query_dicts
// returns native Python values and build_quadrant_response's own
// `float(row.get("value") or 0.0)` coerces immediately anyway), so this is
// purely a Go-plumbing necessity, not a parity divergence.
package quadrant

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every sibling operation package
// (operatingreview.QueryClient, hotspots.QueryClient, etc.) declares
// independently.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// ---------------------------------------------------------------------------
// Metric specs and quadrant definitions -- ported verbatim from
// api/services/quadrant.py's MetricSpec/AxisDefinition/QuadrantDefinition
// dataclasses and TEAM_METRICS/REPO_METRICS/QUADRANT_DEFINITIONS.

// MetricSpec ports the MetricSpec dataclass (quadrant.py:39-51).
type MetricSpec struct {
	Metric                    string
	Label                     string
	Unit                      string
	Table                     string // "<table> AS m" or "<table> AS m" with a join alias, matches Python's table field verbatim
	ValueExpr                 string
	EntityExpr                string
	LabelExpr                 string
	JoinClause                string
	WhereClause               string
	Transform                 func(float64) float64
	UsePrimaryTeamAttribution bool
}

func identity(v float64) float64 { return v }

// hoursToDays ports quadrant.py's `lambda value: value / 24.0` (cycle_time
// specs, both team and repo grain).
func hoursToDays(v float64) float64 { return v / 24.0 }

// AxisDefinition ports AxisDefinition (quadrant.py:54-58).
type AxisDefinition struct {
	Metric string
	Label  string
	Unit   string
}

// QuadrantDefinition ports QuadrantDefinition (quadrant.py:61-66).
type QuadrantDefinition struct {
	Type           string
	X              AxisDefinition
	Y              AxisDefinition
	EvidenceMetric string
}

// teamLabelExpr ports TEAM_LABEL (quadrant.py:69).
const teamLabelExpr = "ifNull(nullIf(m.team_name, ''), m.team_id)"

// TeamMetrics ports TEAM_METRICS (quadrant.py:72-136) verbatim.
var TeamMetrics = map[string]MetricSpec{
	"churn": {
		Metric: "churn", Label: "Churn", Unit: "loc",
		Table: "user_metrics_daily AS m", ValueExpr: "sum(m.loc_touched)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.team_id != ''", Transform: identity,
	},
	"throughput": {
		Metric: "throughput", Label: "Throughput", Unit: "items",
		Table: "work_item_metrics_daily AS m", ValueExpr: "sum(m.items_completed)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.team_id != ''", Transform: identity,
		UsePrimaryTeamAttribution: true,
	},
	"cycle_time": {
		Metric: "cycle_time", Label: "Cycle Time", Unit: "days",
		Table: "work_item_metrics_daily AS m", ValueExpr: "avg(m.cycle_time_p50_hours)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.cycle_time_p50_hours IS NOT NULL AND m.team_id != ''",
		Transform:   hoursToDays, UsePrimaryTeamAttribution: true,
	},
	"wip": {
		Metric: "wip", Label: "WIP", Unit: "items",
		Table: "work_item_metrics_daily AS m", ValueExpr: "avg(m.wip_count_end_of_day)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.team_id != ''", Transform: identity,
	},
	"review_load": {
		Metric: "review_load", Label: "Review Load", Unit: "reviews",
		Table: "user_metrics_daily AS m", ValueExpr: "sum(m.reviews_given) / nullIf(countDistinct(m.identity_id), 0)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.team_id != ''", Transform: identity,
	},
	"review_latency": {
		Metric: "review_latency", Label: "Review Latency", Unit: "hours",
		Table: "user_metrics_daily AS m", ValueExpr: "avg(m.pr_first_review_p50_hours)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.team_id != '' AND m.pr_first_review_p50_hours IS NOT NULL",
		Transform:   identity,
	},
}

// RepoMetrics ports REPO_METRICS (quadrant.py:138-206) verbatim.
var RepoMetrics = map[string]MetricSpec{
	"churn": {
		Metric: "churn", Label: "Churn", Unit: "loc",
		Table: "repo_metrics_daily AS m", ValueExpr: "sum(m.total_loc_touched)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos ON repos.id = m.repo_id",
		WhereClause: "AND repos.repo != ''", Transform: identity,
	},
	"throughput": {
		Metric: "throughput", Label: "Throughput", Unit: "items",
		Table: "repo_metrics_daily AS m", ValueExpr: "sum(m.prs_merged)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos ON repos.id = m.repo_id",
		WhereClause: "AND repos.repo != ''", Transform: identity,
	},
	"cycle_time": {
		Metric: "cycle_time", Label: "Cycle Time", Unit: "days",
		Table: "repo_metrics_daily AS m", ValueExpr: "avg(m.median_pr_cycle_hours)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos ON repos.id = m.repo_id",
		WhereClause: "AND m.median_pr_cycle_hours > 0 AND repos.repo != ''",
		Transform:   hoursToDays,
	},
	"wip": {
		Metric: "wip", Label: "WIP", Unit: "items",
		Table: "work_item_metrics_daily AS m", ValueExpr: "avg(m.wip_count_end_of_day)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos ON repos.repo = m.work_scope_id",
		WhereClause: "AND m.work_scope_id != ''", Transform: identity,
	},
	"review_load": {
		Metric: "review_load", Label: "Review Load", Unit: "reviews",
		Table: "user_metrics_daily AS m", ValueExpr: "sum(m.reviews_given) / nullIf(countDistinct(m.identity_id), 0)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos ON repos.id = m.repo_id",
		WhereClause: "AND repos.repo != ''", Transform: identity,
	},
	"review_latency": {
		Metric: "review_latency", Label: "Review Latency", Unit: "hours",
		Table: "user_metrics_daily AS m", ValueExpr: "avg(m.pr_first_review_p50_hours)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos ON repos.id = m.repo_id",
		WhereClause: "AND repos.repo != '' AND m.pr_first_review_p50_hours IS NOT NULL",
		Transform:   identity,
	},
}

// metricsByScope ports METRICS_BY_SCOPE (quadrant.py:272-276), restricted to
// the two group scopes this port carries -- see package doc comment.
var metricsByScope = map[string]map[string]MetricSpec{
	"team": TeamMetrics,
	"repo": RepoMetrics,
}

// QuadrantDefinitions ports QUADRANT_DEFINITIONS (quadrant.py:278-303)
// verbatim -- all four fixed-axis types.
var QuadrantDefinitions = map[string]QuadrantDefinition{
	"churn_throughput": {
		Type:           "churn_throughput",
		X:              AxisDefinition{Metric: "churn", Label: "Churn", Unit: "loc"},
		Y:              AxisDefinition{Metric: "throughput", Label: "Throughput", Unit: "items"},
		EvidenceMetric: "throughput",
	},
	"cycle_throughput": {
		Type:           "cycle_throughput",
		X:              AxisDefinition{Metric: "cycle_time", Label: "Cycle Time", Unit: "days"},
		Y:              AxisDefinition{Metric: "throughput", Label: "Throughput", Unit: "items"},
		EvidenceMetric: "cycle_time",
	},
	"wip_throughput": {
		Type:           "wip_throughput",
		X:              AxisDefinition{Metric: "wip", Label: "WIP", Unit: "items"},
		Y:              AxisDefinition{Metric: "throughput", Label: "Throughput", Unit: "items"},
		EvidenceMetric: "throughput",
	},
	"review_load_latency": {
		Type:           "review_load_latency",
		X:              AxisDefinition{Metric: "review_load", Label: "Review Load", Unit: "reviews"},
		Y:              AxisDefinition{Metric: "review_latency", Label: "Review Latency", Unit: "hours"},
		EvidenceMetric: "review_latency",
	},
}

// ---------------------------------------------------------------------------
// dedup_from -- ported subset of src/dev_health_ops/clickhouse_dedup.py's
// dedup_from, restricted to the tables the two ported metric sets (team/repo,
// person excluded) actually name: work_item_metrics_daily (rerun-deduped
// ReplacingMergeTree) and user_metrics_daily/repo_metrics_daily (legacy
// append-only MergeTree, both registered in _APPEND_ONLY_DAILY_KEYS with the
// natural keys below).

var rerunDedupedDailyTables = map[string]bool{
	"work_item_metrics_daily": true,
}

var appendOnlyDailyKeys = map[string][]string{
	"user_metrics_daily": {"org_id", "repo_id", "author_email", "day"},
	"repo_metrics_daily": {"org_id", "repo_id", "day"},
}

// dedupFrom ports dedup_from (clickhouse_dedup.py:135-157) for the table
// subset above -- table is "<name>" or "<name> AS <alias>", matching every
// MetricSpec.Table value in this package.
func dedupFrom(table string) string {
	base, alias, hasAlias := strings.Cut(table, " AS ")
	aliasSQL := ""
	if hasAlias {
		aliasSQL = " AS " + alias
	}
	if rerunDedupedDailyTables[base] {
		return base + " FINAL" + aliasSQL
	}
	if keys, ok := appendOnlyDailyKeys[base]; ok {
		sourceAlias := alias
		if !hasAlias {
			sourceAlias = base
		}
		return fmt.Sprintf("(\n            SELECT *\n            FROM %s\n            ORDER BY computed_at DESC\n            LIMIT 1 BY %s\n        ) AS %s", base, strings.Join(keys, ", "), sourceAlias)
	}
	return table
}

// ---------------------------------------------------------------------------
// ClickHouse readers -- port api/queries/quadrant.py verbatim (query shape),
// with the toFloat64 scan-cast documented in the package doc comment.

type metricRow struct {
	Bucket      time.Time
	EntityID    string
	EntityLabel string
	Value       float64
}

func bucketExpr(bucket string) string {
	if bucket == "month" {
		return "toStartOfMonth(day)"
	}
	return "toStartOfWeek(day)"
}

// fetchQuadrantMetric ports fetch_quadrant_metric (queries/quadrant.py:
// 19-58). scope_filter/scope_params are Python parameters this port never
// populates (always empty for team/repo group scope -- see package doc
// comment), so they are not parameters here.
func fetchQuadrantMetric(ctx context.Context, client QueryClient, spec MetricSpec, startDay, endDay time.Time, bucket, orgID string) ([]metricRow, error) {
	joinSQL := ""
	if spec.JoinClause != "" {
		joinSQL = "\n" + spec.JoinClause
	}
	whereSQL := ""
	if spec.WhereClause != "" {
		whereSQL = "\n" + spec.WhereClause
	}
	query := fmt.Sprintf(`
        SELECT
            %s AS bucket,
            %s AS entity_id,
            %s AS entity_label,
            toFloat64(%s) AS value
        FROM %s%s
        WHERE day >= {start_day:Date} AND day < {end_day:Date}%s
          AND org_id = {org_id:String}
        GROUP BY bucket, entity_id, entity_label
        ORDER BY bucket
    `, bucketExpr(bucket), spec.EntityExpr, spec.LabelExpr, spec.ValueExpr, dedupFrom(spec.Table), joinSQL, whereSQL)

	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}
	return scanMetricRows(ctx, client, query, bindings, "quadrant: fetch_quadrant_metric")
}

// primaryWorkItemTeamAttributionSource inlines
// api/queries/investment.py's PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE
// verbatim -- no Go port of this subquery existed anywhere in the tree
// before this PR (grepped cmd/query-api and internal for
// work_item_team_attributions read-side usage: providersync only writes
// it). Ported here as the minimum needed for this route's one caller
// (fetchWorkItemTeamQuadrantMetric), not as a shared package -- a second
// caller should promote it, not copy it again.
const primaryWorkItemTeamAttributionSource = `(
    SELECT
        work_item_id,
        team_id,
        team_name
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String}
      AND is_primary = 1
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String}
          GROUP BY work_item_id
      )
)`

// fetchWorkItemTeamQuadrantMetric ports fetch_work_item_team_quadrant_metric
// (queries/quadrant.py:61-108) verbatim -- the cycle_throughput/team
// attribution-quirk reader.
func fetchWorkItemTeamQuadrantMetric(ctx context.Context, client QueryClient, metric string, startDay, endDay time.Time, bucket, orgID string) ([]metricRow, error) {
	var valueExpr, metricFilter string
	switch metric {
	case "throughput":
		valueExpr = "uniqExact(work_item_id)"
	case "cycle_time":
		valueExpr = "avg(cycle_time_hours)"
		metricFilter = "\n              AND cycle_time_hours IS NOT NULL"
	default:
		return nil, fmt.Errorf("quadrant: unsupported attributed team quadrant metric: %s", metric)
	}

	query := fmt.Sprintf(`
        WITH team_activity AS (
            SELECT
                wct.day,
                wct.work_item_id,
                wct.cycle_time_hours,
                t.team_id AS team_id,
                t.team_name AS team_name
            FROM work_item_cycle_times AS wct FINAL
            INNER JOIN %s AS t
              ON t.work_item_id = wct.work_item_id
            WHERE wct.day >= {start_day:Date} AND wct.day < {end_day:Date}
              AND wct.org_id = {org_id:String}
              AND t.team_id IS NOT NULL
              AND t.team_id != ''%s
        )
        SELECT
            %s AS bucket,
            toString(team_id) AS entity_id,
            ifNull(nullIf(any(team_name), ''), toString(team_id)) AS entity_label,
            toFloat64(%s) AS value
        FROM team_activity
        GROUP BY bucket, entity_id
        ORDER BY bucket
    `, primaryWorkItemTeamAttributionSource, metricFilter, bucketExpr(bucket), valueExpr)

	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}
	return scanMetricRows(ctx, client, query, bindings, "quadrant: fetch_work_item_team_quadrant_metric")
}

func scanMetricRows(ctx context.Context, client QueryClient, query string, bindings []dhclickhouse.Binding, errPrefix string) ([]metricRow, error) {
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("%s query: %w", errPrefix, err)
	}
	defer rows.Close()

	var out []metricRow
	for rows.Next() {
		var row metricRow
		if err := rows.Scan(&row.Bucket, &row.EntityID, &row.EntityLabel, &row.Value); err != nil {
			return nil, fmt.Errorf("%s scan: %w", errPrefix, err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// resolveTeamLabels ports _resolve_team_labels (quadrant.py:400-426),
// including its swallow-and-log-warning behaviour on a ClickHouse failure --
// a broken team-label lookup degrades to entity_id-as-label, it does not
// fail the whole request, matching Python's own `except Exception: logger.
// warning(...); return {}`.
func resolveTeamLabels(ctx context.Context, client QueryClient, teamIDs []string, orgID string) map[string]string {
	if len(teamIDs) == 0 {
		return map[string]string{}
	}
	sorted := append([]string(nil), teamIDs...)
	sort.Strings(sorted)

	query := `
            SELECT toString(id) AS team_id, name AS team_name
            FROM teams FINAL
            WHERE org_id = {org_id:String}
              AND toString(id) IN {team_ids:Array(String)}
            `
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "team_ids", Value: sorted},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		log.Printf("quadrant: could not resolve quadrant team labels: %v", err)
		return map[string]string{}
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var teamID, teamName string
		if err := rows.Scan(&teamID, &teamName); err != nil {
			log.Printf("quadrant: could not resolve quadrant team labels: %v", err)
			return map[string]string{}
		}
		teamID = strings.TrimSpace(teamID)
		teamName = strings.TrimSpace(teamName)
		if teamID != "" && teamName != "" {
			out[teamID] = teamName
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("quadrant: could not resolve quadrant team labels: %v", err)
		return map[string]string{}
	}
	return out
}

func formatDay(t time.Time) string {
	return t.Format("2006-01-02")
}
