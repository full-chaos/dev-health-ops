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
// (work_item_team_attributions, is_primary=1), and wip_throughput's wip
// axis reads the stored team_id of work_item_metrics_daily. One declared
// difference (D5978): the team-grain user_metrics_daily metrics (review load,
// review latency, churn) group by the repository's owning team
// (MetricSpec.OwnerTeam), never by the stored team_id, which is the person's
// membership team.
//
// SCOPE: all four group scopes Python serves (team, repo, person, and the
// org/service aliases _group_scope folds into team) are now ported. The
// person/developer branch additionally ports
// identity.go's identity-alias resolution (people_identity.py's
// load_identity_aliases/identity_variants/person_id_for_identity) and the
// ClickHouse reader queries/people.py's resolve_person_identity backs -- see
// identity.go's package doc comment for the config-file reality that makes
// the alias-reconciliation path a practical no-op today without changing the
// ported algorithm. A person-scope response carries only that person's own
// rows (CHAOS-9343): the person's identity strings bind to the identity
// column of each read; there is no membership-team cohort.
//
// Every behaviour Python has for scope_type in {org, team, repo, service,
// developer, person} -- including the churn_throughput forced-repo-grain
// override (CHAOS-2079, which does NOT apply to person scope, matching
// quadrant.py:493's own `normalized_scope in {"org", "team"}` gate) and the
// cycle_throughput attribution quirk above -- is ported verbatim.
//
// Declared difference (CHAOS-9816): a point is the aggregate of the whole
// requested window, read once by the same reader with one bucket for the window
// (bucketWindow). Python plots the last bucket's values and window_start /
// window_end. The trajectory keeps one entry per bucket.
//
// Declared difference (CHAOS-9815): at the team grain every ACTIVE team of the
// org in scope (the selected teams when the request names some) is a point. A
// team with no point in the window is sent as a no-data point (Point.NoData:
// x and y null, no_data true), never absent and never a zero. See idle.go.
//
// Data-layer note: ClickHouse reads follow src/dev_health_ops/
// clickhouse_dedup.py's dedup_from IN INTENT, not in the reference's own
// current text -- all four daily-family tables the ported metric specs
// read (work_item_metrics_daily, work_item_user_metrics_daily,
// user_metrics_daily, repo_metrics_daily) are ReplacingMergeTree
// (computed_at) in prod (confirmed directly against prod's own
// system.tables), the first two since migration 055, the last two since
// migration 096 (internal/chmigrate/history/
// 096_daily_family_tables_replacing_merge_tree.py -- TARGET_SORT_KEYS
// covers both with a sorting key that matches this package's own dedup
// key), and take the identical FINAL treatment: a ReplacingMergeTree
// table in this binary dedups with FINAL or
// argMax(tuple(col), version)).1, never a bare LIMIT-1-BY, which sorts
// and collapses the whole table with no per-tenant scope of its own. A
// live
// reproduction against a compose ClickHouse instance behind prod on
// migration 096 (its own one-shot migrate run predates the last
// conversion) surfaced a SEPARATE, genuinely broken bug this file still
// fixes: dedupFrom's FINAL branch built `<table> FINAL AS <alias>` --
// invalid ClickHouse GRAMMAR ("Code: 62. DB::Exception: Syntax error...
// Expected one of:... JOIN..."), confirmed live and true for EVERY
// aliased table this package reads, regardless of engine (`<table> AS
// <alias> FINAL` is the only order ClickHouse accepts). Fixed below:
// alias now comes before FINAL, matching every hand-written query
// elsewhere in this tree that already got it right (e.g. "FROM
// work_item_cycle_times AS wct FINAL"). Every dedup read below uses
// FINAL, with org_id filtered in the SAME top-level statement as the
// FINAL source, never a separate unfiltered subquery. teams is read with
// FINAL directly (not through dedupFrom), matching _resolve_team_labels's
// own query.
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
	"log/slog"
	"sort"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
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
	// OwnerTeam groups the rows by the team that OWNS the row's repository
	// (teamscope.OwnerShares, alias o), not by a team id the row stores.
	OwnerTeam bool
	// ReaderExpr, set with OwnerTeam, makes the read a ratio of a weighted sum
	// to a weighted count of distinct readers: ValueExpr is the numerator over
	// the rows of one reader, ReaderWeightExpr is that reader's weight, and the
	// value is sum(numerator) / sum(reader weight) over the readers of the group.
	ReaderExpr       string
	ReaderWeightExpr string
	// DayExpr is the UTC date a row falls on, for the window filter and the
	// bucket. Empty means the daily table's own day column.
	DayExpr string
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

// TeamMetrics ports TEAM_METRICS (quadrant.py:72-136), with one declared
// difference: the specs of user_metrics_daily group by the repository's owning
// team (OwnerTeam), never by the team id the row stores. That stored id is the
// team of the PERSON (a membership), and a team is repository and project
// ownership only. A team's review load and review latency are therefore the
// reviews in the repositories the team owns, not the reviews by its members.
var TeamMetrics = map[string]MetricSpec{
	// churn is additive: a team's churn is the sum over its repositories of the
	// repository's churn x the owner's share (D5998, CHAOS-9693), so the team
	// points conserve the organization's churn.
	"churn": {
		Metric: "churn", Label: "Churn", Unit: "loc",
		Table: "user_metrics_daily AS m", ValueExpr: "sum(toFloat64(m.loc_touched) * o.owner_share)",
		EntityExpr: ownerTeamIDExpr, LabelExpr: ownerTeamLabelExpr,
		Transform: identity, OwnerTeam: true,
	},
	"throughput": {
		Metric: "throughput", Label: "Throughput", Unit: "items",
		Table: "work_item_metrics_daily AS m", ValueExpr: "sum(m.items_completed)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.team_id != '' AND " + rowview.OrgPredicate("work_item_metrics_daily", "m"), Transform: identity,
		UsePrimaryTeamAttribution: true,
	},
	"cycle_time": {
		Metric: "cycle_time", Label: "Cycle Time", Unit: "days",
		Table: "work_item_metrics_daily AS m", ValueExpr: "avg(m.cycle_time_p50_hours)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.cycle_time_p50_hours IS NOT NULL AND m.team_id != '' AND " + rowview.OrgPredicate("work_item_metrics_daily", "m"),
		Transform:   hoursToDays, UsePrimaryTeamAttribution: true,
	},
	"wip": {
		Metric: "wip", Label: "WIP", Unit: "items",
		Table: "work_item_metrics_daily AS m", ValueExpr: "avg(m.wip_count_end_of_day)",
		EntityExpr: "m.team_id", LabelExpr: teamLabelExpr,
		WhereClause: "AND m.team_id != '' AND " + rowview.OrgPredicate("work_item_metrics_daily", "m"), Transform: identity,
	},
	// review_load and review_latency weigh each row of a repository by the
	// owner's share of it (owner_share, D5998, CHAOS-9562), so the weights of
	// one repository's owners sum to 1 and the team points conserve the
	// organization's reviews. The load is sum(share x reviews) over the share
	// held by each distinct reviewer (the reviewer's largest share in the
	// group); the latency is the share-weighted mean over the rows that carry
	// one. A sole owner (share 1 everywhere) reads the unweighted values.
	"review_load": {
		Metric: "review_load", Label: "Review Load", Unit: "reviews",
		Table: "user_metrics_daily AS m", ValueExpr: "sum(toFloat64(m.reviews_given) * o.owner_share)",
		ReaderExpr: "m.identity_id", ReaderWeightExpr: "max(o.owner_share)",
		EntityExpr: ownerTeamIDExpr, LabelExpr: ownerTeamLabelExpr,
		Transform: identity, OwnerTeam: true,
	},
	"review_latency": {
		Metric: "review_latency", Label: "Review Latency", Unit: "hours",
		Table:      "user_metrics_daily AS m",
		ValueExpr:  "sum(m.pr_first_review_p50_hours * o.owner_share) / nullIf(sum(o.owner_share), 0)",
		EntityExpr: ownerTeamIDExpr, LabelExpr: ownerTeamLabelExpr,
		WhereClause: "AND m.pr_first_review_p50_hours IS NOT NULL",
		Transform:   identity, OwnerTeam: true,
	},
}

const (
	ownerTeamIDExpr    = "o.owner_team_id"
	ownerTeamLabelExpr = "o.owner_team_label"
)

// ownerTeamRead is an OwnerTeam spec with the ownership join it reads through:
// each row of a repository is a row of every ACTIVE team that owns the
// repository now (a repository of two owners is a point of each), and a
// repository with no owner gives no team a point. The spec's value expression
// reads o.owner_share, the weight of the row in the team's point. selectedTeams
// narrows the owners to the teams of the request AFTER the shares are computed.
func ownerTeamRead(spec MetricSpec, orgID string, selectedTeams []string, asOf time.Time) (MetricSpec, extraScope) {
	shares, bindings := teamscope.OwnerShares(orgID, selectedTeams, asOf)
	spec.JoinClause = "INNER JOIN " + shares + " AS o ON o.owner_repo_id = toString(m.repo_id)"
	return spec, extraScope{
		SQL:      "\n          AND o.owner_team_id IN (" + activeteams.IDsSubquery + ")",
		Bindings: bindings,
	}
}

// RepoMetrics ports REPO_METRICS (quadrant.py:138-206) verbatim.
var RepoMetrics = map[string]MetricSpec{
	"churn": {
		Metric: "churn", Label: "Churn", Unit: "loc",
		Table: "repo_metrics_daily AS m", ValueExpr: "sum(m.total_loc_touched)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos FINAL ON repos.id = m.repo_id AND repos.org_id = {org_id:String}",
		WhereClause: "AND repos.repo != ''", Transform: identity,
	},
	"throughput": {
		Metric: "throughput", Label: "Throughput", Unit: "items",
		Table: "repo_metrics_daily AS m", ValueExpr: "sum(m.prs_merged)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos FINAL ON repos.id = m.repo_id AND repos.org_id = {org_id:String}",
		WhereClause: "AND repos.repo != ''", Transform: identity,
	},
	// cycle_time is the median over every pull request merged in the bucket,
	// one weight per pull request. A mean or median of
	// repo_metrics_daily's daily medians weights a day with one merge the
	// same as a day with 149, so it is not read here. Old pull requests merged
	// late count with their full cycle time. quantileExactInclusive(0.5) is
	// the median repouser.Compute writes per day: the middle value, or the
	// mean of the two middle values.
	"cycle_time": {
		Metric: "cycle_time", Label: "Cycle Time", Unit: "days",
		Table:      "git_pull_requests AS m",
		ValueExpr:  "quantileExactInclusive(0.5)(dateDiff('millisecond', m.created_at, m.merged_at) / 3600000.0)",
		DayExpr:    "toDate(m.merged_at)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos FINAL ON repos.id = m.repo_id AND repos.org_id = {org_id:String}",
		WhereClause: "AND repos.repo != ''",
		Transform:   hoursToDays,
	},
	"wip": {
		Metric: "wip", Label: "WIP", Unit: "items",
		Table: "work_item_metrics_daily AS m", ValueExpr: "avg(m.wip_count_end_of_day)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause: "INNER JOIN repos FINAL ON repos.repo = m.work_scope_id AND repos.org_id = {org_id:String}",
		// One row per work scope: each item once per scope (CHAOS-8863), legacy rows and the scope view.
		WhereClause: "AND m.work_scope_id != '' AND " + rowview.ScopePredicate("work_item_metrics_daily", "m"), Transform: identity,
	},
	"review_load": {
		Metric: "review_load", Label: "Review Load", Unit: "reviews",
		Table: "user_metrics_daily AS m", ValueExpr: "sum(m.reviews_given) / nullIf(countDistinct(m.identity_id), 0)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos FINAL ON repos.id = m.repo_id AND repos.org_id = {org_id:String}",
		WhereClause: "AND repos.repo != ''", Transform: identity,
	},
	"review_latency": {
		Metric: "review_latency", Label: "Review Latency", Unit: "hours",
		Table: "user_metrics_daily AS m", ValueExpr: "avg(m.pr_first_review_p50_hours)",
		EntityExpr: "repos.repo", LabelExpr: "repos.repo",
		JoinClause:  "INNER JOIN repos FINAL ON repos.id = m.repo_id AND repos.org_id = {org_id:String}",
		WhereClause: "AND repos.repo != '' AND m.pr_first_review_p50_hours IS NOT NULL",
		Transform:   identity,
	},
}

// PersonMetrics ports PERSON_METRICS (quadrant.py:208-270) verbatim.
var PersonMetrics = map[string]MetricSpec{
	"churn": {
		Metric: "churn", Label: "Churn", Unit: "loc",
		Table: "user_metrics_daily AS m", ValueExpr: "sum(m.loc_touched)",
		EntityExpr: "m.identity_id", LabelExpr: "m.identity_id",
		WhereClause: "AND m.identity_id != ''", Transform: identity,
	},
	"throughput": {
		Metric: "throughput", Label: "Throughput", Unit: "items",
		Table: "work_item_user_metrics_daily AS m", ValueExpr: "sum(m.items_completed)",
		EntityExpr: "m.user_identity", LabelExpr: "m.user_identity",
		WhereClause: "AND m.user_identity != ''", Transform: identity,
	},
	"cycle_time": {
		Metric: "cycle_time", Label: "Cycle Time", Unit: "days",
		Table: "work_item_user_metrics_daily AS m", ValueExpr: "avg(m.cycle_time_p50_hours)",
		EntityExpr: "m.user_identity", LabelExpr: "m.user_identity",
		WhereClause: "AND m.cycle_time_p50_hours IS NOT NULL AND m.user_identity != ''",
		Transform:   hoursToDays,
	},
	"wip": {
		Metric: "wip", Label: "WIP", Unit: "items",
		Table: "work_item_user_metrics_daily AS m", ValueExpr: "avg(m.wip_count_end_of_day)",
		EntityExpr: "m.user_identity", LabelExpr: "m.user_identity",
		WhereClause: "AND m.user_identity != ''", Transform: identity,
	},
	"review_load": {
		Metric: "review_load", Label: "Review Load", Unit: "reviews",
		Table: "user_metrics_daily AS m", ValueExpr: "sum(m.reviews_given)",
		EntityExpr: "m.identity_id", LabelExpr: "m.identity_id",
		WhereClause: "AND m.identity_id != ''", Transform: identity,
	},
	"review_latency": {
		Metric: "review_latency", Label: "Review Latency", Unit: "hours",
		Table: "user_metrics_daily AS m", ValueExpr: "avg(m.pr_first_review_p50_hours)",
		EntityExpr: "m.identity_id", LabelExpr: "m.identity_id",
		WhereClause: "AND m.identity_id != '' AND m.pr_first_review_p50_hours IS NOT NULL",
		Transform:   identity,
	},
}

// metricsByScope ports METRICS_BY_SCOPE (quadrant.py:272-276) for all three
// group scopes.
var metricsByScope = map[string]map[string]MetricSpec{
	"team":   TeamMetrics,
	"repo":   RepoMetrics,
	"person": PersonMetrics,
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
// dedup_from, restricted to the tables the three ported metric sets
// actually name. All four are ReplacingMergeTree(computed_at) in prod
// today (work_item_metrics_daily/work_item_user_metrics_daily since
// migration 055; user_metrics_daily/repo_metrics_daily since migration
// 096) and take the identical FINAL treatment -- see the package doc
// comment for why user_metrics_daily/repo_metrics_daily no longer take
// the LIMIT-1-BY path dedup_from's reference text still describes for
// them: a bare LIMIT-1-BY sorts and collapses the whole table with no
// per-tenant scope of its own, so FINAL is the correct dedup here.

var replacingMergeTreeTables = map[string]bool{
	"work_item_metrics_daily":      true,
	"work_item_user_metrics_daily": true,
	"user_metrics_daily":           true,
	"repo_metrics_daily":           true,
	// ReplacingMergeTree(last_synced): a pull request synced twice before a
	// merge must count once in the cycle-time median.
	"git_pull_requests": true,
}

// dedupFrom wraps a ReplacingMergeTree daily-family table in FINAL --
// table is "<name>" or "<name> AS <alias>", matching every MetricSpec.
// Table value in this package. Every caller applies its own org_id
// predicate in the SAME top-level SELECT this FROM clause sits in
// (fetchQuadrantMetric's WHERE), never inside a separate subquery here --
// there is deliberately no subquery-based dedup helper in this file for
// that reason, matching filteroptions's own "no wrapping subquery" rule.
func dedupFrom(table string) string {
	base, alias, hasAlias := strings.Cut(table, " AS ")
	if !replacingMergeTreeTables[base] {
		return table
	}
	aliasSQL := ""
	if hasAlias {
		aliasSQL = " AS " + alias
	}
	// Alias BEFORE FINAL: "<table> FINAL AS <alias>" is a ClickHouse
	// SYNTAX error (Code 62), not merely an engine-support one --
	// confirmed live against this exact ClickHouse version. "<table> AS
	// <alias> FINAL" is the only accepted order, matching every
	// hand-written query elsewhere in this tree that already gets it
	// right (e.g. "FROM work_item_cycle_times AS wct FINAL").
	return base + aliasSQL + " FINAL"
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

// bucketWindow is the bucket of a read that aggregates the whole requested
// window into one row per entity: the window's first day.
const bucketWindow = "window"

func bucketExpr(bucket, dayExpr string) string {
	if bucket == bucketWindow {
		return "toDate({start_day:Date})"
	}
	if bucket == "month" {
		return "toStartOfMonth(" + dayExpr + ")"
	}
	return "toStartOfWeek(" + dayExpr + ")"
}

func specDayExpr(spec MetricSpec) string {
	if spec.DayExpr == "" {
		return "day"
	}
	return spec.DayExpr
}

// extraScope is the selected-teams / selected-repositories condition of a request
// (CHAOS-9159): one " AND ..." fragment and its bindings, "" when the request selects
// neither. It is appended to the WHERE of a metric read, after the org_id predicate.
type extraScope struct {
	SQL      string
	Bindings []dhclickhouse.Binding
}

// quadrantMetricQuery builds fetch_quadrant_metric's SQL text (queries/
// quadrant.py:19-58) for one spec/bucket/teamFilter combination -- split
// out from fetchQuadrantMetric so a test can assert the query SHAPE
// (sqlshape.Depths: org_id sits at the same nesting depth as every
// FINAL/JOIN dedup source, never behind an unfiltered subquery) without a
// live QueryClient. Returns exactly the text fetchQuadrantMetric sends;
// bindings are built separately since they carry request VALUES, not
// shape.

// The rows of one entity group on its id alone and carry one label (max): a
// label that changed inside the bucket (a renamed team) must not split the entity
// into two rows, of which the response keeps the last.
func quadrantMetricQuery(spec MetricSpec, bucket, teamFilter string, scopes ...extraScope) string {
	var extra extraScope
	if len(scopes) > 0 {
		extra = scopes[0]
	}
	joinSQL := ""
	if spec.JoinClause != "" {
		joinSQL = "\n" + spec.JoinClause
	}
	whereSQL := ""
	if spec.WhereClause != "" {
		whereSQL = "\n" + spec.WhereClause
	}
	scopeSQL := ""
	if teamFilter != "" {
		scopeSQL = "\nAND m.team_id = {team_id:String}"
	}
	dayExpr := specDayExpr(spec)
	if spec.ReaderExpr != "" {
		return fmt.Sprintf(`
        SELECT
            bucket,
            entity_id,
            max(entity_label) AS label,
            toFloat64(sum(reader_value) / nullIf(sum(reader_weight), 0)) AS value
        FROM (
            SELECT
                %s AS bucket,
                %s AS entity_id,
                max(%s) AS entity_label,
                %s AS reader,
                %s AS reader_value,
                %s AS reader_weight
            FROM %s%s
            WHERE %s >= {start_day:Date} AND %s < {end_day:Date}%s
              AND org_id = {org_id:String}%s%s%s
            GROUP BY bucket, entity_id, reader
        )
        GROUP BY bucket, entity_id
        ORDER BY bucket
    `, bucketExpr(bucket, dayExpr), spec.EntityExpr, spec.LabelExpr, spec.ReaderExpr, spec.ValueExpr, spec.ReaderWeightExpr,
			dedupFrom(spec.Table), joinSQL, dayExpr, dayExpr, whereSQL, scopeSQL, liveRowClause(spec.Table), extra.SQL)
	}
	return fmt.Sprintf(`
        SELECT
            %s AS bucket,
            %s AS entity_id,
            max(%s) AS entity_label,
            toFloat64(%s) AS value
        FROM %s%s
        WHERE %s >= {start_day:Date} AND %s < {end_day:Date}%s
          AND org_id = {org_id:String}%s%s%s
        GROUP BY bucket, entity_id
        ORDER BY bucket
    `, bucketExpr(bucket, dayExpr), spec.EntityExpr, spec.LabelExpr, spec.ValueExpr, dedupFrom(spec.Table), joinSQL, dayExpr, dayExpr, whereSQL, scopeSQL, liveRowClause(spec.Table), extra.SQL)
}

// liveRowClause keeps only the measurements of a team-keyed daily table (see
// package liverow). A retraction row is the newest row of a key the compute
// no longer produces: its team is not a point to plot, and the 0 it holds in
// a count is not a sample of a mean. The table is read with FINAL (dedupFrom),
// so the predicate sees the newest row of each key.
func liveRowClause(table string) string {
	base, alias, hasAlias := strings.Cut(table, " AS ")
	if !liverow.Registered(base) {
		return ""
	}
	if !hasAlias {
		alias = ""
	}
	return "\n          AND " + liverow.Predicate(base, alias)
}

// personIdentityScope binds a person-scope read to the subject's own identity
// strings, on the identity column of the spec's table.
func personIdentityScope(spec MetricSpec, identities []string) extraScope {
	return extraScope{
		SQL:      "\nAND " + spec.EntityExpr + " IN {person_identities:Array(String)}",
		Bindings: []dhclickhouse.Binding{{Name: "person_identities", Value: identities}},
	}
}

// fetchQuadrantMetric ports fetch_quadrant_metric (queries/quadrant.py:
// 19-58). teamFilter ports the scope_filter/scope_params pair
// _cohort_scope_filter builds (quadrant.py:378-381) -- "" (Python's None
// team_filter) means no cohort filter, matching every team/repo group-scope
// call site (every call site passes "" since CHAOS-9343: a person-scope read
// is bound by personIdentityScope, never by a membership team).
func fetchQuadrantMetric(ctx context.Context, client QueryClient, spec MetricSpec, startDay, endDay time.Time, bucket, orgID, teamFilter string) ([]metricRow, error) {
	return fetchQuadrantMetricIn(ctx, client, spec, startDay, endDay, bucket, orgID, teamFilter, extraScope{})
}

// fetchQuadrantMetricIn is fetchQuadrantMetric under the selected teams and
// repositories of the request (extra).
func fetchQuadrantMetricIn(ctx context.Context, client QueryClient, spec MetricSpec, startDay, endDay time.Time, bucket, orgID, teamFilter string, extra extraScope) ([]metricRow, error) {
	bindings := []dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}
	if teamFilter != "" {
		bindings = append(bindings, dhclickhouse.Binding{Name: "team_id", Value: teamFilter})
	}
	bindings = append(bindings, extra.Bindings...)
	query := quadrantMetricQuery(spec, bucket, teamFilter, extra)

	return scanMetricRows(ctx, client, query, bindings, "quadrant: fetch_quadrant_metric")
}

// teamScopedWorkItemTeamAttributionSource reads an item's latest attribution
// rows of its primary team (is_primary = 1) and of each co-owner team
// (is_primary = 2, teamattribution.AttributionCoOwner), one row per (item,
// team), with the weight of the team in the item: the item counts once in the
// organization, split among its teams (D5983, D5998, CHAOS-9562).
//
// The weight of a team is its owner_share of the item's repository
// (teamscope.OwnerShares), renormalised over the item's teams so the weights of
// one item sum to 1. When any team of the item is not an owner of the item's
// repository (a project-only co-owner holds no repository share), or the shares
// sum to nothing, no weight is invented: the teams split the item equally, the
// documented fallback. An item of one team weighs 1. The shares are computed
// over EVERY owner of the repository and the item's teams over EVERY team of the
// item, before any selected-team narrowing, so a team in view keeps its own
// share of the item, never the whole item.
const teamScopedWorkItemTeamAttributionSourceFmt = `(
    SELECT
        w.work_item_id AS work_item_id,
        w.team_id AS team_id,
        w.team_name AS team_name,
        if(
            sum(w.owner_matched) OVER (PARTITION BY w.work_item_id) = count() OVER (PARTITION BY w.work_item_id)
                AND sum(w.owner_share) OVER (PARTITION BY w.work_item_id) > 0,
            w.owner_share / sum(w.owner_share) OVER (PARTITION BY w.work_item_id),
            1.0 / count() OVER (PARTITION BY w.work_item_id)
        ) AS weight
    FROM (
        SELECT
            a.work_item_id AS work_item_id,
            a.tid AS team_id,
            a.team_name AS team_name,
            s.owner_share AS owner_share,
            s.owner_matched AS owner_matched
        FROM (
            SELECT
                work_item_id,
                ifNull(team_id, '') AS tid,
                any(team_name) AS team_name,
                any(repo_id) AS repo_id
            FROM work_item_team_attributions FINAL
            WHERE org_id = {org_id:String}
              AND is_primary IN (1, 2)
              AND team_id IS NOT NULL
              AND team_id != ''
              AND (work_item_id, computed_at) IN (
                  SELECT work_item_id, max(computed_at)
                  FROM work_item_team_attributions
                  WHERE org_id = {org_id:String}
                  GROUP BY work_item_id
              )
            GROUP BY work_item_id, tid
        ) AS a
        LEFT JOIN %s AS s
          ON s.owner_repo_id = toString(a.repo_id) AND s.owner_team_id = a.tid
    ) AS w
)`

// fetchWorkItemTeamQuadrantMetric ports fetch_work_item_team_quadrant_metric
// (queries/quadrant.py:61-108) -- the cycle_throughput/team attribution-quirk
// reader -- with one declared difference: an item of several teams is split
// among them by owner share (teamScopedWorkItemTeamAttributionSourceFmt), so
// throughput is the sum over the team's distinct items of the item weight and
// cycle time is the weighted mean over its items, never the whole item for each
// team (CHAOS-9562).
//
// UNSAFE STATEMENT: this query was a `WITH team_activity AS
// (...) SELECT ...` CTE until this fix: dev-health-go's client-side
// read-only guard (clickhouse/client.go's validateReadOnlyStatement)
// requires a statement's FIRST token to be the literal "SELECT", so a
// query beginning "WITH ..." is rejected before it ever reaches
// ClickHouse (ErrUnsafeStatement, "clickhouse runtime: unsafe
// statement"), wrapped into the generic 503 every cycle_throughput
// quadrant request degraded to, with nothing logged (the defect this
// ticket's telemetry sweep also fixes). "team_activity" is referenced
// exactly once (in the outer FROM), so inlining it as an ordinary
// derived-table subquery is a purely mechanical, semantically identical
// rewrite.
func fetchWorkItemTeamQuadrantMetric(ctx context.Context, client QueryClient, metric string, startDay, endDay time.Time, bucket, orgID string, asOf time.Time) ([]metricRow, error) {
	return fetchWorkItemTeamQuadrantMetricIn(ctx, client, metric, startDay, endDay, bucket, orgID, asOf, extraScope{})
}

// fetchWorkItemTeamQuadrantMetricIn is fetchWorkItemTeamQuadrantMetric under the
// selected teams of the request (extra: a condition on t.team_id). asOf is the
// instant the repository owners are read at.
func fetchWorkItemTeamQuadrantMetricIn(ctx context.Context, client QueryClient, metric string, startDay, endDay time.Time, bucket, orgID string, asOf time.Time, extra extraScope) ([]metricRow, error) {
	query, shareBindings, err := workItemTeamQuadrantQuery(metric, bucket, orgID, asOf, extra)
	if err != nil {
		return nil, err
	}
	slog.Debug("quadrant: team points weigh each item by the owner share of its teams",
		"org_id", orgID, "metric", metric, "bucket", bucket, "as_of", asOf, "teams_selected", len(extra.Bindings) > 0)
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
		{Name: "org_id", Value: orgID},
	}, shareBindings...)
	bindings = append(bindings, extra.Bindings...)
	return scanMetricRows(ctx, client, query, bindings, "quadrant: fetch_work_item_team_quadrant_metric")
}

// workItemTeamQuadrantQuery builds the statement of one attributed team metric
// and the bindings of its owner-share source (the request's own bindings are the
// caller's).
func workItemTeamQuadrantQuery(metric, bucket, orgID string, asOf time.Time, extra extraScope) (string, []dhclickhouse.Binding, error) {
	var outer, metricFilter string
	switch metric {
	case "throughput":
		outer = `
        SELECT
            bucket,
            toString(team_id) AS entity_id,
            ifNull(nullIf(any(team_name), ''), toString(team_id)) AS entity_label,
            toFloat64(sum(item_weight)) AS value
        FROM (
            SELECT
                bucket,
                team_id,
                any(team_name) AS team_name,
                work_item_id,
                any(weight) AS item_weight
            FROM (%[1]s) AS team_activity
            GROUP BY bucket, team_id, work_item_id
        )
        GROUP BY bucket, entity_id
        ORDER BY bucket
    `
	case "cycle_time":
		outer = `
        SELECT
            bucket,
            toString(team_id) AS entity_id,
            ifNull(nullIf(any(team_name), ''), toString(team_id)) AS entity_label,
            toFloat64(sum(cycle_time_hours * weight) / sum(weight)) AS value
        FROM (%[1]s) AS team_activity
        GROUP BY bucket, entity_id
        ORDER BY bucket
    `
		metricFilter = "\n              AND cycle_time_hours IS NOT NULL"
	default:
		return "", nil, fmt.Errorf("quadrant: unsupported attributed team quadrant metric: %s", metric)
	}

	shares, shareBindings := teamscope.OwnerShares(orgID, nil, asOf)
	rows := fmt.Sprintf(`
            SELECT
                %s AS bucket,
                wct.work_item_id AS work_item_id,
                wct.cycle_time_hours AS cycle_time_hours,
                t.team_id AS team_id,
                t.team_name AS team_name,
                t.weight AS weight
            FROM work_item_cycle_times AS wct FINAL
            INNER JOIN %s AS t
              ON t.work_item_id = wct.work_item_id
            WHERE wct.day >= {start_day:Date} AND wct.day < {end_day:Date}
              AND wct.org_id = {org_id:String}
              AND t.team_id IS NOT NULL
              AND t.team_id != ''%s%s
        `, bucketExpr(bucket, "wct.day"), fmt.Sprintf(teamScopedWorkItemTeamAttributionSourceFmt, shares), metricFilter, extra.SQL)
	return fmt.Sprintf(outer, rows), shareBindings, nil
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
