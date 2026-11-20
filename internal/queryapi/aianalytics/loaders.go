package aianalytics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// impactNewestOrder orders the versions of one ai_impact_metrics_daily key:
// the newest computed_at, then a hash of the version's values, so two versions
// that share a computed_at serve one of them whatever the scan order. Every
// read of the table in this package orders by it, so they pick the same version.
const impactNewestOrder = `(metrics.computed_at, cityHash64(tuple(
    metrics.prs_total, metrics.prs_merged, metrics.ai_assisted_prs, metrics.agent_created_prs,
    metrics.human_prs, metrics.unknown_prs, metrics.ai_assisted_pr_ratio, metrics.cycle_time_avg_hours,
    metrics.ai_cycle_time_delta_hours, metrics.reviews_per_pr, metrics.ai_review_amplification,
    metrics.changes_requested_per_pr, metrics.rework_prs, metrics.rework_drag_rate,
    metrics.followup_commits_count, metrics.revert_prs, metrics.revert_rate, metrics.incidents_count,
    metrics.incident_drag_rate, metrics.test_gap_prs, metrics.test_gap_rate, metrics.leverage_prs_component,
    metrics.leverage_cycle_time_component, metrics.leverage_review_component,
    metrics.leverage_rework_component, metrics.leverage_test_component, metrics.leverage_incident_component)))`

// The value columns come from ONE version (one argMax over a tuple), never a
// column of one version and a column of another. The tuple is not Nullable, so
// a NULL of the newest version stays NULL: argMax over a Nullable column would
// skip it and serve the older value under it.
const dailyStatement = `SELECT
    team_id,
    toString(repo_id) AS repo_id_str,
    day,
    attribution_bucket,
    newest.1 AS prs_total,
    newest.2 AS prs_merged,
    newest.3 AS ai_assisted_prs,
    newest.4 AS agent_created_prs,
    newest.5 AS human_prs,
    newest.6 AS unknown_prs,
    newest.7 AS ai_assisted_pr_ratio,
    newest.8 AS cycle_time_avg_hours,
    newest.9 AS ai_cycle_time_delta_hours,
    newest.10 AS reviews_per_pr,
    newest.11 AS ai_review_amplification,
    newest.12 AS changes_requested_per_pr,
    newest.13 AS rework_prs,
    newest.14 AS rework_drag_rate,
    newest.15 AS followup_commits_count,
    newest.16 AS revert_prs,
    newest.17 AS revert_rate,
    newest.18 AS incidents_count,
    newest.19 AS incident_drag_rate,
    newest.20 AS test_gap_prs,
    newest.21 AS test_gap_rate,
    newest.22 AS leverage_prs_component,
    newest.23 AS leverage_cycle_time_component,
    newest.24 AS leverage_review_component,
    newest.25 AS leverage_rework_component,
    newest.26 AS leverage_test_component,
    newest.27 AS leverage_incident_component,
    computed_at
FROM (
    SELECT
        team_id,
        repo_id,
        work_type,
        day,
        attribution_bucket,
        argMax(tuple(
            metrics.prs_total, metrics.prs_merged, metrics.ai_assisted_prs, metrics.agent_created_prs,
            metrics.human_prs, metrics.unknown_prs, metrics.ai_assisted_pr_ratio, metrics.cycle_time_avg_hours,
            metrics.ai_cycle_time_delta_hours, metrics.reviews_per_pr, metrics.ai_review_amplification,
            metrics.changes_requested_per_pr, metrics.rework_prs, metrics.rework_drag_rate,
            metrics.followup_commits_count, metrics.revert_prs, metrics.revert_rate, metrics.incidents_count,
            metrics.incident_drag_rate, metrics.test_gap_prs, metrics.test_gap_rate,
            metrics.leverage_prs_component, metrics.leverage_cycle_time_component,
            metrics.leverage_review_component, metrics.leverage_rework_component,
            metrics.leverage_test_component, metrics.leverage_incident_component
        ), ` + impactNewestOrder + `) AS newest,
        max(metrics.computed_at) AS computed_at
    FROM ai_impact_metrics_daily AS metrics
    WHERE %s
    GROUP BY org_id, team_id, repo_id, work_type, day, attribution_bucket
)
ORDER BY day, repo_id, work_type, attribution_bucket`

// dateValue is a Date parameter's text form: the UTC calendar day.
func dateValue(t time.Time) string { return t.UTC().Format("2006-01-02") }

// loadDaily reads the rollup rows of one org for a day window, narrowed by
// repository, stored team id and work type.
func loadDaily(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, sc scope) ([]dailyRow, error) {
	bindings := []clickhouse.Binding{
		{Name: "start_day", Value: dateValue(startDay)},
		{Name: "end_day", Value: dateValue(endDay)},
		{Name: "org_id", Value: orgID},
	}
	filters := []string{"day >= {start_day:Date}", "day <= {end_day:Date}"}
	if sc.repoID != "" {
		bindings = append(bindings, clickhouse.Binding{Name: "repo_id", Value: sc.repoID})
		filters = append(filters, "repo_id = {repo_id:UUID}")
	}
	if sc.teamID != "" {
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: sc.teamID})
		filters = append(filters, "team_id = {team_id:String}")
	}
	if sc.workType != "" {
		bindings = append(bindings, clickhouse.Binding{Name: "work_type", Value: sc.workType})
		filters = append(filters, "work_type = {work_type:String}")
	}
	filters = append(filters, "org_id = {org_id:String}")
	rs, err := client.Query(ctx, fmt.Sprintf(dailyStatement, strings.Join(filters, " AND ")), bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: daily query: %w", err)
	}
	defer rs.Close()
	var out []dailyRow
	for rs.Next() {
		var (
			r                                                          dailyRow
			day                                                        time.Time
			prsTotal, prsMerged, aiPrs, agentPrs, humanPrs, unknownPrs float64
			reworkPrs, followups, revertPrs, incidents, testGapPrs     float64
		)
		if err := rs.Scan(
			&r.TeamID, &r.RepoID, &day, &r.Bucket,
			&prsTotal, &prsMerged, &aiPrs, &agentPrs, &humanPrs, &unknownPrs,
			&r.AIAssistedPrRatio, &r.CycleTimeAvgHours, &r.AICycleTimeDelta,
			&r.ReviewsPerPr, &r.AIReviewAmp, &r.ChangesRequestedPer,
			&reworkPrs, &r.ReworkDragRate, &followups, &revertPrs, &r.RevertRate,
			&incidents, &r.IncidentDragRate, &testGapPrs, &r.TestGapRate,
			&r.LevPrs, &r.LevCycle, &r.LevReview, &r.LevRework, &r.LevTest, &r.LevIncident,
			&r.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("aianalytics: daily scan: %w", err)
		}
		r.Day = day.UTC().Format("2006-01-02")
		r.DayTime = day.UTC()
		r.PrsTotal, r.PrsMerged = prsTotal, prsMerged
		r.AIAssistedPrs, r.AgentCreatedPrs = aiPrs, agentPrs
		r.HumanPrs, r.UnknownPrs = humanPrs, unknownPrs
		r.ReworkPrs, r.FollowupCommits, r.RevertPrs = reworkPrs, followups, revertPrs
		r.IncidentsCount, r.TestGapPrs = incidents, testGapPrs
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: daily rows: %w", err)
	}
	return out, nil
}

// loadLabels maps ids to display names with one org-scoped lookup. table is
// "repos" (name column repo) or "teams" (name column name).
func loadLabels(ctx context.Context, client QueryClient, orgID, table string, ids []string) (map[string]string, error) {
	labels := map[string]string{}
	if len(ids) == 0 {
		return labels, nil
	}
	nameCol := "name"
	if table == "repos" {
		nameCol = "repo"
	}
	statement := fmt.Sprintf(`SELECT toString(id) AS label_id, coalesce(%s, '') AS label
FROM %s
WHERE toString(id) IN {ids:Array(String)}
  AND org_id = {org_id:String}`, nameCol, table)
	rs, err := client.Query(ctx, statement, []clickhouse.Binding{
		{Name: "ids", Value: ids},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		return nil, fmt.Errorf("aianalytics: %s labels: %w", table, err)
	}
	defer rs.Close()
	for rs.Next() {
		var id, label string
		if err := rs.Scan(&id, &label); err != nil {
			return nil, fmt.Errorf("aianalytics: %s labels scan: %w", table, err)
		}
		if label == "" {
			label = id
		}
		labels[id] = label
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: %s labels rows: %w", table, err)
	}
	return labels, nil
}

// engagementRow is one (bucket, day) review-engagement aggregate.
type engagementRow struct {
	Bucket             string
	Day                string
	PrsWithFirstReview float64
	PickupLatency      *float64
	CommentsTotal      int64
	LocTotal           int64
}

const engagementStatement = `SELECT
    bucket,
    day,
    toUInt64(ifNull(prs_with_first_review, 0)) AS prs_with_first_review,
    toNullable(pickup_latency_hours) AS pickup_latency_hours,
    toInt64(ifNull(review_comments_total, 0)) AS review_comments_total,
    toInt64(ifNull(loc_total, 0)) AS loc_total
FROM (
SELECT
    multiIf(
        attr_map.kind IN ('ai_assisted', 'agent_created', 'ai_review'),
        attr_map.kind,
        ifNull(attr_map.kind, '') NOT IN ('', 'human'), 'unknown',
        endsWith(lower(trim(BOTH ' ' FROM ifNull(pr.author_name, ''))), '[bot]'), 'bot',
        'human'
    ) AS bucket,
    toDate(if(pr.merged_at IS NOT NULL, pr.merged_at, pr.created_at)) AS day,
    countIf(
        pr.first_review_at IS NOT NULL
        AND pr.first_review_at >= pr.created_at
    ) AS prs_with_first_review,
    avgIf(
        dateDiff('second', pr.created_at, pr.first_review_at) / 3600.0,
        pr.first_review_at IS NOT NULL
        AND pr.first_review_at >= pr.created_at
    ) AS pickup_latency_hours,
    sum(pr.comments_count) AS review_comments_total,
    sum(
        coalesce(pr.additions, 0) + coalesce(pr.deletions, 0)
    ) AS loc_total
FROM (
    %[1]s
) AS pr
LEFT JOIN (
    %[2]s
) AS attr_map
    ON attr_map.repo_id = pr.repo_id AND attr_map.number = pr.number
WHERE %[3]s
GROUP BY bucket, day
ORDER BY day, bucket
)
ORDER BY day, bucket`

const eventExpr = "if(pr.merged_at IS NOT NULL, pr.merged_at, pr.created_at)"

func eventWindowFilter() string {
	return "(" + eventExpr + " >= {start:DateTime64(3, 'UTC')} AND " + eventExpr + " < {end:DateTime64(3, 'UTC')})"
}

// scopeFilter narrows the raw pull-request table to one repository and/or a
// repository list.
func scopeFilter(bindings *[]clickhouse.Binding, repoID string, repoIDs []string, useIDs bool) string {
	out := ""
	if repoID != "" {
		*bindings = append(*bindings, clickhouse.Binding{Name: "repo_id", Value: repoID})
		out += "\n      AND pr.repo_id = {repo_id:UUID}"
	}
	if useIDs {
		*bindings = append(*bindings, clickhouse.Binding{Name: "repo_ids", Value: repoIDs})
		out += "\n      AND toString(pr.repo_id) IN {repo_ids:Array(String)}"
	}
	return out
}

// dedupedPRs collapses git_pull_requests to the latest version of each
// (repo, number) inside the event window. The candidate stage keeps every key
// with ANY version in the window because merged_at changes between versions.
func dedupedPRs(bindings *[]clickhouse.Binding, repoID string, repoIDs []string, useIDs bool) string {
	sf := scopeFilter(bindings, repoID, repoIDs, useIDs)
	return `SELECT
        pr.repo_id AS repo_id,
        pr.number AS number,
        argMax(pr.created_at, pr.last_synced) AS created_at,
        argMax(pr.merged_at, pr.last_synced) AS merged_at,
        argMax(pr.first_review_at, pr.last_synced) AS first_review_at,
        argMax(pr.comments_count, pr.last_synced) AS comments_count,
        argMax(pr.additions, pr.last_synced) AS additions,
        argMax(pr.deletions, pr.last_synced) AS deletions,
        argMax(pr.author_name, pr.last_synced) AS author_name
    FROM git_pull_requests AS pr
    WHERE 1 = 1
      ` + sf + `
      AND pr.org_id = {org_id:String}
      AND (pr.repo_id, pr.number) IN (
          SELECT pr.repo_id, pr.number
          FROM git_pull_requests AS pr
          WHERE ` + eventWindowFilter() + `
            ` + sf + `
            AND pr.org_id = {org_id:String}
      )
    GROUP BY pr.repo_id, pr.number`
}

// prAttributionSubquery is the deduplicated (repo, number, kind) map over
// both attribution linkage paths, org-scoped on every table.
const prAttributionSubquery = `SELECT repo_id, number, any(kind) AS kind
    FROM (
        SELECT
            link.repo_id AS repo_id,
            link.pr_number AS number,
            attr.kind AS kind
        FROM work_graph_issue_pr AS link
        INNER JOIN ai_attribution_resolved AS attr
            ON attr.subject_type = 'pull_request'
            AND attr.subject_id = link.work_item_id
        WHERE (attr.repo_id IS NULL OR attr.repo_id = link.repo_id)
          AND link.org_id = {org_id:String} AND toString(attr.org_id) = {org_id:String}
        UNION ALL
        SELECT
            attr.repo_id AS repo_id,
            toUInt32OrZero(
                arrayElement(splitByChar('#', attr.subject_id), -1)
            ) AS number,
            attr.kind AS kind
        FROM ai_attribution_resolved AS attr
        WHERE attr.subject_type = 'pull_request'
          AND number > 0
          AND toString(attr.org_id) = {org_id:String}
    )
    GROUP BY repo_id, number`

func loadEngagement(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, repoID string, repoIDs []string, useIDs bool) ([]engagementRow, error) {
	start, end := dayBounds(startDay, endDay)
	bindings := []clickhouse.Binding{
		{Name: "start", Value: start},
		{Name: "end", Value: end},
		{Name: "org_id", Value: orgID},
	}
	pr := dedupedPRs(&bindings, repoID, repoIDs, useIDs)
	statement := fmt.Sprintf(engagementStatement, pr, prAttributionSubquery, eventWindowFilter())
	rs, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: engagement query: %w", err)
	}
	defer rs.Close()
	var out []engagementRow
	for rs.Next() {
		var (
			r      engagementRow
			day    *time.Time
			withFR uint64
		)
		if err := rs.Scan(&r.Bucket, &day, &withFR, &r.PickupLatency, &r.CommentsTotal, &r.LocTotal); err != nil {
			return nil, fmt.Errorf("aianalytics: engagement scan: %w", err)
		}
		if day == nil {
			continue
		}
		r.Day = day.UTC().Format("2006-01-02")
		r.PrsWithFirstReview = float64(withFR)
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: engagement rows: %w", err)
	}
	return out, nil
}

// The inner read picks ONE version of each (repository, reviewer, day): the
// newest computed_at, then a hash of its values, so two versions that share a
// computed_at serve one of them whatever the scan order. The outer WHERE leaves
// out a key whose picked version is a retraction row (package liverow), tested
// on that same version.
var concentrationStatement = `SELECT toFloat64(ifNull(sum(reviews_given), 0)) AS reviews_total
FROM (
    SELECT
        umd.repo_id,
        umd.author_email,
        umd.day,
        argMax(
            tuple(umd.reviews_given, umd.retracted),
            (umd.computed_at, cityHash64(tuple(umd.reviews_given, umd.retracted)))
        ) AS picked,
        picked.1 AS reviews_given,
        picked.2 AS retracted
    FROM user_metrics_daily AS umd
    INNER JOIN (
        SELECT DISTINCT attr.repo_id AS repo_id
        FROM ai_attribution_resolved AS attr
        WHERE attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
          AND toDate(attr.observed_at) >= {start_day:Date}
          AND toDate(attr.observed_at) <= {end_day:Date}
          AND toString(attr.org_id) = {org_id:String}
    ) AS ai_repos ON ai_repos.repo_id = umd.repo_id
    WHERE %s
    GROUP BY umd.repo_id, umd.author_email, umd.day
) AS newest
WHERE ` + liverow.Predicate("user_metrics_daily", "newest") + `
GROUP BY author_email`

// loadReviewerConcentration returns the Gini coefficient of per-reviewer
// review counts over repositories with AI-attributed PRs, and the reviewer
// count; a nil coefficient means no reviewer rows. A team scope is the
// repositories the team owns (teamRepoIDs), never user_metrics_daily.team_id,
// which is a person's membership label (CHAOS-14101, CHAOS-4321).
func loadReviewerConcentration(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, sc scope, heldRepoIDs []string) (*float64, int, error) {
	bindings := []clickhouse.Binding{
		{Name: "start_day", Value: dateValue(startDay)},
		{Name: "end_day", Value: dateValue(endDay)},
		{Name: "org_id", Value: orgID},
	}
	filters := []string{"umd.day >= {start_day:Date}", "umd.day <= {end_day:Date}"}
	if sc.repoID != "" {
		bindings = append(bindings, clickhouse.Binding{Name: "repo_id", Value: sc.repoID})
		filters = append(filters, "umd.repo_id = {repo_id:UUID}")
	}
	if sc.teamID != "" {
		bindings = append(bindings, clickhouse.Binding{Name: "team_repo_ids", Value: heldRepoIDs})
		filters = append(filters, "toString(umd.repo_id) IN {team_repo_ids:Array(String)}")
	}
	filters = append(filters, "umd.org_id = {org_id:String}")
	rs, err := client.Query(ctx, fmt.Sprintf(concentrationStatement, strings.Join(filters, " AND ")), bindings)
	if err != nil {
		return nil, 0, fmt.Errorf("aianalytics: reviewer concentration query: %w", err)
	}
	defer rs.Close()
	var loads []float64
	for rs.Next() {
		var v float64
		if err := rs.Scan(&v); err != nil {
			return nil, 0, fmt.Errorf("aianalytics: reviewer concentration scan: %w", err)
		}
		loads = append(loads, v)
	}
	if err := rs.Err(); err != nil {
		return nil, 0, fmt.Errorf("aianalytics: reviewer concentration rows: %w", err)
	}
	if len(loads) == 0 {
		return nil, 0, nil
	}
	g := gini(loads)
	return &g, len(loads), nil
}

// gini is the Gini coefficient of the values. The reference sums floats with
// CPython's compensated sum, so the same summation is used here.
func gini(values []float64) float64 {
	if len(values) == 0 {
		return 0.0
	}
	total := pythonparity.Sum(values)
	if total == 0.0 {
		return 0.0
	}
	sorted := append([]float64(nil), values...)
	sortFloats(sorted)
	count := float64(len(sorted))
	weighted := make([]float64, len(sorted))
	for i, v := range sorted {
		weighted[i] = float64(i+1) * v
	}
	weightedSum := pythonparity.Sum(weighted)
	return (2.0*weightedSum)/(count*total) - (count+1.0)/count
}
