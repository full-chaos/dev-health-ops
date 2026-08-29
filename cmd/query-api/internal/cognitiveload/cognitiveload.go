// Package cognitiveload is the Go port of
// dev_health_ops.api.graphql.resolvers.cognitive_load.resolve_cognitive_load
// (ops/src/dev_health_ops/api/graphql/resolvers/cognitive_load.py),
// CHAOS-4369 Wave 3.
//
// Ported deliberately verbatim, including the TWO distinct read paths the
// Python resolver picks between at the feature-branch tip (see Resolve's
// doc comment):
//
//  1. Single-team (teamId set, repoId NOT set): reads
//     team_cognitive_load_daily directly -- already team-scoped and
//     OWNERSHIP-resolved at write time (CHAOS-4365 item 2). One dedup
//     query, no merge.
//  2. Org-wide OR team+repo COMBINED (teamId unset, OR both teamId and
//     repoId set): the original two-query merge over
//     user_metrics_daily/team_metrics_daily, each deduplicating
//     append-only rows via argMax(..., computed_at) before aggregating,
//     merged over the UNION of days. repoId, when set, narrows only
//     user_metrics_daily (team_metrics_daily has no repo_id FILTER
//     dimension in this path, even though the table itself carries a
//     repo_id column since CHAOS-4329 for its own internal dedup).
//
// IMPORTANT (verified against the actual feature-branch tip, not assumed
// from a briefing): CHAOS-4406's ownership-gated team+repo-combined path
// (resolving team_repo_ownership/teams.repo_patterns to confirm a repo is
// owned by the requesting team before filtering by repo_id alone) exists
// ONLY on origin/main (commit 8519cd2a8) as of this port -- it is NOT an
// ancestor of origin/feature/chaos-4352-go-api's tip
// (`git merge-base --is-ancestor 8519cd2a8
// origin/feature/chaos-4352-go-api` returns false). The Wave 3 task
// briefing's claim that this fix was "already in the feature base" was
// incorrect; this port targets the Python that ACTUALLY exists at the
// feature-branch tip today (team+repo combined still filters
// user_metrics_daily by BOTH team_id AND repo_id -- the same tainted
// team_id column CHAOS-4396/CHAOS-4406 flag, unchanged here since fixing
// it is out of this port's scope). Tracked as CHAOS-4462 (child of
// CHAOS-4352): a future wave that merges main's CHAOS-4406 fix into the
// feature branch will need a follow-up PR to this package.
//
// Both source tables are plain MergeTree (NOT ReplacingMergeTree): a
// recompute/backfill appends a NEW row for the same logical key rather
// than replacing the old one, so every fetch here collapses to the latest
// row per key via argMax(<col>, computed_at) BEFORE aggregating --
// verbatim the same discipline resolvers/complexity.py and this port's
// reviewedges/featureflags siblings already use.
//
// Side effects: none to replicate -- one or more read-only ClickHouse
// queries, no writes, no telemetry/audit hook inside the resolver itself
// (telemetry lives one layer up, in graph/telemetry.go, same convention
// reviewedges/featureflags already established).
//
// Missing-table behavior: like reviewedges (unlike featureflags),
// resolve_cognitive_load has no try/except around any of its ClickHouse
// calls and CognitiveLoadResult has no degradedReason field -- a missing
// table is a real error on the Python side, and this port does not invent
// a degraded path Python doesn't have.
package cognitiveload

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape featureflags.QueryClient and
// reviewedges.QueryClient declare independently (see either package's own
// doc comment for why this is not shared through dev-health-go/readers).
// *clickhouse.Client satisfies this interface directly.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// userLoadRow is one day's summed per-developer load, from
// _fetch_user_metrics.
type userLoadRow struct {
	day                time.Time
	prInterruptionLoad float64
	contextSpreadCount float64
	reviewRequestLoad  float64
}

// teamRatioRow is one day's team-level commit-timing ratios, from
// _fetch_team_metrics. The ratio is always non-null Float64 when a row is
// present for a day (computed via ClickHouse's
// `if(total_commits > 0, ..., 0.0)`) -- absence of a row for a day (not a
// null value within a present row) is what the merge treats as "no team
// signal that day".
type teamRatioRow struct {
	day                   time.Time
	afterHoursCommitRatio float64
	weekendCommitRatio    float64
}

// nonEmpty mirrors Python truthiness for an optional string input field:
// nil and "" both count as "not set".
func nonEmpty(s *string) bool {
	return s != nil && *s != ""
}

// Resolve ports resolve_cognitive_load. orgID must already be the
// AUTHORIZED org (the caller's verified envelope claim, not necessarily
// the client-supplied input.orgId GraphQL argument -- schema.resolvers.go
// passes claims.OrgID here, never input.OrgID, reproducing
// require_org_id's "authorized org always wins" behavior by construction,
// same as reviewedges.Resolve).
func Resolve(ctx context.Context, client QueryClient, orgID string, sinceDate, untilDate graphqldate.Date, teamID, repoID *string) (*model.CognitiveLoadResult, error) {
	if client == nil {
		return nil, errors.New("cognitiveload: clickhouse client is required")
	}

	since := sinceDate.String()
	until := untilDate.String()

	// Path 1: single-team, no repo filter.
	if nonEmpty(teamID) && !nonEmpty(repoID) {
		rows, err := fetchTeamCognitiveLoad(ctx, client, orgID, *teamID, since, until)
		if err != nil {
			return nil, fmt.Errorf("cognitiveload: fetch team cognitive load: %w", err)
		}
		signals := make([]model.CognitiveLoadSignal, 0, len(rows))
		for _, row := range rows {
			signals = append(signals, model.CognitiveLoadSignal{
				Day:                   graphqldate.New(row.day),
				PrInterruptionLoad:    row.prInterruptionLoad,
				ContextSpreadCount:    row.contextSpreadCount,
				ReviewRequestLoad:     row.reviewRequestLoad,
				AfterHoursCommitRatio: row.afterHoursCommitRatio,
				WeekendCommitRatio:    row.weekendCommitRatio,
			})
		}
		return &model.CognitiveLoadResult{
			OrgID:     orgID,
			TeamID:    teamID,
			Signals:   signals,
			TotalDays: len(signals),
		}, nil
	}

	// Path 2: org-wide (teamID unset) OR team+repo combined (both set --
	// team_cognitive_load_daily has no repo_id dimension to filter by, so
	// this falls through to the same two-query merge Python's
	// resolve_cognitive_load uses; repoID narrows user_metrics_daily
	// only, team_metrics_daily is filtered by teamID alone).
	userRows, err := fetchUserMetrics(ctx, client, orgID, since, until, teamID, repoID)
	if err != nil {
		return nil, fmt.Errorf("cognitiveload: fetch user metrics: %w", err)
	}
	teamRows, err := fetchTeamMetrics(ctx, client, orgID, since, until, teamID)
	if err != nil {
		return nil, fmt.Errorf("cognitiveload: fetch team metrics: %w", err)
	}
	signals := mergeUserAndTeamRows(userRows, teamRows)
	return &model.CognitiveLoadResult{
		OrgID:     orgID,
		TeamID:    teamID,
		Signals:   signals,
		TotalDays: len(signals),
	}, nil
}

// mergeUserAndTeamRows outer-joins userRows/teamRows on day into signals,
// porting resolve_cognitive_load's inline merge exactly: a day present in
// only one side is still emitted, with zero/nil for the missing side's
// fields.
func mergeUserAndTeamRows(userRows []userLoadRow, teamRows []teamRatioRow) []model.CognitiveLoadSignal {
	userByDay := make(map[time.Time]userLoadRow, len(userRows))
	for _, row := range userRows {
		userByDay[row.day] = row
	}
	teamByDay := make(map[time.Time]teamRatioRow, len(teamRows))
	for _, row := range teamRows {
		teamByDay[row.day] = row
	}

	daySet := make(map[time.Time]struct{}, len(userRows)+len(teamRows))
	for _, row := range userRows {
		daySet[row.day] = struct{}{}
	}
	for _, row := range teamRows {
		daySet[row.day] = struct{}{}
	}
	days := make([]time.Time, 0, len(daySet))
	for d := range daySet {
		days = append(days, d)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })

	signals := make([]model.CognitiveLoadSignal, 0, len(days))
	for _, d := range days {
		sig := model.CognitiveLoadSignal{Day: graphqldate.New(d)}
		if u, ok := userByDay[d]; ok {
			sig.PrInterruptionLoad = u.prInterruptionLoad
			sig.ContextSpreadCount = u.contextSpreadCount
			sig.ReviewRequestLoad = u.reviewRequestLoad
		}
		if t, ok := teamByDay[d]; ok {
			afterHours := t.afterHoursCommitRatio
			weekend := t.weekendCommitRatio
			sig.AfterHoursCommitRatio = &afterHours
			sig.WeekendCommitRatio = &weekend
		}
		signals = append(signals, sig)
	}
	return signals
}

// ---------------------------------------------------------------------------
// ClickHouse fetch helpers
// ---------------------------------------------------------------------------

// fetchUserMetrics ports _fetch_user_metrics: SUM of latest-per-developer
// cognitive load columns, grouped by day, deduped via
// argMax(<col>, computed_at) per (day, repo_id, author_email) before the
// outer SUM. team_id/repo_id filters are added only when non-empty,
// exactly mirroring the Python function's conditional WHERE-clause
// construction. repoId accepts either a repos.id UUID or a repos.repo
// slug, resolved through an org-scoped subquery -- same shape
// reviewedges.Resolve's own repo filter uses.
func fetchUserMetrics(ctx context.Context, client QueryClient, orgID, sinceDate, untilDate string, teamID, repoID *string) ([]userLoadRow, error) {
	innerWhere := `
            WHERE org_id = {org_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}`
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_date", Value: sinceDate},
		{Name: "until_date", Value: untilDate},
	}
	if nonEmpty(teamID) {
		innerWhere += "\n              AND team_id = {team_id:String}"
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: *teamID})
	}
	if nonEmpty(repoID) {
		innerWhere += `
              AND repo_id IN (
                  SELECT id FROM repos
                  WHERE org_id = {org_id:String}
                    AND (repo = {repo_id:String} OR toString(id) = {repo_id:String})
              )`
		bindings = append(bindings, clickhouse.Binding{Name: "repo_id", Value: *repoID})
	}

	query := `
        SELECT
            day,
            SUM(pr_interruption_load) AS pr_interruption_load,
            SUM(context_spread_count) AS context_spread_count,
            SUM(review_request_load)  AS review_request_load
        FROM (
            SELECT
                day,
                repo_id,
                author_email,
                argMax(pr_interruption_load, computed_at) AS pr_interruption_load,
                argMax(context_spread_count, computed_at) AS context_spread_count,
                argMax(review_request_load,  computed_at) AS review_request_load
            FROM user_metrics_daily
            ` + innerWhere + `
            GROUP BY day, repo_id, author_email
        )
        GROUP BY day
        ORDER BY day`

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []userLoadRow
	for rows.Next() {
		var day time.Time
		// pr_interruption_load/context_spread_count/review_request_load
		// are UInt32 columns (migration 016); SUM(UInt32) widens to
		// UInt64 in ClickHouse -- scan into uint64 first, same
		// widen-then-convert discipline reviewedges.Resolve uses for
		// reviews_count (UInt32), never a signed destination.
		var pr, cs, rr uint64
		if scanErr := rows.Scan(&day, &pr, &cs, &rr); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		result = append(result, userLoadRow{
			day:                day,
			prInterruptionLoad: float64(pr),
			contextSpreadCount: float64(cs),
			reviewRequestLoad:  float64(rr),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
}

// fetchTeamMetrics ports _fetch_team_metrics: AVG across teams of each
// team's after-hours/weekend commit ratio, by day. Four layers, verbatim
// from Python (see that function's docstring for why each layer exists --
// CHAOS-4329 legacy repo_id=” bucket exclusion, SUM-then-recompute
// discipline since a ratio is not additive across repos, then AVG across
// teams). When teamID is set, filters to that team; otherwise averages
// across all teams for an org-wide signal. Takes NO repoID filter -- this
// function has never accepted one at the feature-branch tip (Python's own
// docstring: "_fetch_team_metrics does not (yet) accept a repo_id
// filter"), so the team+repo-combined path's repoID narrows
// fetchUserMetrics only, never this function.
func fetchTeamMetrics(ctx context.Context, client QueryClient, orgID, sinceDate, untilDate string, teamID *string) ([]teamRatioRow, error) {
	innerWhere := `
            WHERE org_id = {org_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}`
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_date", Value: sinceDate},
		{Name: "until_date", Value: untilDate},
	}
	if nonEmpty(teamID) {
		innerWhere += "\n              AND team_id = {team_id:String}"
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: *teamID})
	}

	query := `
        SELECT
            day,
            AVG(after_hours_commit_ratio) AS after_hours_commit_ratio,
            AVG(weekend_commit_ratio)     AS weekend_commit_ratio
        FROM (
            SELECT
                day,
                team_id,
                sum(commits_count)             AS total_commits,
                sum(after_hours_commits_count) AS total_after_hours_commits,
                sum(weekend_commits_count)      AS total_weekend_commits,
                if(total_commits > 0,
                   total_after_hours_commits / total_commits, 0.0
                ) AS after_hours_commit_ratio,
                if(total_commits > 0,
                   total_weekend_commits / total_commits, 0.0
                ) AS weekend_commit_ratio
            FROM (
                SELECT day, team_id, repo_id, commits_count, after_hours_commits_count, weekend_commits_count
                FROM (
                    SELECT
                        day,
                        team_id,
                        repo_id,
                        argMax(commits_count,             computed_at) AS commits_count,
                        argMax(after_hours_commits_count, computed_at) AS after_hours_commits_count,
                        argMax(weekend_commits_count,      computed_at) AS weekend_commits_count,
                        countIf(repo_id != '') OVER (PARTITION BY day, team_id) AS real_repo_count
                    FROM team_metrics_daily
                    ` + innerWhere + `
                    GROUP BY day, team_id, repo_id
                )
                WHERE repo_id != '' OR real_repo_count = 0
            )
            GROUP BY day, team_id
        )
        GROUP BY day
        ORDER BY day`

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []teamRatioRow
	for rows.Next() {
		var day time.Time
		var afterHours, weekend float64
		if scanErr := rows.Scan(&day, &afterHours, &weekend); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		result = append(result, teamRatioRow{day: day, afterHoursCommitRatio: afterHours, weekendCommitRatio: weekend})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
}

// fullDayRow is one day's complete signal read directly from
// team_cognitive_load_daily (fetchTeamCognitiveLoad) -- unlike
// userLoadRow/teamRatioRow, the ratio fields here ARE genuinely nullable
// (migration 081: Nullable(Float64), NULL meaning "unmeasured", distinct
// from a measured 0.0).
type fullDayRow struct {
	day                   time.Time
	prInterruptionLoad    float64
	contextSpreadCount    float64
	reviewRequestLoad     float64
	afterHoursCommitRatio *float64
	weekendCommitRatio    *float64
}

// fetchTeamCognitiveLoad ports _fetch_team_cognitive_load (CHAOS-4365 item
// 2, single-team path): reads team_cognitive_load_daily directly for one
// team -- already team-scoped and OWNERSHIP-resolved at write time, so
// this is a single dedup read, not a merge of two tables.
//
// Bundles every field into one argMax(tuple(...), computed_at) rather
// than five independent per-column argMax calls -- an independent
// per-column argMax silently skips NULL arguments, so a day recomputed
// from "measured" to "unmeasured" (the latest row's ratio genuinely NULL)
// would keep returning a STALE non-null ratio from an older row instead
// of the latest row's true NULL. The tuple form picks the whole row
// atomically from the single latest computed_at, so a NULL in the latest
// row stays NULL -- ports the Python resolver's codex-R1 fix exactly.
func fetchTeamCognitiveLoad(ctx context.Context, client QueryClient, orgID, teamID, sinceDate, untilDate string) ([]fullDayRow, error) {
	query := `
        SELECT
            day,
            tupleElement(latest_row, 1) AS pr_interruption_load,
            tupleElement(latest_row, 2) AS context_spread_count,
            tupleElement(latest_row, 3) AS review_request_load,
            tupleElement(latest_row, 4) AS after_hours_commit_ratio,
            tupleElement(latest_row, 5) AS weekend_commit_ratio
        FROM (
            SELECT
                day,
                argMax(
                    tuple(
                        pr_interruption_load,
                        context_spread_count,
                        review_request_load,
                        after_hours_commit_ratio,
                        weekend_commit_ratio
                    ),
                    computed_at
                ) AS latest_row
            FROM team_cognitive_load_daily
            WHERE org_id = {org_id:String}
              AND team_id = {team_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}
            GROUP BY day
        )
        ORDER BY day`
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "team_id", Value: teamID},
		{Name: "since_date", Value: sinceDate},
		{Name: "until_date", Value: untilDate},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []fullDayRow
	for rows.Next() {
		var day time.Time
		var pr, cs, rr float64
		var afterHours, weekend *float64
		if scanErr := rows.Scan(&day, &pr, &cs, &rr, &afterHours, &weekend); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		result = append(result, fullDayRow{
			day:                   day,
			prInterruptionLoad:    pr,
			contextSpreadCount:    cs,
			reviewRequestLoad:     rr,
			afterHoursCommitRatio: afterHours,
			weekendCommitRatio:    weekend,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
}
