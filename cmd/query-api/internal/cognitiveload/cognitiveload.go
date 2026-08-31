// Package cognitiveload is the Go port of
// dev_health_ops.api.graphql.resolvers.cognitive_load.resolve_cognitive_load
// (ops/src/dev_health_ops/api/graphql/resolvers/cognitive_load.py),
// CHAOS-4369 Wave 3 (initial 2-path port), extended to the THIRD path by
// CHAOS-4462 after the feature branch took origin/main's CHAOS-4406 fix
// (commit 8519cd2a8) via the CHAOS-4352 rebase lane.
//
// Ported deliberately verbatim, including the THREE distinct read paths
// the Python resolver picks between (see Resolve's doc comment):
//
//  1. Single-team (teamId set, repoId NOT set): reads
//     team_cognitive_load_daily directly -- already team-scoped and
//     OWNERSHIP-resolved at write time (CHAOS-4365 item 2). One dedup
//     query, no merge.
//  2. Team+repo COMBINED (both set, CHAOS-4406/CHAOS-4462): neither
//     user_metrics_daily's nor team_metrics_daily's own team_id column can
//     be trusted (CHAOS-4396 taint -- author-membership fallback, or
//     unset for a native org with empty repo_patterns). resolveOwnedRepoID
//     confirms via team_repo_ownership (falling back to teams.repo_patterns
//     only when native ownership resolves no row at all) that the
//     requested repo is CURRENTLY, CANONICALLY owned by the requested
//     team. If not owned (or the repo does not exist), returns an
//     explicit empty result rather than either the wrong team's data or a
//     confusing error. If owned, both queries filter by repo_id ALONE --
//     never team_id -- since ownership already scopes every signal for
//     that repo to this team.
//  3. Org-wide (teamId unset): the original two-query merge over
//     user_metrics_daily/team_metrics_daily, each deduplicating
//     append-only rows via argMax(..., computed_at) before aggregating,
//     merged over the UNION of days. repoId, when set (without teamId),
//     narrows only user_metrics_daily (team_metrics_daily has no repo_id
//     FILTER dimension in this path, even though the table itself carries
//     a repo_id column since CHAOS-4329 for its own internal dedup).
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
	"strings"
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

	// Path 2 (CHAOS-4406/CHAOS-4462): team+repo combined -- ownership-gated.
	// team_cognitive_load_daily has no repo_id dimension, and neither
	// user_metrics_daily's nor team_metrics_daily's own team_id column can
	// be trusted for this case (CHAOS-4396 taint), so this resolves
	// ownership FIRST and then filters both fetches by repoId alone.
	if nonEmpty(teamID) && nonEmpty(repoID) {
		ownedRepoID, found, err := resolveOwnedRepoID(ctx, client, orgID, *teamID, *repoID)
		if err != nil {
			return nil, fmt.Errorf("cognitiveload: resolve owned repo: %w", err)
		}
		if !found {
			return &model.CognitiveLoadResult{
				OrgID:     orgID,
				TeamID:    teamID,
				Signals:   []model.CognitiveLoadSignal{},
				TotalDays: 0,
			}, nil
		}
		userRows, err := fetchUserMetrics(ctx, client, orgID, since, until, nil, &ownedRepoID)
		if err != nil {
			return nil, fmt.Errorf("cognitiveload: fetch user metrics: %w", err)
		}
		teamRows, err := fetchRepoScopedTeamMetrics(ctx, client, orgID, since, until, ownedRepoID)
		if err != nil {
			return nil, fmt.Errorf("cognitiveload: fetch repo-scoped team metrics: %w", err)
		}
		signals := mergeUserAndTeamRows(userRows, teamRows)
		return &model.CognitiveLoadResult{
			OrgID:     orgID,
			TeamID:    teamID,
			Signals:   signals,
			TotalDays: len(signals),
		}, nil
	}

	// Path 3: org-wide -- teamID unset (repoID, when set without teamID,
	// narrows user_metrics_daily only; team_metrics_daily is filtered by
	// teamID alone, which is always nil/empty here).
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

// ---------------------------------------------------------------------------
// team_repo_ownership (CHAOS-4406/CHAOS-4462, team+repo combined path)
// ---------------------------------------------------------------------------

// repoCandidate is one row from the repos lookup: repoID resolves EITHER a
// repos.id UUID or a repos.repo slug to a concrete repo, keeping both the
// id (for ownership matching) and the repo slug (for the repo_patterns
// fallback, which matches on the slug, never the UUID).
type repoCandidate struct {
	id   string
	repo string
}

// ownershipRow is one row from the team_repo_ownership join, already
// resolved and ordered by ClickHouse per resolveOwnedRepoID's ORDER BY --
// only resolvedRepoID/teamID are read; is_primary/specificity/updated_at
// exist purely to drive that ORDER BY server-side, same as Python's query
// (which selects but never re-reads them either).
type ownershipRow struct {
	resolvedRepoID string
	teamID         string
}

// patternTeam is one team's repo_patterns row, for the pattern-fallback
// resolver -- team_name is not needed (only team_id is ever compared).
type patternTeam struct {
	id           string
	repoPatterns []string
}

// resolveOwnedRepoID ports _resolve_owned_repo_id verbatim (see that
// Python function's docstring for the full rationale -- native
// team_repo_ownership wins where it resolves the repo, ranked by
// (is_primary DESC, specificity DESC, updated_at DESC, team_id ASC) same
// as load_team_repo_ownership_map; teams.repo_patterns is consulted ONLY
// for a candidate repo ownership resolves no row for at all, via the same
// cross-team pattern resolver every other pattern-fallback reader uses).
// Returns ("", false, nil) when repoID does not exist, or resolves to a
// different team than teamID.
func resolveOwnedRepoID(ctx context.Context, client QueryClient, orgID, teamID, repoID string) (string, bool, error) {
	candidates, err := fetchRepoCandidates(ctx, client, orgID, repoID)
	if err != nil {
		return "", false, fmt.Errorf("candidates: %w", err)
	}
	if len(candidates) == 0 {
		return "", false, nil
	}
	candidateIDs := make([]string, len(candidates))
	for i, c := range candidates {
		candidateIDs[i] = c.id
	}

	ownershipRows, err := fetchOwnershipCandidates(ctx, client, orgID, candidateIDs)
	if err != nil {
		return "", false, fmt.Errorf("ownership: %w", err)
	}

	// canonical_owner: first-seen team per resolved_repo_id, in the exact
	// order ClickHouse's ORDER BY (resolved_repo_id, is_primary DESC,
	// specificity DESC, updated_at DESC, team_id ASC) returns rows --
	// mirrors Python's dict.setdefault-over-ordered-rows, including
	// iteration order (a Python dict preserves insertion order, so the
	// later "return the first matching entry" loop walks resolved_repo_id
	// ascending; `order` below reproduces that).
	canonicalOwner := make(map[string]string, len(ownershipRows))
	order := make([]string, 0, len(ownershipRows))
	for _, row := range ownershipRows {
		if _, seen := canonicalOwner[row.resolvedRepoID]; seen {
			continue
		}
		canonicalOwner[row.resolvedRepoID] = row.teamID
		order = append(order, row.resolvedRepoID)
	}
	for _, resolvedRepoID := range order {
		if canonicalOwner[resolvedRepoID] == teamID {
			return resolvedRepoID, true, nil
		}
	}

	// A candidate resolved by NATIVE ownership to a DIFFERENT team is
	// claimed and never re-checked against patterns; only a candidate with
	// NO native ownership row at all gets a pattern-fallback chance.
	var unresolved []repoCandidate
	for _, c := range candidates {
		if _, ok := canonicalOwner[c.id]; !ok {
			unresolved = append(unresolved, c)
		}
	}
	if len(unresolved) == 0 {
		return "", false, nil
	}

	teamRows, err := fetchAllTeamsForPatternFallback(ctx, client, orgID)
	if err != nil {
		return "", false, fmt.Errorf("teams: %w", err)
	}
	if len(teamRows) == 0 {
		return "", false, nil
	}
	resolver := newRepoPatternResolver(teamRows)
	for _, c := range unresolved {
		if resolvedTeamID, ok := resolver.resolve(c.repo); ok && resolvedTeamID == teamID {
			return c.id, true, nil
		}
	}
	return "", false, nil
}

// fetchRepoCandidates ports the candidate_rows query inside
// _resolve_owned_repo_id: resolves repoID (a UUID or a repos.repo slug) to
// every repos row it could refer to, reading FINAL (repos is
// ReplacingMergeTree -- a just-renamed repo can briefly have both its old
// and new row present until the background merge collapses them; an
// unqualified read could treat the stale row as a live candidate, codex
// R4 on the Python side).
func fetchRepoCandidates(ctx context.Context, client QueryClient, orgID, repoID string) ([]repoCandidate, error) {
	query := `
        SELECT toString(id) AS id, repo AS repo
        FROM repos FINAL
        WHERE org_id = {org_id:String}
          AND (repo = {repo_id:String} OR toString(id) = {repo_id:String})`
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: repoID},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []repoCandidate
	for rows.Next() {
		var c repoCandidate
		if scanErr := rows.Scan(&c.id, &c.repo); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		result = append(result, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
}

// fetchOwnershipCandidates ports the ownership_rows query inside
// _resolve_owned_repo_id: LEFT JOINs team_repo_ownership against repos on
// (org_id, provider, lower(repo)) so a native GitHub auto-import row
// (repo_id NULL, only repo_full_name set) still resolves, via
// coalesce(o.repo_id, r.id) -- matching load_team_repo_ownership_map's
// exact join, including its "matched" sentinel (ClickHouse's zero-value
// LEFT JOIN default, not NULL, would otherwise silently treat an
// unmatched name as matched to the UUID zero-value). Only
// resolved_repo_id/team_id are selected -- is_primary/specificity/
// updated_at drive the ORDER BY by direct column reference instead of a
// selected alias, so this port never has to scan values it never reads.
func fetchOwnershipCandidates(ctx context.Context, client QueryClient, orgID string, candidateIDs []string) ([]ownershipRow, error) {
	query := `
        SELECT
            coalesce(toString(o.repo_id), toString(r.id)) AS resolved_repo_id,
            o.team_id AS team_id
        FROM team_repo_ownership AS o FINAL
        LEFT JOIN (
            SELECT org_id, provider, id, repo, 1 AS matched
            FROM repos FINAL
            WHERE org_id = {org_id:String}
        ) AS r
            ON r.org_id = o.org_id
               AND r.provider = o.provider
               AND lower(r.repo) = lower(o.repo_full_name)
        WHERE o.org_id = {org_id:String}
          AND (o.repo_id IS NOT NULL OR r.matched = 1)
          AND coalesce(toString(o.repo_id), toString(r.id)) IN {candidate_ids:Array(String)}
          AND o.valid_from <= now64(3)
          AND (o.valid_to IS NULL OR o.valid_to > now64(3))
        ORDER BY resolved_repo_id, o.is_primary DESC, o.specificity DESC, o.updated_at DESC, team_id ASC`
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "candidate_ids", Value: candidateIDs},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []ownershipRow
	for rows.Next() {
		var row ownershipRow
		if scanErr := rows.Scan(&row.resolvedRepoID, &row.teamID); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
}

// fetchAllTeamsForPatternFallback ports the all_team_rows query inside
// _resolve_owned_repo_id: every team's id + repo_patterns in the org, so
// the pattern resolver is built from EVERY team's patterns (not just the
// requesting team's) -- a more specific pattern another team owns must
// never be silently overridden by this team's own broader one.
func fetchAllTeamsForPatternFallback(ctx context.Context, client QueryClient, orgID string) ([]patternTeam, error) {
	query := `SELECT id, repo_patterns FROM teams FINAL WHERE org_id = {org_id:String}`
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []patternTeam
	for rows.Next() {
		var t patternTeam
		if scanErr := rows.Scan(&t.id, &t.repoPatterns); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		result = append(result, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
}

// repoPatternResolver ports RepoPatternTeamResolver/build_repo_pattern_resolver
// (src/dev_health_ops/providers/teams.py:94,248) -- deliberately a private
// copy scoped to this package rather than a shared import: query-api is a
// read-only service and does not import internal/jobs/... (the worker's
// own copy lives in internal/jobs/metrics/daily/wellbeing_native_clickhouse.go
// as repoPatternResolver/NewRepoPatternResolver, verified against this
// same Python function already) -- same convention this package's
// QueryClient doc comment already documents for why small per-package
// helpers are not pulled through a shared library here.
type repoPatternResolver struct {
	exact    map[string]string // repo slug (lowercased) -> team_id
	prefixes []repoPatternPrefix
}

type repoPatternPrefix struct {
	prefix string
	teamID string
}

// newRepoPatternResolver builds the resolver build_repo_pattern_resolver
// builds: an exact-match map for patterns with no "*", and a prefix list
// (pattern with trailing "*" and "/" stripped) for the rest, sorted
// longest-prefix-first so the most specific pattern wins when several
// match.
func newRepoPatternResolver(teams []patternTeam) *repoPatternResolver {
	exact := make(map[string]string)
	var prefixes []repoPatternPrefix
	for _, team := range teams {
		teamID := strings.TrimSpace(team.id)
		if teamID == "" || len(team.repoPatterns) == 0 {
			continue
		}
		for _, raw := range team.repoPatterns {
			pattern := strings.ToLower(strings.TrimSpace(raw))
			if pattern == "" {
				continue
			}
			if strings.Contains(pattern, "*") {
				prefix := strings.TrimRight(strings.TrimRight(pattern, "*"), "/")
				if prefix != "" {
					prefixes = append(prefixes, repoPatternPrefix{prefix: prefix, teamID: teamID})
				}
				continue
			}
			exact[pattern] = teamID
		}
	}
	sort.SliceStable(prefixes, func(i, j int) bool {
		return len(prefixes[i].prefix) > len(prefixes[j].prefix)
	})
	return &repoPatternResolver{exact: exact, prefixes: prefixes}
}

// resolve ports RepoPatternTeamResolver.resolve: exact match first, then
// the longest matching prefix.
func (r *repoPatternResolver) resolve(repoName string) (string, bool) {
	if r == nil || repoName == "" {
		return "", false
	}
	key := strings.ToLower(strings.TrimSpace(repoName))
	if key == "" {
		return "", false
	}
	if teamID, ok := r.exact[key]; ok {
		return teamID, true
	}
	for _, p := range r.prefixes {
		if strings.HasPrefix(key, p.prefix) {
			return p.teamID, true
		}
	}
	return "", false
}

// fetchRepoScopedTeamMetrics ports _fetch_repo_scoped_team_metrics
// (CHAOS-4406): one repo's after-hours/weekend commit-timing ratio, by
// day, collapsed across every team_id label team_metrics_daily attached
// to that repo's rows -- never filtered BY team_id, since the caller has
// already confirmed ownership via resolveOwnedRepoID and every commit
// against this repo belongs to that team by definition regardless of
// which (possibly wrong) team_id an individual row carries. The inner
// join finds this repo's latest computed_at PER DAY, then only rows
// exactly at that timestamp are summed -- excludes a stale earlier
// recompute generation instead of double-counting it (same fix
// metrics/job_daily.py's finalize producer uses, ported per Python's
// docstring).
func fetchRepoScopedTeamMetrics(ctx context.Context, client QueryClient, orgID, sinceDate, untilDate, repoID string) ([]teamRatioRow, error) {
	query := `
        SELECT
            day,
            if(total_commits > 0,
               total_after_hours_commits / total_commits, 0.0
            ) AS after_hours_commit_ratio,
            if(total_commits > 0,
               total_weekend_commits / total_commits, 0.0
            ) AS weekend_commit_ratio
        FROM (
            SELECT
                t.day AS day,
                sum(t.commits_count)             AS total_commits,
                sum(t.after_hours_commits_count) AS total_after_hours_commits,
                sum(t.weekend_commits_count)      AS total_weekend_commits
            FROM team_metrics_daily AS t
            INNER JOIN (
                SELECT day, max(computed_at) AS latest_computed_at
                FROM team_metrics_daily
                WHERE org_id = {org_id:String}
                  AND day >= {since_date:Date}
                  AND day <= {until_date:Date}
                  AND repo_id IN (
                      SELECT toString(id) FROM repos
                      WHERE org_id = {org_id:String}
                        AND (repo = {repo_id:String} OR toString(id) = {repo_id:String})
                  )
                GROUP BY day
            ) AS latest_gen
                ON t.day = latest_gen.day
                   AND t.computed_at = latest_gen.latest_computed_at
            WHERE t.org_id = {org_id:String}
              AND t.day >= {since_date:Date}
              AND t.day <= {until_date:Date}
              AND t.repo_id IN (
                  SELECT toString(id) FROM repos
                  WHERE org_id = {org_id:String}
                    AND (repo = {repo_id:String} OR toString(id) = {repo_id:String})
              )
            GROUP BY t.day
        )
        ORDER BY day`
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_date", Value: sinceDate},
		{Name: "until_date", Value: untilDate},
		{Name: "repo_id", Value: repoID},
	}

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
