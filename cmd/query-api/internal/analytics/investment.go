package analytics

// Go port of the investment-path pieces of api/queries/investment.py
// (e9ea257ff) that api/graphql/sql/compiler.py's `_get_context_params`
// (compiler.py:140-241) actually composes -- NOT a port of the whole
// 1079-line file. investment.py's own `fetch_investment_*` functions
// (:524-1079) back a DIFFERENT surface (api/services/sankey.py /
// investment_flow.py's REST-shaped Sankey endpoints, not the GraphQL
// `analytics` root this package serves) and are out of this PR's scope;
// confirmed by reading compiler.py's imports (investment.py:12-17),
// which pull in exactly LATEST_WORK_UNIT_INVESTMENTS_CTE,
// LATEST_WORK_UNIT_REPO_EFFORT_CTE, LATEST_WORK_UNIT_AUTHORS_CTE and
// build_unit_team_subquery -- nothing else from that file.
//
// RESTRUCTURING (§9 of the brief, same discipline as
// investmentmembershipscope.go): every Python fragment here is a WITH
// CTE; the ClickHouse client requires a leading SELECT, so each becomes
// a bare Go function returning `(SELECT ...)`, composed by string
// substitution at the point of use rather than by name.

import (
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// --- investment.py:342-396: evidence-ref resolution machinery ---------

// workUnitEvidenceRepoSource ports WORK_UNIT_EVIDENCE_REPO_SOURCE
// (investment.py:342-352). No CHAOS-4547 tuple-wrap needed: repos.repo
// is non-nullable String and repos.provider is non-nullable String
// DEFAULT 'unknown' (migrations/clickhouse/000_raw_tables.sql:2,
// 028_repos_provider.sql:4) -- matching investment.py's own comment that
// provider is "never legitimately empty here".
func workUnitEvidenceRepoSource() string {
	return `(
    SELECT
        org_id,
        toString(id) AS repo_uuid,
        argMax(repo, last_synced) AS repo,
        if(uniqExact(provider) = 1, argMax(provider, last_synced), '') AS provider
    FROM repos
    WHERE org_id = {org_id:String}
    GROUP BY org_id, id
)`
}

// workUnitEvidenceWorkItemRefsExpr ports WORK_UNIT_EVIDENCE_WORK_ITEM_REFS
// (investment.py:360-363) verbatim -- a fixed expression, not
// parameterized on anything, so a plain const rather than a func.
const workUnitEvidenceWorkItemRefsExpr = `arrayDistinct(arrayConcat(
                    JSONExtract(structural_evidence_json, 'issues', 'Array(String)'),
                    JSONExtract(structural_evidence_json, 'prs', 'Array(String)')
                ))`

// resolvedEvidenceWorkItemIDExpr ports RESOLVED_EVIDENCE_WORK_ITEM_ID
// (investment.py:385-396) verbatim.
const resolvedEvidenceWorkItemIDExpr = `multiIf(
                        NOT match(evidence_ref, '^[0-9a-fA-F-]{36}#pr[0-9]+$'),
                        evidence_ref,
                        evidence_repo.repo = '' OR evidence_repo.provider = '',
                        '',
                        concat(
                            if(evidence_repo.provider = 'gitlab', 'gitlab:', 'ghpr:'),
                            evidence_repo.repo,
                            if(evidence_repo.provider = 'gitlab', '!', '#'),
                            splitByString('#pr', evidence_ref)[2]
                        )
                    )`

// --- investment.py:399-510: build_unit_team_subquery -------------------

// unitTeamSubqueryOptions is the Go equivalent of build_unit_team_subquery's
// keyword arguments (investment.py:399-408).
type unitTeamSubqueryOptions struct {
	Source         string
	Where          string
	InnerTeamAlias string
	OuterTeamAlias string
	IncludeTeamID  bool
}

// buildUnitTeamSubquery ports build_unit_team_subquery (investment.py:
// 399-510, e9ea257ff) -- the ONE per-work-unit team-resolution subquery
// every investment team join in this package renders, inlined (no WITH,
// no CTE name; embedded directly where Python would have referenced
// "unit_team").
//
// CHAOS-4547-CLASS FIX, site 3 of 3 this lane found (Go plane only,
// chris's GO-ONLY routing 06:52 PT 08-29; ARGUED not yet executed
// against a live engine -- see the PR's RISK-NOTES): the outer
// `argMax(resolved_team, tie_break)` projects `resolved_team`, i.e.
// `max(ifNull(nullIf(t.team_name,'), nullIf(t.team_id,')))` below --
// genuinely NULL whenever a work item's work_item_team_attributions row
// carries no team_id/team_name (migrations/clickhouse/
// 051_team_attribution_dimensions.sql:83-84, both Nullable(String)).
// NULL there is the CORRECT "no team attribution" answer, not a missing
// value, which makes this the worst-case instance of the class: if the
// WINNING (highest cnt, then highest resolved_team_id) tie-break row
// carries a NULL vote while a LOSING row's vote is non-null, plain
// argMax's NULL-skip promotes that losing candidate's label instead of
// correctly reporting "no team" -- i.e. it INVENTS a team attribution
// for a work item that structurally has none. Team attribution is
// ownership-derived and must never be invented (root AGENTS.md,
// CHAOS-2600 contract) -- this is the one of the three CHAOS-4547-class
// sites in this port where the wrong answer is a fabricated fact, not
// merely a stale one. Fixed via (argMax(tuple(resolved_team),
// tie_break)).1. `resolved_team_id` (the vote_id column) is NOT
// wrapped: its own ifNull falls back to the LITERAL ' (never NULL), so
// it carries no null-skip risk and wrapping it would be a no-op change
// misrepresenting the audit.
func buildUnitTeamSubquery(opts unitTeamSubqueryOptions) string {
	outerAlias := opts.OuterTeamAlias
	if outerAlias == "" {
		outerAlias = opts.InnerTeamAlias
	}
	if outerAlias == "" {
		outerAlias = "team"
	}
	const teamExpr = "ifNull(nullIf(t.team_name, ''), nullIf(t.team_id, ''))"
	const vote = "resolved_team"
	const voteID = "resolved_team_id"
	tieBreak := fmt.Sprintf("(cnt, %s)", voteID)

	outerIDLine := ""
	if opts.IncludeTeamID {
		outerIDLine = fmt.Sprintf("                argMax(%s, %s) AS team_id,\n", voteID, tieBreak)
	}

	return fmt.Sprintf(`
            SELECT
                work_unit_id,
%s                (argMax(tuple(%s), %s)).1 AS %s
            FROM (
                SELECT
                    work_unit_investments.work_unit_id AS work_unit_id,
                    ifNull(nullIf(t.team_id, ''), '') AS %s,
                    max(%s) AS %s,
                    uniqExactIf(%s, %s IS NOT NULL) AS cnt
                FROM %s
                ARRAY JOIN %s AS evidence_ref
                LEFT JOIN %s AS evidence_repo
                    ON evidence_repo.org_id = work_unit_investments.org_id
                    AND evidence_repo.repo_uuid = splitByString('#pr', evidence_ref)[1]
                LEFT JOIN %s AS t
                    ON t.work_item_id = %s
%s
                GROUP BY work_unit_id, %s
            )
            GROUP BY work_unit_id
`,
		outerIDLine, vote, tieBreak, outerAlias,
		voteID, teamExpr, vote,
		resolvedEvidenceWorkItemIDExpr, teamExpr,
		opts.Source, workUnitEvidenceWorkItemRefsExpr, workUnitEvidenceRepoSource(),
		primaryWorkItemTeamAttributionSource, resolvedEvidenceWorkItemIDExpr,
		opts.Where, voteID)
}

// --- investment.py:23-58: LATEST_WORK_UNIT_INVESTMENTS_CTE -------------

// latestWorkUnitInvestmentsSource ports LATEST_WORK_UNIT_INVESTMENTS_CTE
// (investment.py:23-58, e9ea257ff), restructured from `WITH ... AS (...)`
// into an inlined derived-table subquery (no trailing alias -- callers
// append their own, exactly like this package's existing
// primaryWorkItemTeamAttributionSource convention in flowmatrix.go).
// The membership-scope gate this CTE chained by name
// (INVESTMENT_MEMBERSHIP_SCOPE_CTES/_FILTER) is inlined via
// investmentMembershipScopeFilter() (investmentmembershipscope.go).
//
// CHAOS-4547 FIX (this port only; chris's GO-ONLY routing, 06:52 PT
// 08-29): four of this CTE's sixteen argMax'd columns are Nullable per
// DDL and Python does not tuple-wrap them:
//   - work_unit_type, work_unit_name: Nullable(String)
//     (migrations/clickhouse/019_work_unit_investment_labels.sql:3,6)
//   - repo_id: Nullable(UUID), provider: Nullable(String)
//     (migrations/clickhouse/017_investment_materialize_tables.sql --
//     CHAOS-4547's already-known pair)
//
// argMax(col, ver) SKIPS rows where col is NULL when picking the row
// with the greatest ver, so a work unit's newest generation losing its
// type/name/repo/provider (a legitimate categorization-run transition)
// would silently keep reporting a STALE non-null value from an OLDER
// generation instead of the true latest value. Fixed via
// (argMax(tuple(col), computed_at)).1, which cannot null-skip since the
// tuple itself is never NULL. The other twelve projected columns are
// non-nullable String/Float64/Map per DDL (Map cannot be Nullable in
// ClickHouse at all) and are left as plain argMax, matching Python.
func latestWorkUnitInvestmentsSource() string {
	return fmt.Sprintf(`(
        SELECT
            work_unit_id,
            (argMax(tuple(work_unit_type), computed_at)).1 AS work_unit_type,
            (argMax(tuple(work_unit_name), computed_at)).1 AS work_unit_name,
            argMax(from_ts, computed_at) AS from_ts,
            argMax(to_ts, computed_at) AS to_ts,
            (argMax(tuple(repo_id), computed_at)).1 AS repo_id,
            (argMax(tuple(provider), computed_at)).1 AS provider,
            argMax(effort_metric, computed_at) AS effort_metric,
            argMax(effort_value, computed_at) AS effort_value,
            argMax(theme_distribution_json, computed_at) AS theme_distribution_json,
            argMax(subcategory_distribution_json, computed_at) AS subcategory_distribution_json,
            argMax(structural_evidence_json, computed_at) AS structural_evidence_json,
            argMax(evidence_quality, computed_at) AS evidence_quality,
            argMax(evidence_quality_band, computed_at) AS evidence_quality_band,
            argMax(categorization_status, computed_at) AS categorization_status,
            argMax(categorization_model_version, computed_at) AS categorization_model_version,
            argMax(categorization_run_id, computed_at) AS categorization_run_id,
            org_id,
            max(computed_at) AS latest_computed_at
        FROM work_unit_investments
        WHERE org_id = {org_id:String}%s
        GROUP BY org_id, work_unit_id
    )`, investmentMembershipScopeFilter())
}

// --- investment.py:90-127: LATEST_WORK_UNIT_REPO_EFFORT_CTE ------------

// latestWorkUnitRepoEffortSource ports LATEST_WORK_UNIT_REPO_EFFORT_CTE
// (investment.py:90-127, e9ea257ff), inlined. No CHAOS-4547 fix needed:
// all three argMax'd columns (effort_metric, effort_value ->
// repo_effort_value, allocation_source) are non-nullable
// String/Float64/String per work_unit_repo_effort's DDL
// (migrations/clickhouse/064_work_unit_repo_effort.sql) -- repo_id is a
// GROUP BY key in the inner subquery, never argMax'd, so its own
// Nullable(UUID) declaration there carries no null-skip risk.
func latestWorkUnitRepoEffortSource() string {
	return `(
        SELECT
            d.work_unit_id AS work_unit_id,
            d.repo_id AS repo_id,
            d.effort_metric AS effort_metric,
            d.repo_effort_value AS repo_effort_value,
            d.allocation_source AS allocation_source,
            d.org_id AS org_id,
            d.latest_repo_effort_computed_at AS latest_repo_effort_computed_at,
            1 AS has_allocation
        FROM (
            SELECT
                work_unit_id,
                repo_id,
                org_id,
                argMax(effort_metric, computed_at) AS effort_metric,
                argMax(effort_value, computed_at) AS repo_effort_value,
                argMax(allocation_source, computed_at) AS allocation_source,
                max(computed_at) AS latest_repo_effort_computed_at
            FROM work_unit_repo_effort
            WHERE org_id = {org_id:String}
            GROUP BY org_id, work_unit_id, repo_id
        ) AS d
        INNER JOIN (
            SELECT
                org_id,
                work_unit_id,
                max(computed_at) AS unit_generation_at
            FROM work_unit_repo_effort
            WHERE org_id = {org_id:String}
            GROUP BY org_id, work_unit_id
        ) AS g
            ON g.org_id = d.org_id
            AND g.work_unit_id = d.work_unit_id
        WHERE d.latest_repo_effort_computed_at = g.unit_generation_at
    )`
}

// --- compiler.py:162-190: the compiler's OWN repo-allocation source ----

// repoAllocationInvestmentSource ports compiler.py's `_get_context_params`
// repo-allocation `source_table` (compiler.py:162-190, e9ea257ff)
// EXACTLY -- this is NOT investment.py's
// REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE (investment.py:155-172),
// which is a similar-looking but DIFFERENT definition used only by
// investment.py's own fetch_investment_* functions (out of this PR's
// scope, see this file's package doc comment). Two differences that
// matter if the two are ever conflated: this version projects the FULL
// investment-row column set (work_unit_type/name, provider, theme_json,
// evidence_quality*, categorization_*) that dimension/measure
// expressions further down the query need, and its match flag is
// `wure.work_unit_id != '` (an unmatched LEFT JOIN's String column
// reads as ' under ClickHouse's default join_use_nulls=0, not NULL) --
// NOT the explicit `has_allocation` flag investment.py's version adds.
// Both are real Python behavior at their respective call sites; this
// port follows compiler.py's shape because THAT is what
// `_get_context_params` (my actual scope) uses. No new argMax
// nullability risk here: every column is a plain reference into
// already-deduped latestWorkUnitInvestmentsSource /
// latestWorkUnitRepoEffortSource, not a fresh aggregate.
func repoAllocationInvestmentSource() string {
	return fmt.Sprintf(`(
                SELECT
                    wui.work_unit_id AS work_unit_id,
                    wui.work_unit_type AS work_unit_type,
                    wui.work_unit_name AS work_unit_name,
                    wui.from_ts AS from_ts,
                    wui.to_ts AS to_ts,
                    if(wure.work_unit_id != '', wure.repo_id, wui.repo_id) AS repo_id,
                    wui.provider AS provider,
                    if(wure.work_unit_id != '', wure.effort_metric, wui.effort_metric) AS effort_metric,
                    if(wure.work_unit_id != '', wure.repo_effort_value, wui.effort_value) AS effort_value,
                    if(wure.work_unit_id != '', wure.allocation_source, 'scalar_fallback') AS allocation_source,
                    if(wure.work_unit_id != '', if(wui.effort_value > 0, wure.repo_effort_value / wui.effort_value, 0.0), 1.0) AS allocation_weight,
                    wui.theme_distribution_json AS theme_distribution_json,
                    wui.subcategory_distribution_json AS subcategory_distribution_json,
                    wui.structural_evidence_json AS structural_evidence_json,
                    wui.evidence_quality AS evidence_quality,
                    wui.evidence_quality_band AS evidence_quality_band,
                    wui.categorization_status AS categorization_status,
                    wui.categorization_model_version AS categorization_model_version,
                    wui.categorization_run_id AS categorization_run_id,
                    wui.org_id AS org_id
                FROM %s AS wui
                LEFT JOIN %s AS wure
                    ON wure.org_id = wui.org_id
                    AND wure.work_unit_id = wui.work_unit_id
            ) AS work_unit_investments`, latestWorkUnitInvestmentsSource(), latestWorkUnitRepoEffortSource())
}

// --- investment.py:214-256: LATEST_WORK_UNIT_AUTHORS_CTE ----------------

// workUnitAuthorsSource ports LATEST_WORK_UNIT_AUTHORS_CTE (investment.py:
// 214-256, e9ea257ff), inlined -- its two `FROM latest_work_unit_investments
// AS wui` references (investment.py:223, :241, one per UNION ALL branch)
// each become a FRESH call to latestWorkUnitInvestmentsSource(), so this
// one query embeds that CTE's full text three times in total once
// composed into a caller (main FROM + this function's two branches).
// Real Python cost parity, not a regression this port introduces: a
// non-recursive ClickHouse WITH clause is closer to macro substitution
// than a materialized temp table for this query shape, so Python's own
// WITH-referenced-thrice CTE already re-executes per reference.
//
// CHAOS-4547 FIX, site 2 of 3 this lane found (Go plane only): both
// UNION ALL branches' `argMax(author_email, last_synced)` project a
// Nullable(String) column -- git_commits.author_email
// (migrations/clickhouse/000_raw_tables.sql:26) and
// git_pull_requests.author_email (000_raw_tables.sql:68), confirmed
// against the EXACT tables this CTE reads (git_blame also declares a
// Nullable author_email at :52 but this CTE never reads git_blame).
// Without the fix, a commit/PR whose newest sync loses its author_email
// (identity resolution regressing, or an upstream field going private)
// would keep the STALE prior author attributed via argMax's null-skip,
// rather than correctly excluding it from `work_unit_authors` the way
// the immediately-following `WHERE ca.author_email IS NOT NULL` clause
// intends. Fixed via (argMax(tuple(author_email), last_synced)).1 in
// both branches.
func workUnitAuthorsSource() string {
	commitsBranch := fmt.Sprintf(`
                SELECT
                    wui.work_unit_id AS work_unit_id,
                    ca.author_email AS author_email
                FROM %s AS wui
                ARRAY JOIN JSONExtract(wui.structural_evidence_json, 'commits', 'Array(String)') AS commit_ref
                INNER JOIN (
                    SELECT
                        org_id,
                        concat(toString(repo_id), '@', hash) AS commit_ref,
                        (argMax(tuple(author_email), last_synced)).1 AS author_email
                    FROM git_commits
                    WHERE org_id = {org_id:String}
                    GROUP BY org_id, repo_id, hash
                ) AS ca ON ca.commit_ref = commit_ref AND ca.org_id = wui.org_id
                WHERE ca.author_email IS NOT NULL AND ca.author_email != ''`, latestWorkUnitInvestmentsSource())

	prsBranch := fmt.Sprintf(`
                SELECT
                    wui.work_unit_id AS work_unit_id,
                    pa.author_email AS author_email
                FROM %s AS wui
                ARRAY JOIN JSONExtract(wui.structural_evidence_json, 'prs', 'Array(String)') AS pr_ref
                INNER JOIN (
                    SELECT
                        org_id,
                        concat(toString(repo_id), '#pr', toString(number)) AS pr_ref,
                        (argMax(tuple(author_email), last_synced)).1 AS author_email
                    FROM git_pull_requests
                    WHERE org_id = {org_id:String}
                    GROUP BY org_id, repo_id, number
                ) AS pa ON pa.pr_ref = pr_ref AND pa.org_id = wui.org_id
                WHERE pa.author_email IS NOT NULL AND pa.author_email != ''`, latestWorkUnitInvestmentsSource())

	return fmt.Sprintf(`(
        SELECT
            work_unit_id,
            groupUniqArray(author_email) AS author_emails
        FROM (%s
                UNION ALL%s
        )
        GROUP BY work_unit_id
    )`, commitsBranch, prsBranch)
}

// --- compiler.py:140-265: _get_context_params + its two needs_* gates --

// investmentContext is the Go equivalent of _get_context_params's return
// dict (compiler.py:140-241), restructured for the inline-subquery
// world: no with_clause field -- every fragment that dict's with_clause
// would have chained onto a WITH keyword is embedded directly inside
// Source/ExtraClauses at its point of use instead.
type investmentContext struct {
	Source            string
	Alias             string
	DateFilter        string
	ExtraClauses      string
	UseInvestment     bool
	UseRepoAllocation bool
}

// resolveUseInvestment ports _get_context_params's own use_investment
// resolution (compiler.py:151-155): investment_dims = {THEME,
// SUBCATEGORY, WORK_TYPE}; forceInvestment wins whenever non-nil
// (INCLUDING an explicit false -- validate.go's dbColumn doc comment
// documents the resulting WORK_TYPE gap this preserves faithfully),
// auto-routing to true only when forceInvestment is nil AND the
// dimension list contains one of the three investment-only dimensions.
func resolveUseInvestment(dimensions []Dimension, forceInvestment *bool) bool {
	if forceInvestment != nil {
		return *forceInvestment
	}
	for _, d := range dimensions {
		switch d {
		case DimensionTheme, DimensionSubcategory, DimensionWorkType:
			return true
		}
	}
	return false
}

// needsTeamJoin ports _needs_team_join (compiler.py:244-247) exactly.
func needsTeamJoin(filters *model.FilterInput) bool {
	if filters == nil || filters.Scope == nil || len(filters.Scope.Ids) == 0 {
		return false
	}
	return filters.Scope.Level == model.ScopeLevelInputTeam
}

// needsAuthorJoin ports _needs_author_join (compiler.py:250-265) exactly.
func needsAuthorJoin(filters *model.FilterInput) bool {
	if filters == nil {
		return false
	}
	if filters.Who != nil && len(filters.Who.Developers) > 0 {
		return true
	}
	return filters.Scope != nil && filters.Scope.Level == model.ScopeLevelInputDeveloper && len(filters.Scope.Ids) > 0
}

func dimensionListHas(dimensions []Dimension, want Dimension) bool {
	for _, d := range dimensions {
		if d == want {
			return true
		}
	}
	return false
}

// investmentContextFor ports _get_context_params's INVESTMENT branch
// (compiler.py:157-232, e9ea257ff) -- the piece that PRODUCES the
// `ut.team_label`/`ut.team_id`/`au.author_emails`/`r.repo` aliases
// validate.go's dbColumn and filtertranslation.go's translateFilters
// already reference. Call ONLY when resolveUseInvestment has already
// returned true for this dimension list; the non-investment default
// (_INVESTMENT_METRICS_DAILY_DEDUP) stays in
// nonInvestmentSourceAndDateFilter (timeseries.go), unchanged by this
// port -- Python's `_get_context_params` is one function with two
// branches selected by one boolean; this port is the same shape as two
// Go functions selected by the same boolean, which is what
// resolveUseInvestment computes.
func investmentContextFor(dimensions []Dimension, needsTeamJoinFlag, needsAuthorJoinFlag bool) investmentContext {
	useRepoAllocation := dimensionListHas(dimensions, DimensionRepo)

	var source string
	if useRepoAllocation {
		source = repoAllocationInvestmentSource()
	} else {
		source = fmt.Sprintf("%s AS work_unit_investments", latestWorkUnitInvestmentsSource())
	}

	// ALWAYS joined for every investment query (compiler.py:191-194) --
	// every investment dimension/measure expression reads
	// subcategory_kv.
	joins := []string{
		"ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv",
	}

	// Team join: compiler.py:196-206. The team-vote subquery ALWAYS
	// reads the plain (non-repo-allocated) latest_work_unit_investments
	// source, regardless of use_repo_allocation -- a deliberate Python
	// asymmetry (compiler.py:199: `source="latest_work_unit_investments
	// AS work_unit_investments"`, never the repo-allocation source, even
	// when the OUTER query's own FROM is repo-allocated) preserved
	// faithfully here, so this embeds its OWN fresh copy of
	// latestWorkUnitInvestmentsSource() independent of `source` above.
	if dimensionListHas(dimensions, DimensionTeam) || needsTeamJoinFlag {
		unitTeamSQL := buildUnitTeamSubquery(unitTeamSubqueryOptions{
			Source:         fmt.Sprintf("%s AS work_unit_investments", latestWorkUnitInvestmentsSource()),
			InnerTeamAlias: "team_label",
			IncludeTeamID:  true,
		})
		joins = append(joins, fmt.Sprintf("LEFT JOIN (%s) AS ut ON ut.work_unit_id = work_unit_investments.work_unit_id", unitTeamSQL))
	}

	// Repo join: compiler.py:209-210.
	if dimensionListHas(dimensions, DimensionRepo) {
		joins = append(joins, "LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)")
	}

	// Author join: compiler.py:212-223 (CHAOS-2492).
	if dimensionListHas(dimensions, DimensionAuthor) || needsAuthorJoinFlag {
		joins = append(joins, fmt.Sprintf("LEFT JOIN %s AS au ON au.work_unit_id = work_unit_investments.work_unit_id", workUnitAuthorsSource()))
	}

	return investmentContext{
		Source: source,
		Alias:  "work_unit_investments",
		// compiler.py:227 -- from_ts/to_ts are DateTime64(3,'UTC')
		// (already-deduped via latestWorkUnitInvestmentsSource's own
		// argMax), compared against the same {start_date:Date}/
		// {end_date:Date} bindings the non-investment path uses;
		// ClickHouse implicitly promotes a Date value to midnight UTC
		// DateTime for this comparison, matching Python's own date (not
		// datetime) request.start_date/end_date parameters.
		DateFilter:        "work_unit_investments.from_ts < {end_date:Date} AND work_unit_investments.to_ts >= {start_date:Date}",
		ExtraClauses:      strings.Join(joins, "\n"),
		UseInvestment:     true,
		UseRepoAllocation: useRepoAllocation,
	}
}
