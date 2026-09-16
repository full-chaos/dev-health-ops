// Package filteroptions is the Go port of GET /api/v1/filters/options
// (src/dev_health_ops/api/queries/filters.py's fetch_filter_options,
// invoked by api/main.py's filter_options handler).
//
// The endpoint has no query parameters and no per-request branching: it
// runs five independent ClickHouse reads, filters one of them through an
// email-shape check, appends the static investment taxonomy as the sixth
// field, and always answers an eighth-shape-but-unused "services" field
// with an empty list -- api/queries/filters.py:24 initialises
// options["services"] = [] and nothing in fetch_filter_options ever writes
// to that key, so every response carries services: [] regardless of what
// ClickHouse holds. That is not a missed dedup case; there is no query to
// dedup. Preserved as-is, matching the HTTP contract exactly.
//
// Data-layer note, ReplacingMergeTree reads (migrations 002/011 teams;
// 000 repos; 055 work_item_user_metrics_daily; 096 user_metrics_daily and
// work_item_state_durations_daily -- all confirmed against
// src/dev_health_ops/migrations/clickhouse/*): the reference's own
// dedup_from()/teams-FINAL convention (clickhouse_dedup.py) already
// applies FINAL to two of the three "teams" UNION branches (the teams
// catalog itself and work_item_user_metrics_daily, both registered in
// RERUN_DEDUPED_DAILY_TABLES) but omits it for three reads that need the
// same treatment and are declared Python-plane defects here:
//
//   - user_metrics_daily (the team_id branch of the teams UNION, and the
//     developers/author_email read): ReplacingMergeTree(computed_at)
//     since migration 096, sorting key (org_id, repo_id, author_email,
//     day) since migration 027. api/queries/filters.py never dedups this
//     read at all. team_id is NOT part of the sorting key, so a
//     re-attribution re-run leaves a stale team_id readable until the
//     next merge; author_email IS part of the key, so that column
//     happens to be immune, but both reads use FINAL here for the same
//     reason the other four reads in this package do: one dedup shape
//     for every table in the endpoint, not a per-column exception.
//   - repos: ReplacingMergeTree(last_synced) since migration 000, ORDER BY
//     (org_id, id) since migration 027 -- the same raw-dimension shape as
//     "teams", which the reference already reads with FINAL two lines
//     away in the same function. The repos read has no such FINAL, so a
//     repo rename leaves the pre-rename slug in the result until the next
//     merge collapses it. Fixed here with FINAL, matching teams's own
//     pattern.
//   - work_item_state_durations_daily: converted to
//     ReplacingMergeTree(computed_at) by migration 096 (TARGET_SORT_KEYS
//     includes it, ORDER BY (org_id, provider, work_scope_id, team_id,
//     status, day)); api/queries/filters.py predates that migration and
//     still reads it raw. status IS part of the sorting key, so distinct
//     status extraction is unaffected by un-merged duplicate versions, but
//     the read is wrapped in FINAL here for the same blanket-consistency
//     reason as above.
//
// issue_type_metrics_daily stays plain MergeTree (not touched by
// migrations 055 or 096) -- no dedup wrapper applies, matching the
// reference's raw read exactly.
//
// # EVERY DEDUP READ IS BOUNDED TO THE REQUESTING ORG INSIDE THE SAME STATEMENT
//
// All five tables above that need FINAL have org_id as the FIRST
// component of their ClickHouse sorting key (teams and repos since
// migration 027's "(org_id, id)"; work_item_user_metrics_daily,
// user_metrics_daily and work_item_state_durations_daily since the same
// migration's per-table keys). Every query in this package puts
// "org_id = {org_id:String}" in the SAME WHERE clause as the table's
// FINAL source -- never behind a separate, unfiltered subquery -- so
// ClickHouse can prune to the requesting org's rows via the primary-key
// prefix before FINAL does any merge-time dedup work. This endpoint is a
// page-load-frequency read; a dedup step that sorted or collapsed the
// whole table across every tenant before an outer filter narrowed it
// would be an unbounded all-tenant scan on every request, not a query
// this endpoint can afford. There is no wrapping "dedup subquery" helper
// in this file for that reason: a separate subquery is exactly the shape
// that would let the org filter end up outside the dedup step by
// accident, so the org predicate is written inline, once, in every query
// constant below, instead of behind a shared indirection a future edit
// could apply after the dedup instead of before it.
package filteroptions

import (
	"context"
	"fmt"
	"regexp"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every sibling operation package
// (quadrant.QueryClient, hotspots.QueryClient, etc.) declares
// independently.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// Response ports FilterOptionsResponse (api/models/filters.py:96-103).
// Plain field names: main.py's FastAPI app carries no alias generator, so
// Pydantic's wire form is the snake_case attribute name verbatim.
type Response struct {
	Teams        []string `json:"teams"`
	Repos        []string `json:"repos"`
	Services     []string `json:"services"`
	Developers   []string `json:"developers"`
	WorkCategory []string `json:"work_category"`
	IssueType    []string `json:"issue_type"`
	FlowStage    []string `json:"flow_stage"`
}

// emailValueRe ports filters.py's _EMAIL_VALUE_RE verbatim -- Go's RE2
// syntax accepts the same character-class pattern unchanged.
var emailValueRe = regexp.MustCompile(`^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$`)

func isEmailValue(value string) bool {
	return emailValueRe.MatchString(value)
}

// teamsQuery ports the team_coro SQL (api/queries/filters.py:31-58)
// verbatim in shape, with the two declared FINAL fixes above (the
// user_metrics_daily branch; the work_item_user_metrics_daily branch
// already carried FINAL in the reference and keeps it). Every branch's
// org_id predicate sits in the same WHERE clause as its table's FINAL
// source -- see the package doc comment's bounded-scan note.
const teamsQuery = `
        SELECT DISTINCT value
        FROM (
            SELECT id AS value
            FROM teams FINAL
            WHERE id != '' AND is_active = 1
              AND org_id = {org_id:String}

            UNION ALL

            SELECT team_id AS value
            FROM user_metrics_daily FINAL
            WHERE team_id != ''
              AND org_id = {org_id:String}

            UNION ALL

            SELECT team_id AS value
            FROM work_item_user_metrics_daily FINAL
            WHERE team_id != ''
              AND org_id = {org_id:String}
        )
        WHERE value != ''
        ORDER BY value
    `

// reposQuery ports repo_coro (api/queries/filters.py:60-63), with the
// declared FINAL fix above.
const reposQuery = `
        SELECT DISTINCT repo AS value
        FROM repos FINAL
        WHERE repo != '' AND org_id = {org_id:String}
        ORDER BY repo
    `

// developersQuery ports dev_coro (api/queries/filters.py:65-73), with the
// declared FINAL fix above. The email-shape filter is applied by the
// caller after the read, matching the reference's own post-read list
// comprehension.
const developersQuery = `
        SELECT DISTINCT author_email AS value
        FROM user_metrics_daily FINAL
        WHERE author_email != '' AND org_id = {org_id:String}
        ORDER BY author_email
    `

// issueTypeQuery ports issue_coro (api/queries/filters.py:75-81) verbatim
// -- issue_type_metrics_daily is plain MergeTree, no dedup wrapper needed.
const issueTypeQuery = `
        SELECT DISTINCT issue_type_norm AS value
        FROM issue_type_metrics_daily
        WHERE issue_type_norm != '' AND org_id = {org_id:String}
        ORDER BY issue_type_norm
    `

// flowStageQuery ports stage_coro (api/queries/filters.py:83-89), with the
// declared FINAL fix above.
const flowStageQuery = `
        SELECT DISTINCT status AS value
        FROM work_item_state_durations_daily FINAL
        WHERE status != '' AND org_id = {org_id:String}
        ORDER BY status
    `

// distinctValues runs a single-column "value" query and returns the
// non-empty results in row order (the SQL's own ORDER BY, matching the
// reference's reliance on ClickHouse for ordering rather than an
// application-layer sort). The truthy-value guard mirrors every one of
// filters.py's "if row.get('value')" list-comprehension filters, which are
// redundant with each query's own empty-string WHERE guard but kept for
// exact parity with the reference's defensive shape.
func distinctValues(ctx context.Context, client QueryClient, query, errPrefix string, bindings []dhclickhouse.Binding) ([]string, error) {
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("%s query: %w", errPrefix, err)
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("%s scan: %w", errPrefix, err)
		}
		if value != "" {
			out = append(out, value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", errPrefix, err)
	}
	return out, nil
}

// workCategory ports "sorted(THEMES) + sorted(SUBCATEGORIES)"
// (api/queries/filters.py:71). units.SortedThemes/SortedSubcategories are
// the already-sorted Go port of the same THEMES/SUBCATEGORIES sets
// (internal/jobs/workgraph/units/taxonomy.go), reused rather than
// re-derived.
func workCategory() []string {
	out := make([]string, 0, len(units.SortedThemes)+len(units.SortedSubcategories))
	out = append(out, units.SortedThemes[:]...)
	out = append(out, units.SortedSubcategories[:]...)
	return out
}

// BuildResponse ports fetch_filter_options (api/queries/filters.py:18-124)
// verbatim in shape: five ClickHouse reads (run sequentially here; the
// reference runs them concurrently via asyncio.gather, a pure performance
// difference that does not change the result), the developers list
// filtered to email-shaped values, work_category from the static
// taxonomy, and services always empty (see the package doc comment).
func BuildResponse(ctx context.Context, client QueryClient, orgID string) (Response, error) {
	bindings := []dhclickhouse.Binding{{Name: "org_id", Value: orgID}}

	teams, err := distinctValues(ctx, client, teamsQuery, "filteroptions: teams", bindings)
	if err != nil {
		return Response{}, err
	}
	repos, err := distinctValues(ctx, client, reposQuery, "filteroptions: repos", bindings)
	if err != nil {
		return Response{}, err
	}
	rawDevelopers, err := distinctValues(ctx, client, developersQuery, "filteroptions: developers", bindings)
	if err != nil {
		return Response{}, err
	}
	issueTypes, err := distinctValues(ctx, client, issueTypeQuery, "filteroptions: issue_type", bindings)
	if err != nil {
		return Response{}, err
	}
	flowStages, err := distinctValues(ctx, client, flowStageQuery, "filteroptions: flow_stage", bindings)
	if err != nil {
		return Response{}, err
	}

	developers := []string{}
	for _, value := range rawDevelopers {
		if isEmailValue(value) {
			developers = append(developers, value)
		}
	}

	return Response{
		Teams:        teams,
		Repos:        repos,
		Services:     []string{},
		Developers:   developers,
		WorkCategory: workCategory(),
		IssueType:    issueTypes,
		FlowStage:    flowStages,
	}, nil
}
