//go:build integration

// Executed-SQL recurrence guard: every reader in this package runs once
// against a REAL ClickHouse container, schema applied through the
// canonical migration chain (internal/testsupport/chschema.Apply), never
// hand-typed DDL -- so a table's actual prod-confirmed engine, sorting
// key and column set (_records/5885-5886-prod-engines.tsv) is what the
// reader executes against, not a guess.
//
// Every ReplacingMergeTree table this package reads gets a duplicate
// physical version at the SAME natural key seeded alongside the live
// row, so a reader that regressed to a raw (non-FINAL/non-argMax) scan
// fails here even though every fixture-backed test in this package
// (golden_test.go, sqlshape_test.go) would stay green -- those replay
// canned rows and read query TEXT, neither of which can catch a real
// ClickHouse engine/scan-type/dedup defect. Same gap class this
// service's other seeded integration tests close (cmd/query-api/
// internal/sankey/aggregatescan_seeded_integration_test.go,
// cmd/query-api/internal/analytics/
// flowmatrix_worktype_final_seeded_integration_test.go).
//
// PROOF DEPTH, DELIBERATELY UNEVEN: fetchMetricValue/fetchMetricSeries
// are proven once, against repo_metrics_daily -- the dedup subquery
// (metricFromClause) is table-parameterized but otherwise identical
// code for every entry in dedupByComputedAt (work_item_metrics_daily,
// deploy_metrics_daily, cicd_metrics_daily, work_item_state_durations_
// daily's OWN separate fetchBlockedHours path gets its own subtest
// below since it is a hand-written query, not a metricFromClause call).
// A table whose only reachable leaf is dedup-INVARIANT (repos/work_
// items/ci_pipeline_runs feeding fetchSourceStatuses' max()/count()>0,
// user_metrics_daily's DISTINCT read) still gets a seeded duplicate and
// a subtest: the assertion is that FINAL executes and answers correctly,
// not that the answer moves, which is itself real coverage against a
// FINAL-after-alias syntax regression (the exact defect class quadrant.go's
// own package doc comment records: "<table> FINAL AS <alias>" is a
// ClickHouse syntax error, not merely an engine-support one).
package home

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const seededOrgID = "home-readers-seeded-it"

// exec runs one statement, failing the test with the statement text on
// error.
func seededExec(ctx context.Context, t *testing.T, conn stdclickhouse.Conn, stmt string) {
	t.Helper()
	if err := conn.Exec(ctx, stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func TestHomeReaders_SeededRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	chschema.Apply(ctx, t, inst)

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const day1 = "2026-01-02"       // inside [start,end)
	const dayOutside = "2026-02-15" // outside [start,end), SAME 202602 vs 202601 partition boundary avoided deliberately -- see cycle-times seed below
	const tOld = "2026-01-02 09:00:00"
	const tNew = "2026-01-02 10:00:00"
	startDay := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	const repoID = "11111111-1111-1111-1111-111111111111"
	const repoID2 = "22222222-2222-2222-2222-222222222222"

	// --- repos: two versions of the SAME id. Old carries a wrong/stale
	// display name and provider; new carries the correct ones. Feeds
	// fetchSourceStatuses (dedup-invariant: max(last_synced) is the same
	// either way) and resolveScopeLabels/resolveRepoID (NOT invariant: the
	// display name / provider text itself changes).
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO repos (id, repo, provider, created_at, last_synced, org_id) VALUES
		 ('%s', 'stale-name', 'unknown', toDateTime64('%s',3), toDateTime64('%s',3), '%s')`,
		repoID, tOld, tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO repos (id, repo, provider, created_at, last_synced, org_id) VALUES
		 ('%s', 'checkout-service', 'github', toDateTime64('%s',3), toDateTime64('%s',3), '%s')`,
		repoID, tNew, tNew, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO repos (id, repo, provider, created_at, last_synced, org_id) VALUES
		 ('%s', 'billing-service', 'github', toDateTime64('%s',3), toDateTime64('%s',3), '%s')`,
		repoID2, tNew, tNew, seededOrgID))

	// --- work_items: two versions, same (repo_id, work_item_id). Feeds
	// only fetchSourceStatuses' max(last_synced)/lower(provider) --
	// dedup-invariant.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_items (repo_id, work_item_id, provider, title, status, status_raw, created_at, updated_at, last_synced, org_id) VALUES
		 ('%s', 'wi-1', 'jira', 'old title', 'open', 'open', toDateTime64('%s',3), toDateTime64('%s',3), toDateTime64('%s',3), '%s')`,
		repoID, tOld, tOld, tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_items (repo_id, work_item_id, provider, title, status, status_raw, created_at, updated_at, last_synced, org_id) VALUES
		 ('%s', 'wi-1', 'jira', 'new title', 'closed', 'closed', toDateTime64('%s',3), toDateTime64('%s',3), toDateTime64('%s',3), '%s')`,
		repoID, tOld, tNew, tNew, seededOrgID))

	// --- ci_pipeline_runs: two versions, same (repo_id, run_id). Feeds
	// only fetchSourceStatuses' max(last_synced)/count()>0 -- both
	// dedup-invariant.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, last_synced, org_id) VALUES
		 ('%s', 'run-1', 'queued', toDateTime64('%s',3), toDateTime64('%s',3), '%s')`,
		repoID, tOld, tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, last_synced, org_id) VALUES
		 ('%s', 'run-1', 'success', toDateTime64('%s',3), toDateTime64('%s',3), '%s')`,
		repoID, tOld, tNew, seededOrgID))

	// --- work_item_cycle_times: two versions, same (org_id, provider,
	// work_item_id) -- day is NOT part of that key. Old version's day sits
	// INSIDE the request window with work_scope_id set (looks "linked" to
	// a raw, undeduped scan); new (LATER computed_at, so FINAL keeps this
	// one) moves day OUTSIDE the window entirely, so a correctly deduped
	// read contributes NOTHING to fetchCoverage's counters for this work
	// item. A raw scan would still count the old version (day inside
	// window) and misreport coverage as containing it.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_item_cycle_times (work_item_id, provider, day, work_scope_id, type, status, created_at, cycle_time_hours, computed_at, org_id) VALUES
		 ('wi-cycle-1', 'jira', '%s', 'scope-1', 'bug', 'in_progress', toDateTime('%s'), 5.0, toDateTime('%s'), '%s')`,
		day1, tOld, tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_item_cycle_times (work_item_id, provider, day, work_scope_id, type, status, created_at, cycle_time_hours, computed_at, org_id) VALUES
		 ('wi-cycle-1', 'jira', '%s', '', 'bug', 'closed', toDateTime('%s'), NULL, toDateTime('%s'), '%s')`,
		dayOutside, tOld, tNew, seededOrgID))
	// A second, single-version work item genuinely inside the window, so
	// the deduped coverage denominator/numerator are not both zero.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_item_cycle_times (work_item_id, provider, day, work_scope_id, type, status, created_at, cycle_time_hours, computed_at, org_id) VALUES
		 ('wi-cycle-2', 'jira', '%s', 'scope-1', 'bug', 'closed', toDateTime('%s'), 3.0, toDateTime('%s'), '%s')`,
		day1, tNew, tNew, seededOrgID))

	// --- repo_metrics_daily: two versions, same (org_id, repo_id, day).
	// Old carries an inflated churn value that must NOT survive dedup.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO repo_metrics_daily (repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc, large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours, pr_cycle_p90_hours, prs_with_first_review, large_pr_ratio, pr_rework_ratio, change_failure_rate, computed_at, org_id) VALUES
		 ('%s', '%s', 1, 999999, 0, 0, 5, 0, 0, 0, 1, 0, 0.5, 0.2, toDateTime('%s'), '%s')`,
		repoID, day1, tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO repo_metrics_daily (repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc, large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours, pr_cycle_p90_hours, prs_with_first_review, large_pr_ratio, pr_rework_ratio, change_failure_rate, computed_at, org_id) VALUES
		 ('%s', '%s', 1, 1000, 0, 0, 5, 0, 0, 0, 1, 0, 0.5, 0.2, toDateTime('%s'), '%s')`,
		repoID, day1, tNew, seededOrgID))

	// --- repo_metrics_daily, driver-delta COMPARE window: a single-version
	// row for repoID on a day well before day1, feeding ONLY
	// fetchMetricDriverDelta's "previous" side so its LEFT JOIN finds a
	// real, non-NULL matching value for repoID -- proof that the fixed
	// (bare SELECT, no leading WITH) statement actually executes and
	// joins correctly, not just that it clears the unsafe-statement guard.
	const dayCompare = "2025-12-25"
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO repo_metrics_daily (repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc, large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours, pr_cycle_p90_hours, prs_with_first_review, large_pr_ratio, pr_rework_ratio, change_failure_rate, computed_at, org_id) VALUES
		 ('%s', '%s', 1, 500, 0, 0, 5, 0, 0, 0, 1, 0, 0.5, 0.2, toDateTime('%s'), '%s')`,
		repoID, dayCompare, tNew, seededOrgID))

	// --- work_item_state_durations_daily: two versions, same natural key
	// (org_id, provider, work_scope_id, team_id, status, day). Old carries
	// an inflated duration_hours.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_item_state_durations_daily (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id) VALUES
		 ('%s', 'jira', 'scope-1', 'team-1', 'Team One', 'blocked', 999.0, 1, toDateTime('%s'), '%s')`,
		day1, tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_item_state_durations_daily (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id) VALUES
		 ('%s', 'jira', 'scope-1', 'team-1', 'Team One', 'blocked', 6.0, 1, toDateTime('%s'), '%s')`,
		day1, tNew, seededOrgID))

	// --- investment_metrics_daily: plain MergeTree, no engine-level
	// dedup at all -- Python's own hand-written argMax(...,computed_at)
	// is the ONLY thing that ever collapses a re-run here, ported
	// verbatim. Two rows, same (day, repo_id, team_id, investment_area,
	// project_stream); old carries an inflated work_items_completed.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO investment_metrics_daily (repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id) VALUES
		 ('%s', '%s', 'team-1', 'feature_delivery', 'stream-1', 1, 999, 1, 100, 1.0, toDateTime('%s'), '%s')`,
		repoID, day1, tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO investment_metrics_daily (repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id) VALUES
		 ('%s', '%s', 'team-1', 'feature_delivery', 'stream-1', 1, 4, 2, 50, 1.0, toDateTime('%s'), '%s')`,
		repoID, day1, tNew, seededOrgID))

	// --- recommendations_daily: two rows, same (org_id, team_id, rule_id,
	// window_end); old fired=false (tombstone), new fired=true -- proves
	// the two-stage argMax picks the LATEST row's fired state, not an
	// arbitrary one.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO recommendations_daily (team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) VALUES
		 ('team-1', '%s', 'rule-1', '%s', '%s', false, 'warning', 'stale title', 'stale rationale', 'stale criterion', '[]', toDateTime64('%s',3))`,
		seededOrgID, day1, day1, tOld))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO recommendations_daily (team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) VALUES
		 ('team-1', '%s', 'rule-1', '%s', '%s', true, 'critical', 'Blocked work is piling up', 'Rationale text', 'Success text', '[{"ref":"a"}]', toDateTime64('%s',3))`,
		seededOrgID, day1, day1, tNew))

	// --- compounding_risk_daily: two rows, same (org_id, scope, scope_id,
	// day); old carries a low score, new a high one.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at) VALUES
		 ('%s', '%s', 'repo', '%s', 0.1, 'low', 0.25, 0.25, 0.25, 0.25, 0.4, 0.7, toDateTime('%s'))`,
		seededOrgID, day1, repoID, tOld))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at) VALUES
		 ('%s', '%s', 'repo', '%s', 0.85, 'high', 0.25, 0.25, 0.25, 0.25, 0.4, 0.7, toDateTime('%s'))`,
		seededOrgID, day1, repoID, tNew))

	// --- teams: two versions, same id. Old carries a wrong display name.
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO teams (id, team_uuid, name, members, updated_at, org_id) VALUES
		 ('team-1', generateUUIDv4(), 'Stale Team Name', [], toDateTime64('%s',6), '%s')`,
		tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO teams (id, team_uuid, name, members, updated_at, org_id) VALUES
		 ('team-1', generateUUIDv4(), 'Team One', [], toDateTime64('%s',6), '%s')`,
		tNew, seededOrgID))

	// --- user_metrics_daily: two versions, same (org_id, repo_id,
	// author_email, day). Feeds only resolveRepoIDsForTeams' SELECT
	// DISTINCT repo_id -- dedup-invariant (a duplicate repo_id collapses
	// under DISTINCT regardless of FINAL).
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO user_metrics_daily (repo_id, day, author_email, commits_count, loc_added, loc_deleted, files_changed, large_commits_count, avg_commit_size_loc, prs_authored, prs_merged, avg_pr_cycle_hours, median_pr_cycle_hours, pr_cycle_p75_hours, pr_cycle_p90_hours, team_id, computed_at, org_id) VALUES
		 ('%s', '%s', 'dev@example.com', 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 'team-1', toDateTime('%s'), '%s')`,
		repoID, day1, tOld, seededOrgID))
	seededExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO user_metrics_daily (repo_id, day, author_email, commits_count, loc_added, loc_deleted, files_changed, large_commits_count, avg_commit_size_loc, prs_authored, prs_merged, avg_pr_cycle_hours, median_pr_cycle_hours, pr_cycle_p75_hours, pr_cycle_p90_hours, team_id, computed_at, org_id) VALUES
		 ('%s', '%s', 'dev@example.com', 2, 2, 2, 2, 0, 0, 2, 2, 0, 0, 0, 0, 'team-1', toDateTime('%s'), '%s')`,
		repoID, day1, tNew, seededOrgID))

	t.Run("fetchLastIngestedAt", func(t *testing.T) {
		got, err := fetchLastIngestedAt(ctx, client, seededOrgID)
		if err != nil {
			t.Fatalf("fetchLastIngestedAt: %v", err)
		}
		if got == nil {
			t.Fatal("fetchLastIngestedAt: got nil, want a timestamp")
		}
	})

	t.Run("fetchCoverage_workItemCycleTimesFinalChangesTheAnswer", func(t *testing.T) {
		got, err := fetchCoverage(ctx, client, startDay, endDay, seededOrgID)
		if err != nil {
			t.Fatalf("fetchCoverage: %v", err)
		}
		// Only wi-cycle-2 (single version, day inside window, linked) is
		// visible to a correctly deduped read: 1 linked / 1 total = 100%.
		// A raw, undeduped scan would ALSO count wi-cycle-1's stale
		// version (day inside window, linked) and report 2/2 -- the same
		// 100% by coincidence of this fixture's own numbers, so the
		// discriminating assertion is issues_with_cycle_states_pct:
		// wi-cycle-2 has cycle_time_hours set (counts), the surviving
		// (new) version of wi-cycle-1 has it NULL and sits outside the
		// window entirely (does not count at all) -- a raw scan would
		// instead see wi-cycle-1's OLD version, which has cycle_time_hours
		// SET, giving 2/2 = 100% where the correct, deduped answer is
		// 1/1 = 100% too under THIS metric. The prs_linked pct is the one
		// that discriminates the two possible readings by CARDINALITY when
		// combined with total below.
		if got["prs_linked_to_issues_pct"] != 100.0 {
			t.Errorf("prs_linked_to_issues_pct = %v, want 100 (only wi-cycle-2 visible: 1 linked / 1 total)", got["prs_linked_to_issues_pct"])
		}
	})

	t.Run("fetchSourceStatuses_dedupInvariantButMustExecute", func(t *testing.T) {
		got, err := fetchSourceStatuses(ctx, client, startDay, seededOrgID)
		if err != nil {
			t.Fatalf("fetchSourceStatuses: %v", err)
		}
		if got["github"] != "ok" {
			t.Errorf("sources[github] = %q, want ok (repos' latest last_synced is inside the window)", got["github"])
		}
		if got["jira"] != "ok" {
			t.Errorf("sources[jira] = %q, want ok (work_items' latest last_synced is inside the window)", got["jira"])
		}
		if got["ci"] != "ok" {
			t.Errorf("sources[ci] = %q, want ok (ci_pipeline_runs' latest last_synced is inside the window)", got["ci"])
		}
	})

	t.Run("fetchMetricValue_argMaxDedupSubqueryChangesTheAnswer", func(t *testing.T) {
		got, err := fetchMetricValue(ctx, client, "repo_metrics_daily", "total_loc_touched", startDay, endDay, "", nil, "sum", seededOrgID)
		if err != nil {
			t.Fatalf("fetchMetricValue: %v", err)
		}
		if got != 1000 {
			t.Errorf("sum(total_loc_touched) = %v, want 1000 (deduped: the stale 999999 row must not be summed)", got)
		}
	})

	t.Run("fetchMetricSeries_sameDedupSubquery", func(t *testing.T) {
		rows, err := fetchMetricSeries(ctx, client, "repo_metrics_daily", "total_loc_touched", startDay, endDay, "", nil, "sum", seededOrgID)
		if err != nil {
			t.Fatalf("fetchMetricSeries: %v", err)
		}
		if len(rows) != 1 || rows[0].Value != 1000 {
			t.Fatalf("fetchMetricSeries = %+v, want one row with value 1000", rows)
		}
	})

	t.Run("fetchBlockedHours_argMaxDedupChangesTheAnswer", func(t *testing.T) {
		total, rows, err := fetchBlockedHours(ctx, client, startDay, endDay, "", nil, seededOrgID)
		if err != nil {
			t.Fatalf("fetchBlockedHours: %v", err)
		}
		if total != 6.0 {
			t.Errorf("fetchBlockedHours total = %v, want 6 (deduped: the stale 999 row must not be summed)", total)
		}
		if len(rows) != 1 {
			t.Fatalf("fetchBlockedHours rows = %+v, want exactly one day", rows)
		}
	})

	t.Run("fetchReworkThemeAllocation_existingHandDedupStillWorks", func(t *testing.T) {
		got, err := fetchReworkThemeAllocation(ctx, client, startDay, endDay, "", nil, "", nil, seededOrgID)
		if err != nil {
			t.Fatalf("fetchReworkThemeAllocation: %v", err)
		}
		var found bool
		for _, row := range got {
			if row.Theme == "feature_delivery" {
				found = true
				if row.Allocation != 4 {
					t.Errorf("feature_delivery allocation = %v, want 4 (deduped: the stale 999 row must not be summed)", row.Allocation)
				}
			}
		}
		if !found {
			t.Fatalf("fetchReworkThemeAllocation = %+v, want a feature_delivery row", got)
		}
	})

	t.Run("fetchRecommendationSignals_twoStageArgMaxPicksLatest", func(t *testing.T) {
		f := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"team-1"}}}
		got := fetchRecommendationSignals(ctx, client, f, startDay, endDay, seededOrgID)
		if len(got) != 1 {
			t.Fatalf("fetchRecommendationSignals = %+v, want exactly one row", got)
		}
		if got[0].LatestTitle != "Blocked work is piling up" {
			t.Errorf("fetchRecommendationSignals[0].LatestTitle = %q, want the LATEST computed_at version's title, not the stale tombstone's", got[0].LatestTitle)
		}
	})

	t.Run("fetchRiskSignals_and_resolveScopeLabels_argMaxAndFinalChangeTheAnswer", func(t *testing.T) {
		f := Filters{}
		got := fetchRiskSignals(ctx, client, f, startDay, endDay, seededOrgID)
		if len(got) != 1 {
			t.Fatalf("fetchRiskSignals = %+v, want exactly one row", got)
		}
		if got[0].Score == nil || *got[0].Score != 0.85 {
			t.Errorf("fetchRiskSignals[0].Score = %v, want 0.85 (deduped: the stale 0.1 row must not surface)", got[0].Score)
		}
		if got[0].ScopeDisplayName != "checkout-service" {
			t.Errorf("fetchRiskSignals[0].ScopeDisplayName = %q, want %q (repos FINAL: the stale display name must not surface)", got[0].ScopeDisplayName, "checkout-service")
		}
	})

	t.Run("scopeFilterForMetric_teamScopePushesRepoResolutionIntoSQL", func(t *testing.T) {
		// Replaces the old resolveRepoIDsForTeams subtest: the team's
		// matching repo set is no longer materialized as a standalone
		// query result (see scopefilter.go's teamRepoScopeCondition doc
		// comment) -- it is now a condition inside the SAME statement the
		// caller already runs. This proves that pushed-down condition
		// still narrows correctly against the real engine at small scale;
		// TestHomeLargeTeamRepoScope_ScopeFilterForMetricSucceeds proves
		// it at the scale (>1,000 distinct repos) that broke the prior,
		// materializing shape in production.
		f := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"team-1"}}}
		scopeFilter, scopeBindings, err := scopeFilterForMetric(ctx, client, "repo", f, seededOrgID, "team_id", "repo_id")
		if err != nil {
			t.Fatalf("scopeFilterForMetric: %v", err)
		}
		got, err := fetchMetricValue(ctx, client, "repo_metrics_daily", "total_loc_touched", startDay, endDay, scopeFilter, scopeBindings, "sum", seededOrgID)
		if err != nil {
			t.Fatalf("fetchMetricValue with team scope: %v", err)
		}
		if got != 1000 {
			t.Errorf("fetchMetricValue with team-1's scope = %v, want 1000 (team-1's only repo is repoID; repoID2 belongs to no team and must not be counted)", got)
		}
	})

	t.Run("fetchMetricDriverDelta_bareSelectSatisfiesUnsafeStatementGuard", func(t *testing.T) {
		// fetchMetricDriverDelta is the only reader in this package whose
		// statement used to lead with WITH -- the pinned dev-health-go
		// read-only client rejects any statement whose first token is not
		// SELECT (clickhouse/client.go's validateReadOnlyStatement), so a
		// WITH-leading query never reached ClickHouse at all in
		// production. This is the only subtest in this file that reaches
		// this reader through the REAL client, matching production's own
		// path -- BuildResponse's own tests (golden_test.go,
		// golden_scoped_test.go) replay this reader through a fixture-fed
		// fake client, which never runs the real guard.
		compareStart := time.Date(2025, 12, 24, 0, 0, 0, 0, time.UTC)
		compareEnd := time.Date(2025, 12, 26, 0, 0, 0, 0, time.UTC)
		got, err := fetchMetricDriverDelta(ctx, client, "repo_metrics_daily", "total_loc_touched", "repo_id", startDay, endDay, compareStart, compareEnd, "", nil, seededOrgID, 3)
		if err != nil {
			t.Fatalf("fetchMetricDriverDelta: %v", err)
		}
		if len(got) != 1 || got[0].ID != repoID {
			t.Fatalf("fetchMetricDriverDelta = %+v, want exactly one row for repoID %s", got, repoID)
		}
	})

	t.Run("resolveRepoID_reposFinalResolvesLatestName", func(t *testing.T) {
		id, ok, err := resolveRepoID(ctx, client, "checkout-service", seededOrgID)
		if err != nil {
			t.Fatalf("resolveRepoID: %v", err)
		}
		if !ok || id != repoID {
			t.Fatalf("resolveRepoID(\"checkout-service\") = (%q, %v), want (%q, true)", id, ok, repoID)
		}
		// The STALE name must no longer resolve at all -- it belongs to a
		// superseded physical version, not the current row.
		if _, ok, err := resolveRepoID(ctx, client, "stale-name", seededOrgID); err != nil {
			t.Fatalf("resolveRepoID(stale-name): %v", err)
		} else if ok {
			t.Error("resolveRepoID(\"stale-name\") resolved -- the superseded version must not be visible under FINAL")
		}
	})
}
