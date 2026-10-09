//go:build integration

package operatingreview

import (
	"context"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aigovernance"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This file is the executed proof that fetchWorkItems, fetchRepoMetrics,
// fetchIncidentsAgg and fetchAIImpact return the NEWEST version's value for
// their Nullable(Float64) projected columns, never a stale non-null value
// from an older version. Each subtest seeds an OLDER row with a real value
// and a NEWER row (by computed_at) with that column NULL, for the SAME
// dedup identity, then asserts the reader's returned pointer is nil.

func startOperatingReviewSchema(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return ctx, admin, client
}

func TestFetchWorkItemsReturnsNewestNullHoursNotStaleValue(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	staleHours := 42.0

	insert := `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, items_started, items_completed, wip_count_end_of_day,
		 cycle_time_p50_hours, cycle_time_p90_hours, wip_age_p50_hours, wip_age_p90_hours, org_id, computed_at)
		VALUES (?, 'github', 'scope-1', '', 1, 1, 1, ?, ?, ?, ?, 'org-4547', ?)`
	if err := admin.Exec(ctx, insert, day, staleHours, staleHours, staleHours, staleHours, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := admin.Exec(ctx, insert, day, nil, nil, nil, nil, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	rows, err := fetchWorkItems(ctx, client, "org-4547", nil, day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchWorkItems: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("fetchWorkItems returned %d rows, want 1: %+v", len(rows), rows)
	}
	if rows[0].cycleTimeP50Hours != nil {
		t.Fatalf("cycleTimeP50Hours = %v, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %v instead",
			*rows[0].cycleTimeP50Hours, staleHours)
	}
	if rows[0].wipAgeP50Hours != nil {
		t.Fatalf("wipAgeP50Hours = %v, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %v instead",
			*rows[0].wipAgeP50Hours, staleHours)
	}
}

func TestFetchRepoMetricsReturnsNewestNullHoursNotStaleValue(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	repoID := "77777777-7777-4777-8777-777777777777"
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	staleHours := 99.0

	insert := `INSERT INTO repo_metrics_daily
		(repo_id, day, prs_merged, pr_first_review_p50_hours, mttr_hours, org_id, computed_at)
		VALUES (?, ?, 1, ?, ?, 'org-4547', ?)`
	if err := admin.Exec(ctx, insert, repoID, day, staleHours, staleHours, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := admin.Exec(ctx, insert, repoID, day, nil, nil, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	rows, err := fetchRepoMetrics(ctx, client, "org-4547", day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchRepoMetrics: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("fetchRepoMetrics returned %d rows, want 1: %+v", len(rows), rows)
	}
	if rows[0].prFirstReviewP50Hours != nil {
		t.Fatalf("prFirstReviewP50Hours = %v, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %v instead",
			*rows[0].prFirstReviewP50Hours, staleHours)
	}
	if rows[0].mttrHours != nil {
		t.Fatalf("mttrHours = %v, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %v instead",
			*rows[0].mttrHours, staleHours)
	}
}

func TestFetchIncidentsAggReturnsNewestNullMTTRNotStaleValue(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	repoID := "88888888-8888-4888-8888-888888888888"
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	staleHours := 17.5

	insert := `INSERT INTO incident_metrics_daily
		(repo_id, day, incidents_count, mttr_p50_hours, org_id, computed_at)
		VALUES (?, ?, 1, ?, 'org-4547', ?)`
	if err := admin.Exec(ctx, insert, repoID, day, staleHours, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := admin.Exec(ctx, insert, repoID, day, nil, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	rows, err := fetchIncidentsAgg(ctx, client, "org-4547", day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchIncidentsAgg: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("fetchIncidentsAgg returned %d rows, want 1: %+v", len(rows), rows)
	}
	if rows[0].mttrP50Hours != nil {
		t.Fatalf("mttrP50Hours = %v, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %v instead",
			*rows[0].mttrP50Hours, staleHours)
	}
}

func TestFetchAIImpactReturnsNewestNullRatesNotStaleValues(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	repoID := "99999999-9999-4999-8999-999999999999"
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	staleRate := 3.25

	insert := `INSERT INTO ai_impact_metrics_daily
		(org_id, team_id, repo_id, work_type, day, attribution_bucket,
		 prs_total, ai_assisted_prs, agent_created_prs, human_prs, unknown_prs,
		 ai_cycle_time_delta_hours, ai_review_amplification, rework_drag_rate, test_gap_rate, incident_drag_rate,
		 computed_at)
		VALUES ('org-4547', '', ?, 'feature', ?, 'human', 1, 0, 0, 1, 0, ?, ?, ?, ?, ?, ?)`
	if err := admin.Exec(ctx, insert, repoID, day, staleRate, staleRate, staleRate, staleRate, staleRate, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := admin.Exec(ctx, insert, repoID, day, nil, nil, nil, nil, nil, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	rows, err := fetchAIImpact(ctx, client, "org-4547", nil, day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchAIImpact: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("fetchAIImpact returned %d rows, want 1: %+v", len(rows), rows)
	}
	got := rows[0]
	for name, field := range map[string]*float64{
		"aiCycleTimeDeltaHours": got.aiCycleTimeDeltaHours,
		"aiReviewAmplification": got.aiReviewAmplification,
		"reworkDragRate":        got.reworkDragRate,
		"testGapRate":           got.testGapRate,
		"incidentDragRate":      got.incidentDragRate,
	} {
		if field != nil {
			t.Errorf("%s = %v, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %v instead",
				name, *field, staleRate)
		}
	}
}

// TestFetchAIGovernanceRealClickHouse_RatioOfSumsUsesSelectedRawGroups runs
// the production reader against ClickHouse. The selected team has unequal
// (day, team_id, repo_id) groups, so a mean of ratios would be 0.85 while the
// required ratio of sums is 107/110. Rows from another team and org must not
// enter the reader.
//
// A team with no AI activity has no row: the real rollup stores a group only
// for an AI-detected artifact, so a stored row with 0 in every count is not a
// measurement of "no AI activity". It is the retraction row the daily writer
// stores over a key it no longer produces, and it reads as no row.
func TestFetchAIGovernanceRealClickHouse_RatioOfSumsUsesSelectedRawGroups(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	const (
		org       = "org-8524"
		otherOrg  = "org-8524-other"
		team      = "team-8524"
		otherTeam = "team-8524-other"
		zeroTeam  = "team-8524-zero"
	)
	selectedTeam := team
	zeroActivityTeam := zeroTeam
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	older := day.Add(time.Hour)
	newer := day.Add(2 * time.Hour)

	seed := func(rowOrg, rowTeam, repoID string, aiArtifacts, declaredArtifacts, humanReviewedPrs, securityScannedPrs, inPolicyArtifacts uint64, computedAt time.Time) {
		t.Helper()
		if err := admin.Exec(ctx, `INSERT INTO ai_governance_coverage_daily
			(org_id, team_id, repo_id, day, ai_artifacts, declared_artifacts,
			 human_reviewed_prs, security_scanned_prs, in_policy_artifacts, computed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			rowOrg, rowTeam, repoID, day, aiArtifacts, declaredArtifacts,
			humanReviewedPrs, securityScannedPrs, inPolicyArtifacts, computedAt); err != nil {
			t.Fatalf("seed ai governance row: %v", err)
		}
	}

	// The newer value of this identity is 7/10 in every coverage kind. The
	// older all-zero value must not be selected by the argMax reader.
	seed(org, team, "85240000-0000-4000-8000-000000000001", 10, 0, 0, 0, 0, older)
	seed(org, team, "85240000-0000-4000-8000-000000000001", 10, 7, 7, 7, 7, newer)
	seed(org, team, "85240000-0000-4000-8000-000000000002", 100, 100, 100, 100, 100, newer)
	// Neither of these rows belongs in the selected-team result.
	seed(org, otherTeam, "85240000-0000-4000-8000-000000000003", 1000, 0, 0, 0, 0, newer)
	seed(otherOrg, team, "85240000-0000-4000-8000-000000000004", 1000, 0, 0, 0, 0, newer)
	// The team with no AI activity, through the real rollup: an artifact that
	// is not AI-detected gives no group, so the writer stores no row for it.
	zeroTeamID := zeroTeam
	if produced := aigovernance.RollupCoverageDaily([]aigovernance.Artifact{{
		OrgID: org, TeamID: &zeroTeamID, SubjectType: "pull_request", SubjectID: "1", ObservedAt: day.Add(time.Hour),
	}}, day); len(produced) != 0 {
		t.Fatalf("the rollup stores %+v for a team with no AI-detected artifact, want no row", produced)
	}
	// What the store CAN hold for that team with 0 in every count: a key that
	// was measured once and holds a retraction row now.
	seed(org, zeroTeam, "85240000-0000-4000-8000-000000000005", 3, 3, 3, 3, 3, older)
	seed(org, zeroTeam, "85240000-0000-4000-8000-000000000005", 0, 0, 0, 0, 0, newer)

	rows, err := fetchAIGovernance(ctx, client, org, teamSelection{selectedTeam}, day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchAIGovernance(selected team): %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("fetchAIGovernance(selected team) returned %d rows, want 2: %+v", len(rows), rows)
	}
	const wantCoverage = 107.0 / 110.0
	if got := aiGovernanceCoverage(rows); math.Abs(got-wantCoverage) > 1e-12 {
		t.Errorf("aiGovernanceCoverage(selected team) = %v, want %v", got, wantCoverage)
	}

	allRows, err := fetchAIGovernance(ctx, client, org, nil, day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchAIGovernance(all teams): %v", err)
	}
	if len(allRows) != 3 {
		t.Fatalf("fetchAIGovernance(all teams) returned %d rows, want 3 (the retraction row is not a row): %+v", len(allRows), allRows)
	}
	const wantAllTeamsCoverage = 107.0 / 1110.0
	if got := aiGovernanceCoverage(allRows); math.Abs(got-wantAllTeamsCoverage) > 1e-12 {
		t.Errorf("aiGovernanceCoverage(all teams) = %v, want %v", got, wantAllTeamsCoverage)
	}

	zeroRows, err := fetchAIGovernance(ctx, client, org, teamSelection{zeroActivityTeam}, day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchAIGovernance(zero-activity team): %v", err)
	}
	if len(zeroRows) != 0 {
		t.Fatalf("fetchAIGovernance(zero-activity team) returned %d rows, want none: %+v", len(zeroRows), zeroRows)
	}
	// The coverage of a team with a retraction row only is the coverage the
	// reader gives for no row at all.
	if got, want := aiGovernanceCoverage(zeroRows), aiGovernanceCoverage(nil); got != want {
		t.Errorf("aiGovernanceCoverage(zero-activity team) = %v, want %v (as for no row)", got, want)
	}
}
