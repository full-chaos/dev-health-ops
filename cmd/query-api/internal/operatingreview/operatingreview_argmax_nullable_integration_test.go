//go:build integration

package operatingreview

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

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
