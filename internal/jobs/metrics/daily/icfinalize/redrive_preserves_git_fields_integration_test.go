//go:build integration

package icfinalize

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-5151, third defect class. compute_ic.py:143 sets `base = g` for a
// git-backed identity, and `dataclasses.replace(base, ...)` at compute_ic.py:169
// carries EVERY field of that row forward unchanged except the ones IC itself
// derives (identity_id, team_id, loc_touched, prs_opened,
// work_items_completed, work_items_active, delivery_units, cycle_p50_hours,
// cycle_p90_hours). The native executor's write path used to select and write
// back only the columns its own math needed, so every OTHER column
// (commits_count, files_changed, avg_commit_size_loc, prs_with_first_review,
// review_reciprocity, ...) silently dropped to its ClickHouse table default
// on the newly-inserted row -- and since user_metrics_daily's dedup reads
// `ORDER BY computed_at DESC LIMIT 1 BY (...)`, that later, IC-written row is
// the one every downstream reader sees. This test seeds a full row the way
// repouser's own writer would (internal/jobs/metrics/daily/repouser/
// clickhouse.go's column list), runs the finalize executor once, and asserts
// the pass-through columns survive verbatim.
func TestRedrivePreservesGitFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const orgID = "00000000-0000-4000-8000-000000000702"
	repoID := uuid.MustParse("8f5c1f2e-6b4a-4a1e-9f0c-2f2a2d6d5a12")
	day := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)

	// Seed the row the way repouser's own writer would: every pass-through
	// column non-zero, so a dropped column is UNMISTAKABLE (a zero-valued seed
	// would make "defaulted to zero" indistinguishable from "correctly zero").
	if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, team_name,
         commits_count, loc_added, loc_deleted, files_changed, large_commits_count,
         avg_commit_size_loc, prs_authored, prs_merged, avg_pr_cycle_hours,
         median_pr_cycle_hours, pr_cycle_p75_hours, pr_cycle_p90_hours,
         prs_with_first_review, pr_first_review_p50_hours, pr_first_review_p90_hours,
         pr_review_time_p50_hours, pr_pickup_time_p50_hours, reviews_given,
         changes_requested_given, reviews_received, review_reciprocity,
         pr_interruption_load, context_spread_count, review_request_load,
         active_hours, weekend_days, computed_at, org_id)
        VALUES (?, ?, 'preserve@example.com', 'preserve@example.com', 'team-a', 'Team A',
                7, 40, 10, 5, 2,
                12.5, 3, 2, 6.0,
                4.5, 5.5, 12.0,
                2, 3.0, 4.0,
                2.5, 1.5, 6,
                1, 4, 0.75,
                2, 3, 1,
                8.25, 2, ?, ?)`,
		repoID, day, day, orgID); err != nil {
		t.Fatal(err)
	}

	executor := NewExecutor(conn)
	executor.now = func() time.Time { return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC) }
	if _, err := executor.ComputeFinalizeFamily(ctx, RunScope{
		OrganizationID: orgID, TargetDay: day,
	}); err != nil {
		t.Fatalf("ComputeFinalizeFamily: %v", err)
	}

	var (
		commitsCount, filesChanged, largeCommitsCount, prsWithFirstReview uint32
		reviewsGiven, changesRequestedGiven, reviewsReceived              uint32
		prInterruptionLoad, contextSpreadCount, reviewRequestLoad         uint32
		weekendDays                                                       uint8
		avgCommitSizeLOC, avgPRCycleHours, prCycleP75Hours                float64
		reviewReciprocity, activeHours                                    float64
		teamName                                                          string
	)
	err = conn.QueryRow(ctx, `
        SELECT commits_count, files_changed, large_commits_count, prs_with_first_review,
               reviews_given, changes_requested_given, reviews_received,
               pr_interruption_load, context_spread_count, review_request_load,
               weekend_days, avg_commit_size_loc, avg_pr_cycle_hours, pr_cycle_p75_hours,
               review_reciprocity, active_hours, team_name
        FROM (
            SELECT * FROM user_metrics_daily
            ORDER BY computed_at DESC
            LIMIT 1 BY org_id, repo_id, author_email, day
        ) WHERE org_id = ? AND author_email = 'preserve@example.com'`,
		orgID,
	).Scan(&commitsCount, &filesChanged, &largeCommitsCount, &prsWithFirstReview,
		&reviewsGiven, &changesRequestedGiven, &reviewsReceived,
		&prInterruptionLoad, &contextSpreadCount, &reviewRequestLoad,
		&weekendDays, &avgCommitSizeLOC, &avgPRCycleHours, &prCycleP75Hours,
		&reviewReciprocity, &activeHours, &teamName)
	if err != nil {
		t.Fatalf("readback: %v", err)
	}

	// Every one of these MUST survive the finalize write unchanged. A single
	// dropped column reads back as its ClickHouse default (0, 0.0, "").
	cases := []struct {
		name string
		got  any
		want any
	}{
		{"commits_count", commitsCount, uint32(7)},
		{"files_changed", filesChanged, uint32(5)},
		{"large_commits_count", largeCommitsCount, uint32(2)},
		{"prs_with_first_review", prsWithFirstReview, uint32(2)},
		{"reviews_given", reviewsGiven, uint32(6)},
		{"changes_requested_given", changesRequestedGiven, uint32(1)},
		{"reviews_received", reviewsReceived, uint32(4)},
		{"pr_interruption_load", prInterruptionLoad, uint32(2)},
		{"context_spread_count", contextSpreadCount, uint32(3)},
		{"review_request_load", reviewRequestLoad, uint32(1)},
		{"weekend_days", weekendDays, uint8(2)},
		{"avg_commit_size_loc", avgCommitSizeLOC, 12.5},
		{"avg_pr_cycle_hours", avgPRCycleHours, 6.0},
		{"pr_cycle_p75_hours", prCycleP75Hours, 5.5},
		{"review_reciprocity", reviewReciprocity, 0.75},
		{"active_hours", activeHours, 8.25},
		{"team_name", teamName, "Team A"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v after finalize write, want %v (pass-through column dropped)", c.name, c.got, c.want)
		}
	}
}
