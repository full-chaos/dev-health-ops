//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The stale-key rule through the real executors of ai_impact, ai_governance
// and ic_finalize, on the schema of the migration chain.
//
// Each case starts where a recompute after a team change starts: the table
// holds a row of an earlier compute under a key that the compute of today does
// not produce (the bare id of a team that is now inactive, or a key whose
// input is gone). The executor must leave no measure under that key, must not
// touch a key outside its scope, and must write its own rows as before.
//
// The file uses one symbol of the rule, endStaleKeyRun (see its file for how
// the same test runs on a tree without the rule).

const otherFamiliesOrg = "00000000-0000-4000-8000-00000000d0d3"

var otherFamiliesDay = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

func TestTheOtherTeamKeyedFamiliesLeaveNoMeasureUnderASupersededKey(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	org, day := otherFamiliesOrg, otherFamiliesDay
	api := uuid.MustParse("00000000-0000-4000-8000-0000000000c1")
	other := uuid.MustParse("00000000-0000-4000-8000-0000000000c2")
	earlier := day.Add(30 * time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	scan := func(what, query string, args []any, targets ...any) {
		t.Helper()
		if err := conn.QueryRow(ctx, query, args...).Scan(targets...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}

	// The team was replaced: the bare id is inactive, the keyed id is active
	// and owns the repository. Both hold the same pattern and member.
	t0 := day.Add(-72 * time.Hour)
	for id, active := range map[string]uint8{"platform": 0, "github:platform": 1} {
		exec("insert team "+id, `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)), "Team "+id, []string{"dev@example.com"},
			[]string{"acme/*"}, t0, t0, org, "github", active)
	}
	exec("insert repo", "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)",
		api, "acme/api", org, "github", t0)
	for teamID, primary := range map[string]uint8{"platform": 1, "github:platform": 0} {
		exec("insert ownership by "+teamID, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, ?, ?, ?, ?, 'exact', 'native', ?, ?, ?, ?)`, org, "github", teamID, api, "acme/api", primary, uint16(10), t0, t0)
	}

	t.Run("ai_impact", func(t *testing.T) {
		// An earlier compute left rows under the bare id: one for the
		// repository of the partition and one for a repository of another
		// partition.
		for _, repo := range []uuid.UUID{api, other} {
			exec("insert the earlier ai_impact row", `INSERT INTO ai_impact_metrics_daily
    (org_id, team_id, repo_id, work_type, day, attribution_bucket, prs_total, prs_merged, human_prs, cycle_time_avg_hours, computed_at)
    VALUES (?, 'platform', ?, 'earlier', ?, 'human', 5, 5, 5, 12.5, ?)`, org, repo, day, earlier)
		}
		merged := day.Add(12 * time.Hour)
		exec("insert pull request", `INSERT INTO git_pull_requests
    (repo_id, number, title, state, author_name, author_email, created_at, merged_at, additions, deletions, changed_files, last_synced, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			api, uint32(7), "Add the thing", "merged", "Dev", "dev@example.com", day.Add(2*time.Hour), merged,
			uint32(10), uint32(2), uint32(1), earlier, org)

		executor, err := NewAIImpactExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return earlier.Add(10 * time.Hour) }
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
		partition := Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(api.String())}}
		if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("ai_impact: %v", err)
		}

		var bare, bareNull, keyed, outside uint64
		const held = `SELECT toUInt64(sum(prs_total + prs_merged + human_prs)), toUInt64(countIf(cycle_time_avg_hours IS NOT NULL))
FROM ai_impact_metrics_daily FINAL WHERE org_id = ? AND day = ? AND team_id = ? AND repo_id = ?`
		scan("the bare id, the partition's repository", held, []any{org, day, "platform", api}, &bare, &bareNull)
		if bare != 0 || bareNull != 0 {
			t.Errorf("the inactive team still holds measures for the repository of the partition: counts %d, %d value(s) not NULL", bare, bareNull)
		}
		var ignored uint64
		scan("the keyed id", held, []any{org, day, "github:platform", api}, &keyed, &ignored)
		if keyed == 0 {
			t.Errorf("the compute wrote no pull request under the keyed team: the case does not test a produced key")
		}
		scan("the bare id, the other repository", held, []any{org, day, "platform", other}, &outside, &ignored)
		if outside != 15 {
			t.Errorf("the row of a repository outside the partition holds %d, want its 15: the rule must not leave its scope", outside)
		}
		var total uint64
		scan("every team", `SELECT toUInt64(sum(prs_total)) FROM ai_impact_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND repo_id = ?`, []any{org, day, api}, &total)
		if total != 1 {
			t.Errorf("a read with no team filter counts %d pull request(s) for the repository, want 1", total)
		}
	})

	t.Run("ai_governance", func(t *testing.T) {
		// An earlier compute left a coverage row whose artifacts are gone.
		exec("insert the earlier coverage row", `INSERT INTO ai_governance_coverage_daily
    (org_id, team_id, repo_id, day, ai_artifacts, declared_artifacts, human_reviewed_prs, security_scanned_prs, in_policy_artifacts, computed_at)
    VALUES (?, 'platform', ?, ?, 4, 3, 2, 1, 1, ?)`, org, api, day, earlier)
		executor, err := NewAIGovernanceExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return earlier.Add(10 * time.Hour) }
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
		if _, err := executor.ComputeFamily(ctx, run, Partition{ID: uuid.NewString(), RunID: run.ID}); err != nil {
			t.Fatalf("ai_governance: %v", err)
		}
		// Every partition computes this table for the whole organization, so
		// its stale keys are superseded once, at the end of the run.
		endStaleKeyRun(t, ctx, conn, run, earlier.Add(11*time.Hour))
		var artifacts uint64
		scan("coverage of the day", `SELECT toUInt64(sum(ai_artifacts + declared_artifacts + human_reviewed_prs + security_scanned_prs + in_policy_artifacts))
FROM ai_governance_coverage_daily FINAL WHERE org_id = ? AND day = ?`, []any{org, day}, &artifacts)
		if artifacts != 0 {
			t.Errorf("a day with no artifact still counts %d under an earlier key", artifacts)
		}
	})

	t.Run("ic_finalize", func(t *testing.T) {
		// An earlier compute left the person's point under the bare id.
		exec("insert the earlier landscape point", `INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm, churn_loc_30d, delivery_units_30d, computed_at, org_id)
    VALUES (?, ?, 'dev@example.com', 'platform', 'churn_throughput', 3, 4, 0.5, 0.5, 30, 4, ?, ?)`, uuid.Nil, day, earlier, org)
		exec("insert user metrics", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, commits_count, loc_added, loc_deleted, prs_authored, prs_merged, computed_at, org_id)
    VALUES (?, ?, 'dev@example.com', 3, 40, 10, 1, 1, ?, ?)`, api, day, earlier, org)

		if _, err := NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}); err != nil {
			t.Fatalf("ic_finalize: %v", err)
		}
		const held = `SELECT count(), toFloat64(sum(x_raw + y_raw + x_norm + y_norm + churn_loc_30d + delivery_units_30d))
FROM ic_landscape_rolling_30d FINAL WHERE org_id = ? AND as_of_day = ? AND identity_id = 'dev@example.com' AND team_id = ?`
		var keys uint64
		var measures float64
		scan("the points under the bare id", held, []any{org, day, "platform"}, &keys, &measures)
		if measures != 0 {
			t.Errorf("the inactive team still holds the person's point of the day: %d key(s), measures %v", keys, measures)
		}
		scan("the points under the keyed id", held, []any{org, day, "github:platform"}, &keys, &measures)
		if keys != 3 || measures == 0 {
			t.Errorf("the keyed team holds %d point(s) with measures %v, want the 3 maps of the person", keys, measures)
		}
	})
}
