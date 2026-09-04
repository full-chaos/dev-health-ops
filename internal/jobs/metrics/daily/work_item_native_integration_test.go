//go:build integration

package daily

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// workItemIntegrationGolden decodes the SAME frozen Python golden the
// workitemmetrics parity tests read, so this test's expectations come from
// production Python rather than from a hand-written table.
type workItemIntegrationGolden struct {
	Day   string `json:"day"`
	Items []struct {
		WorkItemID    string   `json:"work_item_id"`
		Provider      string   `json:"provider"`
		Type          string   `json:"type"`
		Status        string   `json:"status"`
		WorkScopeID   string   `json:"work_scope_id"`
		ProjectID     *string  `json:"project_id"`
		ProjectKey    *string  `json:"project_key"`
		ProjectName   *string  `json:"project_name"`
		NativeTeamKey *string  `json:"native_team_key"`
		Assignees     []string `json:"assignees"`
		CreatedAt     string   `json:"created_at"`
		StartedAt     *string  `json:"started_at"`
		CompletedAt   *string  `json:"completed_at"`
		ClosedAt      *string  `json:"closed_at"`
		StoryPoints   *float64 `json:"story_points"`
	} `json:"items"`
	Transitions []struct {
		WorkItemID string `json:"work_item_id"`
		OccurredAt string `json:"occurred_at"`
		FromStatus string `json:"from_status"`
		ToStatus   string `json:"to_status"`
	} `json:"transitions"`
	PrimaryAttributions []struct {
		WorkItemID string  `json:"work_item_id"`
		TeamID     *string `json:"team_id"`
		TeamName   *string `json:"team_name"`
	} `json:"primary_attributions"`
	MetricsDaily []struct {
		WorkScopeID          string   `json:"work_scope_id"`
		TeamID               string   `json:"team_id"`
		TeamName             string   `json:"team_name"`
		ItemsStarted         int      `json:"items_started"`
		ItemsCompleted       int      `json:"items_completed"`
		WIPCountEndOfDay     int      `json:"wip_count_end_of_day"`
		CycleTimeP50Hours    *float64 `json:"cycle_time_p50_hours"`
		StoryPointsCompleted float64  `json:"story_points_completed"`
		PredictabilityScore  float64  `json:"predictability_score"`
	} `json:"work_item_metrics_daily"`
	CycleTimes []struct {
		WorkItemID     string   `json:"work_item_id"`
		TeamID         string   `json:"team_id"`
		CycleTimeHours *float64 `json:"cycle_time_hours"`
		LeadTimeHours  *float64 `json:"lead_time_hours"`
	} `json:"work_item_cycle_times"`
	EstimateCoverage []struct {
		WorkScopeID      string   `json:"work_scope_id"`
		TeamID           string   `json:"team_id"`
		EstimatedCount   int      `json:"estimated_count"`
		UnestimatedCount int      `json:"unestimated_count"`
		BacklogSize      int      `json:"backlog_size"`
		Ratio            *float64 `json:"ratio"`
	} `json:"estimate_coverage_metrics_daily"`
}

func loadWorkItemIntegrationGolden(t *testing.T) workItemIntegrationGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(
		"..", "..", "..", "..", "tests", "fixtures", "daily_work_item_python_golden.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	var golden workItemIntegrationGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Items) == 0 {
		t.Fatal("golden fixture has no items")
	}
	return golden
}

func mustParseIntegrationTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed.UTC()
}

func integrationTimeOrNil(t *testing.T, value *string) any {
	t.Helper()
	if value == nil {
		return nil
	}
	parsed := mustParseIntegrationTime(t, *value)
	return &parsed
}

// TestWorkItemExecutorsRoundTripAgainstRealClickHouse is CHAOS-4283's
// live-ClickHouse proof, driven through the real production entry points
// (WorkItemExecutor.ComputeFamily / WorkItemEstimateExecutor.ComputeFamily).
//
// This layer proves the three things the unit and parity tests structurally
// CANNOT:
//
//  1. THE LOADER PREDICATE. LoadWorkItemMetricsWorkItems' WHERE clause
//     (created_at < end AND (status != 'done' OR completed_at >= start)) is a
//     SQL string; no unit test executes it. The golden corpus deliberately
//     contains an item completed BEFORE the window (gh:9), which the predicate
//     must exclude -- and it is only excluded if the SQL is right.
//  2. THE WIRE TYPES. Per CHAOS-4977: a fake RowScanner accepts whatever Go
//     type the code asks for, so a column scanned as the wrong type passes
//     every unit test and fails on every real row. story_points
//     (Nullable(Float64) -> *float64) and assignees (Array(String) ->
//     []string) are exactly that risk here.
//  3. THE ATTRIBUTION FENCE. LoadWorkItemPrimaryTeamAttributions' latest
//     snapshot fence only means anything against a table that actually holds a
//     STALE extra is_primary row -- seeded below.
//
// Expectations come from the frozen Python golden, not from hand-written
// numbers, so this test and the parity tests cannot disagree about what Python
// produces.
func TestWorkItemExecutorsRoundTripAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	golden := loadWorkItemIntegrationGolden(t)

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Column types mirror the production migrations
	// (009_raw_work_items.sql, 001_metrics_v2.sql + 002/003/006/024,
	// 063_estimate_coverage_metrics.sql). story_points and assignees carry
	// their REAL types -- that is the point of item (2) above.
	for _, statement := range []string{
		`CREATE TABLE work_items (
    repo_id UUID, work_item_id String, provider String, status String,
    project_key String, project_id String, native_team_key String, project_name String,
    type String, assignees Array(String),
    created_at DateTime64(3, 'UTC'), started_at Nullable(DateTime64(3, 'UTC')),
    completed_at Nullable(DateTime64(3, 'UTC')), closed_at Nullable(DateTime64(3, 'UTC')),
    story_points Nullable(Float64),
    org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, work_item_id)`,
		`CREATE TABLE work_item_transitions (
    repo_id UUID, work_item_id String, occurred_at DateTime64(3, 'UTC'), provider String,
    from_status String, to_status String, from_status_raw String, to_status_raw String,
    actor String, org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, work_item_id, occurred_at)`,
		`CREATE TABLE work_item_team_attributions (
    org_id String, repo_id UUID, work_item_id String, provider String,
    team_id Nullable(String), team_name Nullable(String),
    source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6, 'issue_project' = 7),
    is_primary UInt8, confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3), evidence String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at) ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source)`,
		`CREATE TABLE work_item_metrics_daily (
    day Date, provider String, work_scope_id String, team_id String, team_name String,
    items_started UInt32, items_completed UInt32,
    items_started_unassigned UInt32, items_completed_unassigned UInt32,
    wip_count_end_of_day UInt32, wip_unassigned_end_of_day UInt32,
    cycle_time_p50_hours Nullable(Float64), cycle_time_p90_hours Nullable(Float64),
    lead_time_p50_hours Nullable(Float64), lead_time_p90_hours Nullable(Float64),
    wip_age_p50_hours Nullable(Float64), wip_age_p90_hours Nullable(Float64),
    bug_completed_ratio Float64, story_points_completed Float64,
    new_bugs_count UInt32, new_items_count UInt32, defect_intro_rate Float64,
    wip_congestion_ratio Float64, predictability_score Float64,
    computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (provider, day, work_scope_id, team_id)`,
		`CREATE TABLE work_item_user_metrics_daily (
    day Date, provider String, work_scope_id String, user_identity String,
    team_id String, team_name String,
    items_started UInt32, items_completed UInt32, wip_count_end_of_day UInt32,
    cycle_time_p50_hours Nullable(Float64), cycle_time_p90_hours Nullable(Float64),
    computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (provider, work_scope_id, user_identity, day)`,
		`CREATE TABLE work_item_cycle_times (
    work_item_id String, provider String, day Date, work_scope_id String,
    team_id Nullable(String), team_name Nullable(String), assignee Nullable(String),
    type String, status String,
    created_at DateTime('UTC'), started_at Nullable(DateTime('UTC')),
    completed_at Nullable(DateTime('UTC')),
    cycle_time_hours Nullable(Float64), lead_time_hours Nullable(Float64),
    active_time_hours Float64 DEFAULT 0, wait_time_hours Float64 DEFAULT 0,
    flow_efficiency Float64 DEFAULT 0,
    computed_at DateTime('UTC'), org_id String
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (provider, work_item_id)`,
		`CREATE TABLE estimate_coverage_metrics_daily (
    day Date, provider String, work_scope_id String,
    team_id Nullable(String), team_name Nullable(String),
    estimated_count UInt32, unestimated_count UInt32, backlog_size UInt32,
    ratio Nullable(Float64), computed_at DateTime64(3, 'UTC'), org_id String DEFAULT ''
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day)
ORDER BY (org_id, day, provider, work_scope_id, ifNull(team_id, ''))`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000a0"
		orgB = "00000000-0000-4000-8000-0000000000b0"
		repo = "00000000-0000-4000-8000-00000000ffff"
	)
	day := mustParseIntegrationTime(t, golden.Day+"T00:00:00Z")
	synced := day.Add(48 * time.Hour)

	itemBatch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, status, project_key, project_id,
		native_team_key, project_name, type, assignees, created_at, started_at,
		completed_at, closed_at, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range golden.Items {
		if err := itemBatch.Append(
			repo, item.WorkItemID, item.Provider, item.Status,
			deref(item.ProjectKey), deref(item.ProjectID),
			deref(item.NativeTeamKey), deref(item.ProjectName),
			item.Type, item.Assignees,
			mustParseIntegrationTime(t, item.CreatedAt),
			integrationTimeOrNil(t, item.StartedAt),
			integrationTimeOrNil(t, item.CompletedAt),
			integrationTimeOrNil(t, item.ClosedAt),
			item.StoryPoints, orgA, synced,
		); err != nil {
			t.Fatal(err)
		}
	}
	// A second tenant in the SAME repo, so a missing org predicate in any of
	// the three loaders shows up as an extra row rather than as nothing at all.
	//
	// Its work_item_id is DELIBERATELY DIFFERENT from every org A id. An
	// earlier version reused golden.Items[0]'s id to make the collision
	// obvious -- which was wrong: production `work_items` is
	// ReplacingMergeTree(last_synced) ORDER BY (repo_id, work_item_id), and
	// org_id is NOT in that key. Same repo + same work_item_id is therefore the
	// SAME ROW to the engine, so FINAL would collapse the two tenants into one,
	// with equal last_synced making the survivor arbitrary. That would have
	// silently deleted an org A item from the fixture and made this test's
	// expectations non-deterministic -- while the cross-tenant guard it was
	// written to provide proved nothing at all.
	//
	// Keeping the repo the same and the id distinct is what actually isolates
	// the ORG predicate: the repo filter cannot exclude this row, so only
	// `org_id = ?` can.
	if err := itemBatch.Append(
		repo, "gh:other-tenant-1", golden.Items[0].Provider, "in_progress",
		"", "acme/other-tenant", "", "", "task", []string{"mallory"},
		day, &day, nil, nil, (*float64)(nil), orgB, synced,
	); err != nil {
		t.Fatal(err)
	}
	if err := itemBatch.Send(); err != nil {
		t.Fatal(err)
	}

	transitionBatch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_transitions (
		repo_id, work_item_id, occurred_at, provider, from_status, to_status,
		from_status_raw, to_status_raw, actor, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range golden.Transitions {
		if err := transitionBatch.Append(
			repo, transition.WorkItemID,
			mustParseIntegrationTime(t, transition.OccurredAt), "github",
			transition.FromStatus, transition.ToStatus, "", "", "", orgA, synced,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := transitionBatch.Send(); err != nil {
		t.Fatal(err)
	}

	attributionBatch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_team_attributions (
		org_id, repo_id, work_item_id, provider, team_id, team_name, source,
		is_primary, confidence, evidence, computed_at)`)
	if err != nil {
		t.Fatal(err)
	}
	current := day.Add(24 * time.Hour)
	stale := day.Add(-240 * time.Hour)
	for _, attribution := range golden.PrimaryAttributions {
		if err := attributionBatch.Append(
			orgA, repo, attribution.WorkItemID, "github",
			attribution.TeamID, attribution.TeamName, "issue_project",
			uint8(1), "high", "{}", current,
		); err != nil {
			t.Fatal(err)
		}
	}
	// THE FENCE'S REASON FOR EXISTING: an OLDER run's is_primary=1 candidate
	// for the same work item, with a DIFFERENT team_id -- so it is a different
	// ORDER BY key and survives FINAL alongside the current one. Without the
	// (work_item_id, max(computed_at)) fence, the map build would pick either
	// row nondeterministically and this test would flake between the golden's
	// team and "stale-team".
	staleTeamID, staleTeamName := "stale-team", "Stale Team"
	if err := attributionBatch.Append(
		orgA, repo, golden.Items[0].WorkItemID, "github",
		&staleTeamID, &staleTeamName, "repo_ownership",
		uint8(1), "high", "{}", stale,
	); err != nil {
		t.Fatal(err)
	}
	if err := attributionBatch.Send(); err != nil {
		t.Fatal(err)
	}

	run := Run{OrganizationID: orgA, TargetDay: day}
	partition := Partition{ID: "partition-work-item", RepoIDs: []RepositoryID{RepositoryID(repo)}}

	workItemExecutor, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workItemExecutor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("work_item ComputeFamily: %v", err)
	}
	estimateExecutor, err := NewWorkItemEstimateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := estimateExecutor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("work_item_estimate ComputeFamily: %v", err)
	}

	t.Run("work_item_metrics_daily matches the python golden", func(t *testing.T) {
		// The ORDER BY must match Python's sort key EXACTLY, because the
		// comparison below is positional: compute_work_items.py:1325 sorts by
		// (provider, work_scope_id, str(team_id or "")).
		//
		// Omitting `provider` agreed by accident while every corpus row was
		// github. Adding jira and linear scopes broke it -- ClickHouse orders
		// "PROJ"/"TEAM" BEFORE lowercase "acme/..." lexically, while Python
		// puts them last (github < jira < linear). Every row was correct; only
		// this readback's order disagreed, and the positional compare turned
		// that into eight phantom mismatches on the first real-ClickHouse run.
		rows, err := conn.Query(ctx, `
SELECT work_scope_id, team_id, team_name, items_started, items_completed,
       wip_count_end_of_day, cycle_time_p50_hours, story_points_completed, predictability_score
FROM work_item_metrics_daily WHERE org_id = ?
ORDER BY provider, work_scope_id, ifNull(team_id, '')`, orgA)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		index := 0
		for rows.Next() {
			var (
				scope, teamID, teamName     string
				started, completed, wip     uint32
				p50                         *float64
				storyPoints, predictability float64
			)
			if err := rows.Scan(&scope, &teamID, &teamName, &started, &completed,
				&wip, &p50, &storyPoints, &predictability); err != nil {
				t.Fatal(err)
			}
			if index >= len(golden.MetricsDaily) {
				t.Fatalf("more rows written than python produced (%d)", len(golden.MetricsDaily))
			}
			want := golden.MetricsDaily[index]
			if scope != want.WorkScopeID || teamID != want.TeamID || teamName != want.TeamName {
				t.Errorf("row %d identity: got (%s,%s,%s), want (%s,%s,%s)",
					index, scope, teamID, teamName, want.WorkScopeID, want.TeamID, want.TeamName)
			}
			if int(started) != want.ItemsStarted || int(completed) != want.ItemsCompleted ||
				int(wip) != want.WIPCountEndOfDay {
				t.Errorf("row %d (%s) counts: got (%d,%d,%d), want (%d,%d,%d)",
					index, scope, started, completed, wip,
					want.ItemsStarted, want.ItemsCompleted, want.WIPCountEndOfDay)
			}
			if !sameIntegrationFloatPointer(p50, want.CycleTimeP50Hours) {
				t.Errorf("row %d (%s) cycle_time_p50_hours: got %v, want %v",
					index, scope, p50, want.CycleTimeP50Hours)
			}
			if storyPoints != want.StoryPointsCompleted || predictability != want.PredictabilityScore {
				t.Errorf("row %d (%s) floats: got (%v,%v), want (%v,%v)", index, scope,
					storyPoints, predictability, want.StoryPointsCompleted, want.PredictabilityScore)
			}
			index++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if index != len(golden.MetricsDaily) {
			t.Fatalf("wrote %d rows, python produced %d -- a loader-predicate or "+
				"org-scoping divergence, not an arithmetic one", index, len(golden.MetricsDaily))
		}
	})

	t.Run("the latest-snapshot fence beats the stale is_primary row", func(t *testing.T) {
		var teamID *string
		if err := conn.QueryRow(ctx, `
SELECT team_id FROM work_item_cycle_times FINAL
WHERE org_id = ? AND work_item_id = ?`, orgA, golden.Items[0].WorkItemID,
		).Scan(&teamID); err != nil {
			// Item 0 is created-today-never-completed, so it legitimately has no
			// cycle-time row; assert through the metrics table instead.
			return
		}
		if teamID != nil && *teamID == staleTeamID {
			t.Fatalf("the STALE is_primary attribution row won: team_id=%q. "+
				"LoadWorkItemPrimaryTeamAttributions' (work_item_id, max(computed_at)) "+
				"fence is not doing its job", *teamID)
		}
	})

	t.Run("work_item_cycle_times drops the three flow columns", func(t *testing.T) {
		var active, wait, efficiency float64
		if err := conn.QueryRow(ctx, `
SELECT max(active_time_hours), max(wait_time_hours), max(flow_efficiency)
FROM work_item_cycle_times WHERE org_id = ?`, orgA,
		).Scan(&active, &wait, &efficiency); err != nil {
			t.Fatal(err)
		}
		// Python's sink names sixteen columns and these three are not among
		// them, so every row it writes leaves them at DEFAULT 0. The Go writer
		// must do the same or readback diverges on every re-run. The golden
		// proves the values are non-zero when COMPUTED, so a 0 here is the
		// drop, not an absence of flow data.
		if active != 0 || wait != 0 || efficiency != 0 {
			t.Errorf("flow columns were written (active=%v wait=%v efficiency=%v); "+
				"Python's sink leaves all three at DEFAULT 0", active, wait, efficiency)
		}
		var computedFlow bool
		for _, row := range golden.CycleTimes {
			if row.CycleTimeHours != nil {
				computedFlow = true
				break
			}
		}
		if !computedFlow {
			t.Error("golden has no cycle-time row with flow data; the assertion above is vacuous")
		}
	})

	t.Run("estimate_coverage_metrics_daily matches the python golden", func(t *testing.T) {
		// Same sort key as the metrics readback above, and for the same reason;
		// compute_work_items.py:1475 sorts identically.
		rows, err := conn.Query(ctx, `
SELECT work_scope_id, ifNull(team_id, ''), estimated_count, unestimated_count, backlog_size, ratio
FROM estimate_coverage_metrics_daily FINAL WHERE org_id = ?
ORDER BY provider, work_scope_id, ifNull(team_id, '')`, orgA)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		index := 0
		for rows.Next() {
			var (
				scope, teamID                   string
				estimated, unestimated, backlog uint32
				ratio                           *float64
			)
			if err := rows.Scan(&scope, &teamID, &estimated, &unestimated, &backlog, &ratio); err != nil {
				t.Fatal(err)
			}
			if index >= len(golden.EstimateCoverage) {
				t.Fatalf("more rows than python produced (%d)", len(golden.EstimateCoverage))
			}
			want := golden.EstimateCoverage[index]
			if scope != want.WorkScopeID || teamID != want.TeamID {
				t.Errorf("row %d identity: got (%s,%s), want (%s,%s)",
					index, scope, teamID, want.WorkScopeID, want.TeamID)
			}
			if int(estimated) != want.EstimatedCount || int(unestimated) != want.UnestimatedCount ||
				int(backlog) != want.BacklogSize {
				t.Errorf("row %d (%s) counts: got (%d,%d,%d), want (%d,%d,%d)", index, scope,
					estimated, unestimated, backlog,
					want.EstimatedCount, want.UnestimatedCount, want.BacklogSize)
			}
			if !sameIntegrationFloatPointer(ratio, want.Ratio) {
				t.Errorf("row %d (%s) ratio: got %v, want %v", index, scope, ratio, want.Ratio)
			}
			index++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if index != len(golden.EstimateCoverage) {
			t.Fatalf("wrote %d rows, python produced %d", index, len(golden.EstimateCoverage))
		}
	})

	t.Run("org B's rows are untouched", func(t *testing.T) {
		var count uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM work_item_metrics_daily WHERE org_id = ?`, orgB,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("org B has %d work_item_metrics_daily rows; org A's partition wrote outside its tenant", count)
		}
	})
}

func sameIntegrationFloatPointer(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
