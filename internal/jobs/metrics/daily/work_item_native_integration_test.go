//go:build integration

package daily

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

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
	PredicateExcludedItems []struct {
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
	} `json:"predicate_excluded_items"`
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
		Provider         string   `json:"provider"`
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
	// The loader-widening guard needs more than "an excluded row exists".
	//
	// A non-empty check alone is satisfiable by a row that changes NOTHING when
	// wrongly loaded. Concretely: retarget the excluded row to
	// (gitlab, acme/boundary) and a widened predicate loads it, but it joins
	// the group that scope ALREADY has -- and because estimate coverage creates
	// a bucket BEFORE skipping terminal items, the emitted counts stay
	// identical. The guard would still pass while proving nothing. That is the
	// same present-but-non-discriminating class as CHAOS-5100, arriving inside
	// the fix written to close it.
	//
	// What actually makes the row discriminating is that its GROUP KEY is
	// disjoint from every group the golden emits: only then does loading it
	// necessarily ADD a row the golden does not have. Assert that shape, not
	// its cardinality.
	if len(golden.PredicateExcludedItems) == 0 {
		t.Fatal("golden fixture has no predicate_excluded_items -- the loader-widening guard would be vacuous")
	}
	emittedScopes := make(map[string]struct{}, len(golden.EstimateCoverage))
	for _, row := range golden.EstimateCoverage {
		emittedScopes[row.Provider+"\x00"+row.WorkScopeID] = struct{}{}
	}
	for _, item := range golden.PredicateExcludedItems {
		key := item.Provider + "\x00" + item.WorkScopeID
		if _, collides := emittedScopes[key]; collides {
			t.Fatalf(
				"predicate-excluded item %s has group key (%s, %s), which the golden ALREADY emits: "+
					"if the predicate were widened this row would join that existing bucket instead of "+
					"adding one, the readback would be unchanged, and the widening guard would prove "+
					"nothing. The excluded row's (provider, work_scope_id) must be disjoint from every "+
					"emitted group.",
				item.WorkItemID, item.Provider, item.WorkScopeID,
			)
		}
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

	// Column types AND SORTING KEYS mirror production. The keys come from the
	// LATEST migration that touched each table, which for five of these is a
	// PYTHON migration, not a .sql one:
	//
	//   055_rmt (work_item_daily_rollups) work_item_metrics_daily     ENGINE ReplacingMergeTree(computed_at)
	//                                   work_item_user_metrics_daily ENGINE ReplacingMergeTree(computed_at)
	//   042_rmt_org_id_dedup_keys.py    work_items                  (org_id, repo_id, work_item_id)
	//                                   work_item_transitions       (org_id, repo_id, work_item_id, occurred_at)
	//   027_add_org_id_to_sorting_keys.py
	//                                   work_item_metrics_daily     (org_id, provider, day, work_scope_id, team_id)
	//                                   work_item_user_metrics_daily(org_id, provider, work_scope_id, user_identity, day)
	//                                   work_item_cycle_times       (org_id, provider, work_item_id)
	//
	// An earlier version of this fixture copied the keys from
	// 009_raw_work_items.sql / 001_metrics_v2.sql -- the tables' ORIGINAL DDL,
	// pre-rekey -- and so declared org_id-less keys that production has not
	// used since CHAOS-2290. That is not a cosmetic drift: on the stale
	// work_item_cycle_times key, two tenants' rows for one work_item_id
	// COLLAPSE under ReplacingMergeTree, which production does not do. A
	// fixture that dedups differently from production can only prove things
	// about a schema nobody runs.
	//
	// story_points and assignees carry their REAL types -- that is the point of
	// item (2) above.
	for _, statement := range []string{
		`CREATE TABLE work_items (
    repo_id UUID, work_item_id String, provider String, status String,
    project_key String, project_id String, native_team_key String, project_name String,
    type String, assignees Array(String),
    created_at DateTime64(3, 'UTC'), started_at Nullable(DateTime64(3, 'UTC')),
    completed_at Nullable(DateTime64(3, 'UTC')), closed_at Nullable(DateTime64(3, 'UTC')),
    story_points Nullable(Float64),
    org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, work_item_id)`,
		`CREATE TABLE work_item_transitions (
    repo_id UUID, work_item_id String, occurred_at DateTime64(3, 'UTC'), provider String,
    from_status String, to_status String, from_status_raw String, to_status_raw String,
    actor String, org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, work_item_id, occurred_at)`,
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
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, provider, day, work_scope_id, team_id)`,
		`CREATE TABLE work_item_user_metrics_daily (
    day Date, provider String, work_scope_id String, user_identity String,
    team_id String, team_name String,
    items_started UInt32, items_completed UInt32, wip_count_end_of_day UInt32,
    cycle_time_p50_hours Nullable(Float64), cycle_time_p90_hours Nullable(Float64),
    computed_at DateTime('UTC'), org_id String
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, provider, work_scope_id, user_identity, day)`,
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
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day) ORDER BY (org_id, provider, work_item_id)`,
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

	assertFixtureSchemaMatchesProduction(ctx, t, conn)

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
	// Rows the LOADER PREDICATE must exclude, seeded into the same repo and
	// tenant as everything else. They are absent from the golden's compute
	// input by construction (the generator never passes them to Python), so if
	// the Go predicate is WIDENED they load, produce output Python never
	// produced, and the readbacks below diverge.
	//
	// This is what gives the oracle authority over the predicate. The Go-to-Go
	// guard cannot: codex r2 showed it passes when both loaders are NARROWED,
	// and r3 showed it passes when both are WIDENED. The r2 fix -- an in-scope
	// gitlab item -- only catches narrowing, because a widened predicate still
	// returns it.
	for _, item := range golden.PredicateExcludedItems {
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
	// A second tenant in the SAME repo REUSING an org A work_item_id.
	//
	// Under the CURRENT production key -- (org_id, repo_id, work_item_id), set
	// by 042_rmt_org_id_dedup_keys.py -- these are two DISTINCT rows that
	// coexist, so this is the strongest form of the cross-tenant guard: only
	// the `org_id = ?` predicate can exclude it, since repo and id are both
	// shared.
	//
	// It was briefly changed to a distinct id after this fixture appeared to
	// collapse the two tenants. That collapse was real but the diagnosis was
	// half right: the cause was this fixture declaring the PRE-rekey key
	// (repo_id, work_item_id), not anything about sharing an id. With the key
	// corrected to production's, sharing the id is safe AND is the better test.
	if err := itemBatch.Append(
		repo, golden.Items[0].WorkItemID, golden.Items[0].Provider, "in_progress",
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

	// CHAOS-5323/CHAOS-3092: "estimate_coverage_metrics_daily matches the
	// python golden" used to live here -- deleted, not fixed, because its
	// data source is gone by this PR's own design: the shared golden
	// generator script no longer calls compute_estimate_coverage_metrics_daily
	// (deleted entirely), so `golden.EstimateCoverage` is now permanently empty and
	// this subtest could only ever fail the moment the native Go executor
	// wrote a single real row. Go-vs-Python parity for this family still has
	// dedicated, non-redundant coverage:
	// TestComputeEstimateCoverageMatchesPythonGolden (workitemmetrics/
	// golden_test.go), which compares against its OWN frozen golden, not
	// this shared one.

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

// productionSchema is what the LATEST migration touching each table declares --
// engine (with its version column) AND sorting key, read straight back out of
// system.tables so the fixture cannot drift from production silently.
//
// Every value here cites the migration that set it. Note how many are PYTHON
// migrations: a .sql-only search finds none of them, which is exactly how this
// fixture drifted twice.
//
//	009_raw_work_items.sql / 001_metrics_v2.sql   original DDL (superseded)
//	027_add_org_id_to_sorting_keys.py             org_id-first sorting keys
//	042_rmt_org_id_dedup_keys.py                  org_id-first RMT dedup keys
//	055_work_item_daily_rollups_replacing_merge_tree.py
//	                                              the two daily rollups ->
//	                                              ReplacingMergeTree(computed_at)
//
// THE TRAP THIS CLOSES. An earlier version of this fixture copied the ORIGINAL
// .sql DDL and so declared pre-rekey ORDER BYs. That was fixed by auditing the
// KEYS -- and only the keys. Migration 055 had ALSO changed the ENGINE of two
// of the same tables, from MergeTree to ReplacingMergeTree(computed_at), and
// that went unnoticed because the previous fix asked "are the keys right?"
// rather than "is every clause derived from the latest migration?". A fixture
// that dedups differently from production can only prove things about a schema
// nobody runs -- and with the wrong ENGINE it does exactly that while every
// sorting key looks correct.
//
// So this asserts the whole shape, for every table, not the clause that last
// failed.
var productionSchema = []struct {
	table      string
	engine     string // engine_full's leading engine expression, incl. version column
	sortingKey string
}{
	{"work_items", "ReplacingMergeTree(last_synced)", "org_id, repo_id, work_item_id"},
	{"work_item_transitions", "ReplacingMergeTree(last_synced)", "org_id, repo_id, work_item_id, occurred_at"},
	{"work_item_team_attributions", "ReplacingMergeTree(computed_at)", "org_id, repo_id, work_item_id, ifNull(team_id, ''), source"},
	{"work_item_metrics_daily", "ReplacingMergeTree(computed_at)", "org_id, provider, day, work_scope_id, team_id"},
	{"work_item_user_metrics_daily", "ReplacingMergeTree(computed_at)", "org_id, provider, work_scope_id, user_identity, day"},
	{"work_item_cycle_times", "ReplacingMergeTree(computed_at)", "org_id, provider, work_item_id"},
	{"estimate_coverage_metrics_daily", "ReplacingMergeTree(computed_at)", "org_id, day, provider, work_scope_id, ifNull(team_id, '')"},
}

// assertFixtureSchemaMatchesProduction reads engine and sorting key back from
// system.tables. Reading them back matters: asserting against the CREATE
// strings this file just executed would only prove the file agrees with itself.
func assertFixtureSchemaMatchesProduction(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()
	for _, want := range productionSchema {
		var engine, sortingKey string
		row := conn.QueryRow(ctx,
			`SELECT engine_full, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = ?`,
			want.table)
		if err := row.Scan(&engine, &sortingKey); err != nil {
			t.Fatalf("read schema for %s: %v", want.table, err)
		}
		if !strings.HasPrefix(engine, want.engine) {
			t.Fatalf("%s engine is %q, want it to start %q -- the fixture must declare the engine the LATEST migration sets "+
				"(055 converted the daily rollups to ReplacingMergeTree(computed_at)); a fixture that dedups differently "+
				"from production proves nothing about production", want.table, engine, want.engine)
		}
		if sortingKey != want.sortingKey {
			t.Fatalf("%s sorting key is %q, want %q -- rekeyed by the PYTHON migrations 027/042, which a .sql-only search misses",
				want.table, sortingKey, want.sortingKey)
		}
	}
}
