//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The DDL below is the three tables AS PRODUCTION HAS THEM after migrations
// 001 (create), 002/003/006 (added columns), 024 (org_id), 027 (org_id-first
// sorting keys) and 055 (MergeTree -> ReplacingMergeTree(computed_at)). It runs
// against a throwaway container, never a developer's `default` database.
//
// Three details are load-bearing and deliberately not simplified:
//   - the engines are ReplacingMergeTree(computed_at), which is what makes
//     "an older version", "a newer version" and "this version" different
//     answers rather than the same row;
//   - work_item_cycle_times keeps active_time_hours/wait_time_hours/
//     flow_efficiency with their DEFAULT 0, because that is exactly what the
//     Python sink leaves behind and what the Go writer must also leave behind;
//   - team_id/team_name are Nullable on work_item_cycle_times and plain
//     LowCardinality(String) on the two rollups, which is the asymmetry the
//     readback has to survive.
const (
	githubWorkItemMetricsDailyDDL = `CREATE TABLE work_item_metrics_daily (
  day Date,
  provider LowCardinality(String),
  work_scope_id LowCardinality(String),
  team_id LowCardinality(String),
  team_name String,
  items_started UInt32,
  items_completed UInt32,
  items_started_unassigned UInt32,
  items_completed_unassigned UInt32,
  wip_count_end_of_day UInt32,
  wip_unassigned_end_of_day UInt32,
  cycle_time_p50_hours Nullable(Float64),
  cycle_time_p90_hours Nullable(Float64),
  lead_time_p50_hours Nullable(Float64),
  lead_time_p90_hours Nullable(Float64),
  wip_age_p50_hours Nullable(Float64),
  wip_age_p90_hours Nullable(Float64),
  bug_completed_ratio Float64,
  story_points_completed Float64,
  new_bugs_count UInt32 DEFAULT 0,
  new_items_count UInt32 DEFAULT 0,
  defect_intro_rate Float64 DEFAULT 0.0,
  wip_congestion_ratio Float64 DEFAULT 0.0,
  predictability_score Float64 DEFAULT 0.0,
  computed_at DateTime('UTC'),
  org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, provider, day, work_scope_id, team_id)`

	githubWorkItemUserMetricsDailyDDL = `CREATE TABLE work_item_user_metrics_daily (
  day Date,
  provider LowCardinality(String),
  work_scope_id LowCardinality(String),
  user_identity String,
  team_id LowCardinality(String),
  team_name String,
  items_started UInt32,
  items_completed UInt32,
  wip_count_end_of_day UInt32,
  cycle_time_p50_hours Nullable(Float64),
  cycle_time_p90_hours Nullable(Float64),
  computed_at DateTime('UTC'),
  org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, provider, work_scope_id, user_identity, day)`

	githubWorkItemCycleTimesDDL = `CREATE TABLE work_item_cycle_times (
  work_item_id String,
  provider LowCardinality(String),
  day Date,
  work_scope_id LowCardinality(String),
  team_id Nullable(String),
  team_name Nullable(String),
  assignee Nullable(String),
  type LowCardinality(String),
  status LowCardinality(String),
  created_at DateTime('UTC'),
  started_at Nullable(DateTime('UTC')),
  completed_at Nullable(DateTime('UTC')),
  cycle_time_hours Nullable(Float64),
  lead_time_hours Nullable(Float64),
  active_time_hours Float64 DEFAULT 0,
  wait_time_hours Float64 DEFAULT 0,
  flow_efficiency Float64 DEFAULT 0,
  computed_at DateTime('UTC'),
  org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, provider, work_item_id)`
)

func githubWorkItemMetricIntegrationConn(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, statement := range []string{
		githubWorkItemMetricsDailyDDL,
		githubWorkItemUserMetricsDailyDDL,
		githubWorkItemCycleTimesDDL,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	return conn
}

func githubWorkItemMetricIntegrationLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

// TestGitHubWorkItemMetricTripletReadbacksAgainstRealClickHouse drives all three
// adapters through the full write/readback cycle against a real server. Each
// destination runs the same sequence of situations, because all three answer the
// same four questions and getting any of them wrong has the same consequence:
// an effect that can never be verified, or one verified against another
// tenant's or another version's row.
func TestGitHubWorkItemMetricTripletReadbacksAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubWorkItemMetricIntegrationConn(t, ctx)
	lease := githubWorkItemMetricIntegrationLease()
	claim := githubWorkItemMetricTestClaim()
	foreignClaim := claim
	foreignClaim.OrgID = "org-other"

	t.Run(githubWorkItemMetricsDailyDestination, func(t *testing.T) {
		sink := GitHubWorkItemMetricsDailyClickHouseEffects{Conn: conn, Lease: lease}
		row := githubWorkItemMetricTestGroupRow()
		// A NULL percentile alongside non-null ones: the nullable round-trip has
		// to preserve the difference between "no samples" and "zero hours".
		row.LeadTimeP50Hours, row.WIPAgeP50Hours = floatPointer(6.5), nil
		identity, effect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemMetricsDailyDestination, []githubWorkItemMetricsDailyRow{row})

		foreign := row
		foreign.OrgID = foreignClaim.OrgID
		foreignIdentity, foreignEffect := githubWorkItemMetricTestForeignIdentity(t,
			foreignClaim, githubWorkItemMetricsDailyDestination,
			[]githubWorkItemMetricsDailyRow{foreign})
		if err := sink.WriteGitHubWorkItemEffect(ctx, foreignIdentity, foreignEffect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectAbsent,
			"another tenant's row for the same key satisfied this tenant's readback")

		if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectExact,
			"a freshly written row was not recognized")
		// Idempotent re-write: a recovering worker replays the same effect.
		if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectExact,
			"a replayed write stopped being recognized")

		newer := row
		newer.ComputedAt = row.ComputedAt.Add(time.Hour)
		newer.ItemsCompleted = 9
		newerIdentity, newerEffect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemMetricsDailyDestination, []githubWorkItemMetricsDailyRow{newer})
		if err := sink.WriteGitHubWorkItemEffect(ctx, newerIdentity, newerEffect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectConflict,
			"a newer persisted version did not conflict with the older effect")
		assertGitHubWorkItemMetricInspection(t, ctx, sink, newerIdentity, newerEffect, EffectExact,
			"the newest version was not recognized")
	})

	t.Run(githubWorkItemUserMetricsDailyDestination, func(t *testing.T) {
		sink := GitHubWorkItemUserMetricsDailyClickHouseEffects{Conn: conn, Lease: lease}
		row := githubWorkItemMetricTestUserRow()
		row.CycleTimeP50Hours, row.CycleTimeP90Hours = floatPointer(1.25), nil
		identity, effect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemUserMetricsDailyDestination, []githubWorkItemUserMetricsDailyRow{row})

		foreign := row
		foreign.OrgID = foreignClaim.OrgID
		foreignIdentity, foreignEffect := githubWorkItemMetricTestForeignIdentity(t,
			foreignClaim, githubWorkItemUserMetricsDailyDestination,
			[]githubWorkItemUserMetricsDailyRow{foreign})
		if err := sink.WriteGitHubWorkItemEffect(ctx, foreignIdentity, foreignEffect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectAbsent,
			"another tenant's row for the same key satisfied this tenant's readback")

		if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectExact,
			"a freshly written row was not recognized")

		newer := row
		newer.ComputedAt = row.ComputedAt.Add(time.Hour)
		newer.ItemsStarted = 4
		newerIdentity, newerEffect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemUserMetricsDailyDestination, []githubWorkItemUserMetricsDailyRow{newer})
		if err := sink.WriteGitHubWorkItemEffect(ctx, newerIdentity, newerEffect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectConflict,
			"a newer persisted version did not conflict with the older effect")
	})

	t.Run(githubWorkItemCycleTimesDestination, func(t *testing.T) {
		sink := GitHubWorkItemCycleTimesClickHouseEffects{Conn: conn, Lease: lease}
		assignee := "dev@example.com"
		row := githubWorkItemMetricTestCycleRow()
		row.Assignee = &assignee
		row.StartedAt = githubWorkItemMetricTestTime(githubWorkItemMetricTestDay)
		row.CycleTimeHours, row.LeadTimeHours = floatPointer(2), floatPointer(72)
		identity, effect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemCycleTimesDestination, []githubWorkItemCycleTimePersistenceRow{row})

		foreign := row
		foreign.OrgID = foreignClaim.OrgID
		foreignIdentity, foreignEffect := githubWorkItemMetricTestForeignIdentity(t,
			foreignClaim, githubWorkItemCycleTimesDestination,
			[]githubWorkItemCycleTimePersistenceRow{foreign})
		if err := sink.WriteGitHubWorkItemEffect(ctx, foreignIdentity, foreignEffect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectAbsent,
			"another tenant's row for the same work item satisfied this tenant's readback")

		if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectExact,
			"a freshly written row was not recognized")

		// The three flow columns exist on the table and are NOT part of the
		// persisted projection. Python leaves them at DEFAULT 0; so must Go, or
		// a unit that Python wrote and Go re-verified would conflict forever.
		var active, wait, efficiency float64
		if err := conn.QueryRow(ctx, `
SELECT active_time_hours, wait_time_hours, flow_efficiency
FROM work_item_cycle_times FINAL
WHERE org_id = ? AND provider = ? AND work_item_id = ?`,
			row.OrgID, row.Provider, row.WorkItemID,
		).Scan(&active, &wait, &efficiency); err != nil {
			t.Fatal(err)
		}
		if active != 0 || wait != 0 || efficiency != 0 {
			t.Fatalf("the Go writer populated columns the Python sink leaves at their "+
				"default: active=%v wait=%v efficiency=%v", active, wait, efficiency)
		}

		newer := row
		newer.ComputedAt = row.ComputedAt.Add(time.Hour)
		newer.Status = "cancelled"
		newerIdentity, newerEffect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemCycleTimesDestination, []githubWorkItemCycleTimePersistenceRow{newer})
		if err := sink.WriteGitHubWorkItemEffect(ctx, newerIdentity, newerEffect); err != nil {
			t.Fatal(err)
		}
		assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectConflict,
			"a newer persisted version did not conflict with the older effect")
	})
}

// TestGitHubWorkItemMetricReadbackAcceptsRowsPythonWrote is the cross-language
// half. During the migration both implementations write these tables, so a Go
// worker recovering a unit Python already completed MUST recognize Python's rows
// as its own. The inserts below use Python's exact column list -- naming only
// the sixteen columns write_work_item_cycle_times names, and the twenty-six /
// thirteen the two rollups name -- so anything the Go writer adds, omits, or
// coerces differently shows up here as a conflict.
func TestGitHubWorkItemMetricReadbackAcceptsRowsPythonWrote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubWorkItemMetricIntegrationConn(t, ctx)
	lease := githubWorkItemMetricIntegrationLease()

	group := githubWorkItemMetricTestGroupRow()
	group.LeadTimeP50Hours, group.LeadTimeP90Hours = floatPointer(6.5), floatPointer(9.25)
	groupDay, err := group.Day.time()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily (
day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
wip_unassigned_end_of_day, cycle_time_p50_hours, cycle_time_p90_hours,
lead_time_p50_hours, lead_time_p90_hours, wip_age_p50_hours, wip_age_p90_hours,
bug_completed_ratio, story_points_completed, new_bugs_count, new_items_count,
defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at, org_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		groupDay, group.Provider, group.WorkScopeID, group.TeamID, group.TeamName,
		group.ItemsStarted, group.ItemsCompleted, group.ItemsStartedUnassigned,
		group.ItemsCompletedUnassigned, group.WIPCountEndOfDay, group.WIPUnassignedEndOfDay,
		group.CycleTimeP50Hours, group.CycleTimeP90Hours, group.LeadTimeP50Hours,
		group.LeadTimeP90Hours, group.WIPAgeP50Hours, group.WIPAgeP90Hours,
		group.BugCompletedRatio, group.StoryPointsCompleted, group.NewBugsCount,
		group.NewItemsCount, group.DefectIntroRate, group.WIPCongestionRatio,
		group.PredictabilityScore, group.ComputedAt, group.OrgID,
	); err != nil {
		t.Fatal(err)
	}
	groupIdentity, groupEffect := githubWorkItemMetricTestIdentity(t,
		githubWorkItemMetricsDailyDestination, []githubWorkItemMetricsDailyRow{group})
	assertGitHubWorkItemMetricInspection(t, ctx,
		GitHubWorkItemMetricsDailyClickHouseEffects{Conn: conn, Lease: lease},
		groupIdentity, groupEffect, EffectExact,
		"a row written through Python's own column list was not recognized by the Go readback")

	user := githubWorkItemMetricTestUserRow()
	user.CycleTimeP50Hours = floatPointer(1.25)
	userDay, err := user.Day.time()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily (
day, provider, work_scope_id, user_identity, team_id, team_name, items_started,
items_completed, wip_count_end_of_day, cycle_time_p50_hours, cycle_time_p90_hours,
computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		userDay, user.Provider, user.WorkScopeID, user.UserIdentity, user.TeamID,
		user.TeamName, user.ItemsStarted, user.ItemsCompleted, user.WIPCountEndOfDay,
		user.CycleTimeP50Hours, user.CycleTimeP90Hours, user.ComputedAt, user.OrgID,
	); err != nil {
		t.Fatal(err)
	}
	userIdentity, userEffect := githubWorkItemMetricTestIdentity(t,
		githubWorkItemUserMetricsDailyDestination, []githubWorkItemUserMetricsDailyRow{user})
	assertGitHubWorkItemMetricInspection(t, ctx,
		GitHubWorkItemUserMetricsDailyClickHouseEffects{Conn: conn, Lease: lease},
		userIdentity, userEffect, EffectExact,
		"a row written through Python's own column list was not recognized by the Go readback")

	assignee := "dev@example.com"
	cycle := githubWorkItemMetricTestCycleRow()
	cycle.Assignee = &assignee
	cycle.StartedAt = githubWorkItemMetricTestTime(githubWorkItemMetricTestDay)
	cycle.CycleTimeHours, cycle.LeadTimeHours = floatPointer(2), floatPointer(72)
	cycleDay, err := cycle.Day.time()
	if err != nil {
		t.Fatal(err)
	}
	// Exactly the sixteen columns write_work_item_cycle_times names: the three
	// flow columns are left to their DEFAULT, as Python leaves them.
	if err := conn.Exec(ctx, `INSERT INTO work_item_cycle_times (
work_item_id, provider, day, work_scope_id, team_id, team_name, assignee, type,
status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours,
computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cycle.WorkItemID, cycle.Provider, cycleDay, cycle.WorkScopeID, cycle.TeamID,
		cycle.TeamName, cycle.Assignee, cycle.Type, cycle.Status, cycle.CreatedAt,
		cycle.StartedAt, cycle.CompletedAt, cycle.CycleTimeHours, cycle.LeadTimeHours,
		cycle.ComputedAt, cycle.OrgID,
	); err != nil {
		t.Fatal(err)
	}
	cycleIdentity, cycleEffect := githubWorkItemMetricTestIdentity(t,
		githubWorkItemCycleTimesDestination, []githubWorkItemCycleTimePersistenceRow{cycle})
	assertGitHubWorkItemMetricInspection(t, ctx,
		GitHubWorkItemCycleTimesClickHouseEffects{Conn: conn, Lease: lease},
		cycleIdentity, cycleEffect, EffectExact,
		"a row written through Python's own column list was not recognized by the Go readback")
}

func githubWorkItemMetricTestForeignIdentity[T any](
	t *testing.T, claim Claim, destination string, rows []T,
) (GitHubWorkItemEffectIdentity, EffectBatch) {
	t.Helper()
	effect, err := effectBatchFromValues(destination, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := newGitHubWorkItemEffectIdentity(claim, effect)
	if err != nil {
		t.Fatal(err)
	}
	return identity, effect
}

func assertGitHubWorkItemMetricInspection(
	t *testing.T,
	ctx context.Context,
	adapter GitHubWorkItemEffectAdapter,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
	want EffectInspection,
	message string,
) {
	t.Helper()
	got, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect)
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
	if got != want {
		t.Fatalf("%s: inspection = %s, want %s", message, got, want)
	}
}
