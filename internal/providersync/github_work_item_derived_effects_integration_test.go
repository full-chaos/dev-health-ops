//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The DDL below is these three tables AS PRODUCTION HAS THEM after the
// migrations that shaped them: 001 (create) and 002 (avg_wip) for state
// durations, 051 for team attributions, 063 for estimate coverage, 024/027 for
// org_id and org_id-first sorting keys. It runs against a throwaway container,
// never a developer's database and never the ask-dev acceptance stack.
//
// The engines are the whole point of this file and are NOT simplified:
//   - estimate coverage is ReplacingMergeTree PARTITION BY toYYYYMM(day)
//   - state durations is PLAIN MergeTree, also partitioned, where FINAL does
//     nothing at all
//   - team attributions is ReplacingMergeTree with NO PARTITION BY
const (
	githubEstimateCoverageDDL = `CREATE TABLE estimate_coverage_metrics_daily (
  day Date,
  provider String,
  work_scope_id String,
  team_id Nullable(String),
  team_name Nullable(String),
  estimated_count UInt32,
  unestimated_count UInt32,
  backlog_size UInt32,
  ratio Nullable(Float64),
  computed_at DateTime64(3, 'UTC'),
  org_id String DEFAULT ''
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, day, provider, work_scope_id, ifNull(team_id, ''))`

	githubStateDurationsDDL = `CREATE TABLE work_item_state_durations_daily (
  day Date,
  provider LowCardinality(String),
  work_scope_id LowCardinality(String),
  team_id LowCardinality(String),
  team_name String,
  status LowCardinality(String),
  duration_hours Float64,
  items_touched UInt32,
  computed_at DateTime('UTC'),
  avg_wip Float64 DEFAULT 0.0,
  org_id String DEFAULT ''
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, provider, work_scope_id, team_id, status, day)`

	githubTeamAttributionsDDL = `CREATE TABLE work_item_team_attributions (
  org_id String,
  repo_id UUID,
  work_item_id String,
  provider String,
  team_id Nullable(String),
  team_name Nullable(String),
  source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6),
  is_primary UInt8,
  confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3),
  evidence String,
  computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source)`
)

const githubDerivedIntegrationOrg = "org-acme"

func githubDerivedIntegrationConn(t *testing.T, ctx context.Context) driver.Conn {
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
		githubEstimateCoverageDDL, githubStateDurationsDDL, githubTeamAttributionsDDL,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	return conn
}

func githubDerivedIntegrationLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

func githubDerivedIntegrationEffect(t *testing.T, destination string, rows any) EffectBatch {
	t.Helper()
	encoded, err := marshalGitHubWorkItemDerivedRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(destination, EffectReadbackRequired, encoded)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func githubDerivedIntegrationIdentity(destination string, rowCount int) GitHubWorkItemEffectIdentity {
	return GitHubWorkItemEffectIdentity{
		OrgID: githubDerivedIntegrationOrg, Provider: "github", Dataset: "work-items",
		Generation: "gen-1", Destination: destination, RowCount: rowCount,
	}
}

// TestGitHubEstimateCoverageReadbackAgainstRealClickHouse proves the partition
// fence with data rather than by reading the SQL. The two rows share EVERY
// sorting-key column except `day`, and their days fall in DIFFERENT MONTHS --
// so they live in different toYYYYMM partitions. FINAL collapses versions only
// within a partition, which is exactly why a readback that omits `day` sees
// both and answers found=2. This is the shape that blocked #1535.
func TestGitHubEstimateCoverageReadbackAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	sink := GitHubEstimateCoverageClickHouseEffects{Conn: conn, Lease: githubDerivedIntegrationLease()}

	teamID, teamName := "t1", "Team One"
	ratio := 0.5
	july := githubEstimateCoverageMetricsDailyRow{
		Day: "2026-07-31", Provider: "github", WorkScopeID: "acme/api",
		TeamID: &teamID, TeamName: &teamName, EstimatedCount: 1, UnestimatedCount: 1,
		BacklogSize: 2, Ratio: &ratio,
		ComputedAt: time.Date(2026, 8, 1, 0, 30, 0, 0, time.UTC),
		OrgID:      githubDerivedIntegrationOrg,
	}
	august := july
	august.Day = "2026-08-01"
	august.ComputedAt = time.Date(2026, 8, 2, 0, 30, 0, 0, time.UTC)

	both := []githubEstimateCoverageMetricsDailyRow{july, august}
	effect := githubDerivedIntegrationEffect(t, githubEstimateCoverageDestination, both)
	identity := githubDerivedIntegrationIdentity(githubEstimateCoverageDestination, len(both))

	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("before write: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	// SNAPSHOT REPLAY: re-inspecting the very same effect must answer Exact,
	// not Absent. An adapter that answered Absent here would rewrite on every
	// recovery replay and duplicate the rows it was verifying.
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
		t.Fatalf("snapshot replay: inspection = %v, err = %v, want EffectExact", inspection, err)
	}
	// And replaying the WRITE must stay Exact too: the RMT version column is
	// computed_at, so re-writing identical rows collapses rather than doubles.
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
		t.Fatalf("write replay: inspection = %v, err = %v, want EffectExact", inspection, err)
	}

	// THE FENCE ITSELF: with both months present, a query that keeps every
	// sorting-key column EXCEPT day returns two rows. That is the state the
	// production readback must never be in, and it is measured here rather
	// than asserted from the SQL text.
	var unfenced uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM estimate_coverage_metrics_daily FINAL
WHERE org_id = ? AND provider = ? AND work_scope_id = ? AND ifNull(team_id, '') = ?`,
		githubDerivedIntegrationOrg, "github", "acme/api", teamID).Scan(&unfenced); err != nil {
		t.Fatal(err)
	}
	if unfenced != 2 {
		t.Fatalf("fence precondition: unfenced count = %d, want 2 -- the two rows must "+
			"straddle a month boundary or this test proves nothing about partitioning", unfenced)
	}

	// A stale persisted version must read as Absent so the writer replaces it.
	stale := august
	stale.ComputedAt = august.ComputedAt.Add(time.Hour)
	staleEffect := githubDerivedIntegrationEffect(t, githubEstimateCoverageDestination,
		[]githubEstimateCoverageMetricsDailyRow{stale})
	staleIdentity := githubDerivedIntegrationIdentity(githubEstimateCoverageDestination, 1)
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, staleIdentity, staleEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("newer expected version: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}

	// A foreign tenant must never satisfy the check.
	foreign := githubDerivedIntegrationIdentity(githubEstimateCoverageDestination, len(both))
	foreign.OrgID = "org-other"
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, foreign, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign tenant: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}
}

// TestGitHubWorkItemStateDurationsReadbackAgainstRealClickHouse proves the
// argMax dedup against a DUPLICATE-PART fixture. The table is plain MergeTree,
// so two inserts of the same natural key genuinely coexist as two rows and
// FINAL would not remove them -- only argMax over computed_at picks the
// current version. Writing twice with different computed_at values is what
// creates that state; asserting the SQL shape would not.
func TestGitHubWorkItemStateDurationsReadbackAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	sink := GitHubWorkItemStateDurationsClickHouseEffects{Conn: conn, Lease: githubDerivedIntegrationLease()}

	older := githubWorkItemStateDurationDailyRow{
		Day: "2026-08-04", Provider: "github", WorkScopeID: "acme/api",
		TeamID: "unassigned", TeamName: "Unassigned", Status: "in_progress",
		DurationHours: 6, ItemsTouched: 1,
		ComputedAt: time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC),
		AvgWIP:     0.25, OrgID: githubDerivedIntegrationOrg,
	}
	newer := older
	newer.DurationHours = 18
	newer.ItemsTouched = 2
	newer.AvgWIP = 0.75
	newer.ComputedAt = time.Date(2026, 8, 6, 0, 30, 0, 0, time.UTC)

	olderEffect := githubDerivedIntegrationEffect(t, githubStateDurationsDestination,
		[]githubWorkItemStateDurationDailyRow{older})
	newerEffect := githubDerivedIntegrationEffect(t, githubStateDurationsDestination,
		[]githubWorkItemStateDurationDailyRow{newer})
	identity := githubDerivedIntegrationIdentity(githubStateDurationsDestination, 1)

	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, olderEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, olderEffect); err != nil || inspection != EffectExact {
		t.Fatalf("snapshot replay of the only version: inspection = %v, err = %v, want EffectExact", inspection, err)
	}
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, newerEffect); err != nil {
		t.Fatal(err)
	}

	// A SECOND STATUS under the same (org, provider, scope, team, day). Without
	// it, dropping `status` from the readback's GROUP BY merges nothing and the
	// grouping key is untestable -- which is exactly how that mutation first
	// survived.
	otherStatus := newer
	otherStatus.Status = "todo"
	otherStatus.DurationHours = 6
	otherStatus.ItemsTouched = 1
	otherStatus.AvgWIP = 0.25
	otherStatusEffect := githubDerivedIntegrationEffect(t, githubStateDurationsDestination,
		[]githubWorkItemStateDurationDailyRow{otherStatus})
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, otherStatusEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, otherStatusEffect); err != nil || inspection != EffectExact {
		t.Fatalf("second status: inspection = %v, err = %v, want EffectExact", inspection, err)
	}

	// DUPLICATE-PART PRECONDITION: both versions must genuinely be present.
	// FINAL is a no-op on plain MergeTree, so if this is not 2 the fixture is
	// not exercising what the argMax exists for.
	var raw uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_state_durations_daily
WHERE org_id = ? AND day = ? AND provider = ? AND work_scope_id = ? AND team_id = ? AND status = ?`,
		githubDerivedIntegrationOrg, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
		"github", "acme/api", "unassigned", "in_progress").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != 2 {
		t.Fatalf("duplicate-part precondition: raw row count = %d, want 2 -- plain "+
			"MergeTree must retain both appended versions or this proves nothing "+
			"about argMax", raw)
	}

	// argMax must report the NEWER version as current...
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, newerEffect); err != nil || inspection != EffectExact {
		t.Fatalf("newer version after duplicate append: inspection = %v, err = %v, want EffectExact", inspection, err)
	}
	// ...and replaying the SUPERSEDED effect must CONFLICT, not read as
	// absent. My first version of this test asserted Absent and the real
	// server disagreed, which was the test being wrong rather than the
	// comparator: "absent" tells the committer to write, and writing a stale
	// generation back over fresher data is a silent regression. The table
	// currently serves duration_hours=18; an effect claiming 6 is not a
	// missing row, it is a contradicting one, and it must fail loudly.
	//
	// The normal ordering -- persisted OLDER than the effect being written --
	// is the Absent case, and the estimate-coverage test covers it. Only the
	// reverse, a replay of a stale generation, reaches here.
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, olderEffect); err != nil || inspection != EffectConflict {
		t.Fatalf("superseded version: inspection = %v, err = %v, want EffectConflict", inspection, err)
	}

	foreign := githubDerivedIntegrationIdentity(githubStateDurationsDestination, 1)
	foreign.OrgID = "org-other"
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, foreign, newerEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign tenant: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}
}

// TestGitHubWorkItemTeamAttributionsReadbackAgainstRealClickHouse covers the
// unpartitioned ReplacingMergeTree, where FINAL deduplicates globally and there
// is no partition key to fence. It also pins the nil-repo case: repo_id is a
// NON-nullable UUID column, so an item with no repo persists as the nil UUID
// and must not be confused with a real one.
func TestGitHubWorkItemTeamAttributionsReadbackAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{Conn: conn, Lease: githubDerivedIntegrationLease()}

	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	teamID, teamName := "t1", "Team One"
	withRepo := githubWorkItemTeamAttributionRow{
		WorkItemID: "acme/api#1", Provider: "github", Source: "repo_ownership",
		IsPrimary: 1, Confidence: "high", Evidence: "repo:acme/api",
		ComputedAt: time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC),
		RepoID:     &repoID, TeamID: &teamID, TeamName: &teamName,
		OrgID: githubDerivedIntegrationOrg,
	}
	// The unassigned shape this table persists: NULL team, nil repo.
	unassigned := githubWorkItemTeamAttributionRow{
		WorkItemID: "acme/api#2", Provider: "github", Source: "unassigned",
		IsPrimary: 1, Confidence: "low", Evidence: "no_candidate",
		ComputedAt: time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC),
		RepoID:     nil, TeamID: nil, TeamName: nil,
		OrgID: githubDerivedIntegrationOrg,
	}

	rows := []githubWorkItemTeamAttributionRow{withRepo, unassigned}
	effect := githubDerivedIntegrationEffect(t, githubTeamAttributionsDestination, rows)
	identity := githubDerivedIntegrationIdentity(githubTeamAttributionsDestination, len(rows))

	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("before write: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	// SNAPSHOT REPLAY, including the NULL-team row: a comparator that mapped
	// NULL to "" on one side only would answer Absent here and rewrite forever.
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
		t.Fatalf("snapshot replay: inspection = %v, err = %v, want EffectExact", inspection, err)
	}
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil || inspection != EffectExact {
		t.Fatalf("write replay: inspection = %v, err = %v, want EffectExact", inspection, err)
	}

	// A different SOURCE for the same work item is a DIFFERENT row, not a new
	// version of the same one: source is part of the sorting key.
	otherSource := withRepo
	otherSource.Source = "assignee_membership"
	otherEffect := githubDerivedIntegrationEffect(t, githubTeamAttributionsDestination,
		[]githubWorkItemTeamAttributionRow{otherSource})
	otherIdentity := githubDerivedIntegrationIdentity(githubTeamAttributionsDestination, 1)
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, otherIdentity, otherEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("different source: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}

	foreign := githubDerivedIntegrationIdentity(githubTeamAttributionsDestination, len(rows))
	foreign.OrgID = "org-other"
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, foreign, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign tenant: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}
}
