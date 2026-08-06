//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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

// TestGitHubDerivedTablesPartitionExpressionsAreDerivableFromTheSortingKey is
// the precondition every partition-fenced readback in this package rests on,
// and it reads the LIVE schema via SHOW CREATE TABLE rather than trusting the
// migration file we believe produced it.
//
// The rule (cross-lane finding, measured on a real container): fencing a
// readback on the partition key is only safe when the partition expression is
// DERIVABLE from the sorting key, so a given key can occupy exactly one
// partition. If a table partitioned on a column outside its sorting key, the
// fence would be actively WRONG under the default server setting: FINAL merges
// across partitions to a single winner, the fence filters that winner out, a
// correctly superseded row reads Absent, and the committer rewrites forever.
//
// All three tables here satisfy the rule -- the two partitioned ones carry
// `day` in their sorting keys, and the third has no PARTITION BY at all -- but
// that is a fact to VERIFY, not to assume, because it is the migration
// authors' choice and not ours.
func TestGitHubDerivedTablesPartitionExpressionsAreDerivableFromTheSortingKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)

	for _, tt := range []struct {
		table           string
		wantPartitioned bool
	}{
		{"estimate_coverage_metrics_daily", true},
		{"work_item_state_durations_daily", true},
		{"work_item_team_attributions", false},
	} {
		t.Run(tt.table, func(t *testing.T) {
			var ddl string
			if err := conn.QueryRow(ctx, "SHOW CREATE TABLE "+tt.table).Scan(&ddl); err != nil {
				t.Fatal(err)
			}
			partitioned := strings.Contains(ddl, "PARTITION BY")
			if partitioned != tt.wantPartitioned {
				t.Fatalf("%s: PARTITION BY present = %v, want %v -- the readback shape "+
					"chosen for this table assumes otherwise\nDDL: %s",
					tt.table, partitioned, tt.wantPartitioned, ddl)
			}
			if !partitioned {
				return
			}
			if !strings.Contains(ddl, "PARTITION BY toYYYYMM(day)") {
				t.Fatalf("%s: partition expression is not toYYYYMM(day); a fence on `day` "+
					"may no longer pin one partition\nDDL: %s", tt.table, ddl)
			}
			orderBy := ddl[strings.Index(ddl, "ORDER BY"):]
			if end := strings.Index(orderBy, "\nSETTINGS"); end > 0 {
				orderBy = orderBy[:end]
			}
			// `day` must be IN the sorting key, so toYYYYMM(day) is derivable
			// from it and one key cannot span two partitions.
			if !strings.Contains(orderBy, "day") {
				t.Fatalf("%s: `day` is NOT in the sorting key, so toYYYYMM(day) is not "+
					"derivable from it and fencing the readback on day is UNSAFE "+
					"under the default do_not_merge_across_partitions_select_final=0"+
					"\nORDER BY: %s", tt.table, orderBy)
			}
		})
	}
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
		ComputedAt: time.Date(2026, 8, 1, 0, 30, 0, 123456789, time.UTC),
		OrgID:      githubDerivedIntegrationOrg,
	}
	august := july
	august.Day = "2026-08-01"
	august.ComputedAt = time.Date(2026, 8, 2, 0, 30, 0, 987654321, time.UTC)

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

	// BOTH SETTINGS. Under the DEFAULT (0) the server may merge across
	// partitions when evaluating FINAL, so a #1535-class fence defect presents
	// as a FALSE CONFLICT (the wrong month's row wins the merge and is
	// compared) rather than as found=2. A test asserting only the found=2
	// symptom therefore passes on a default server while the bug is live.
	// Running the real readback under both settings pins the verdict rather
	// than the symptom: with `day` in the sorting key the answer must be
	// Exact either way.
	for _, merge := range []int{0, 1} {
		settingCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"do_not_merge_across_partitions_select_final": merge,
		}))
		if inspection, err := sink.InspectGitHubWorkItemEffect(settingCtx, identity, effect); err != nil || inspection != EffectExact {
			t.Fatalf("do_not_merge_across_partitions_select_final=%d: inspection = %v, err = %v, want EffectExact",
				merge, inspection, err)
		}
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
		ComputedAt: time.Date(2026, 8, 5, 0, 30, 0, 123456789, time.UTC),
		AvgWIP:     0.25, OrgID: githubDerivedIntegrationOrg,
	}
	newer := older
	newer.DurationHours = 18
	newer.ItemsTouched = 2
	newer.AvgWIP = 0.75
	newer.ComputedAt = time.Date(2026, 8, 6, 0, 30, 0, 987654321, time.UTC)

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
		ComputedAt: time.Date(2026, 8, 5, 0, 30, 0, 123456789, time.UTC),
		RepoID:     &repoID, TeamID: &teamID, TeamName: &teamName,
		OrgID: githubDerivedIntegrationOrg,
	}
	// The unassigned shape this table persists: NULL team, nil repo.
	unassigned := githubWorkItemTeamAttributionRow{
		WorkItemID: "acme/api#2", Provider: "github", Source: "unassigned",
		IsPrimary: 1, Confidence: "low", Evidence: "no_candidate",
		ComputedAt: time.Date(2026, 8, 5, 0, 30, 0, 123456789, time.UTC),
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

	// SAME-SORTING-KEY COLLAPSE. Two candidates for one item that differ ONLY
	// in team_name -- the exact shape the resolver produces when two ownership
	// facts name one team differently -- share a full sorting key, so
	// ReplacingMergeTree stores ONE row. Without deterministic dedup on both
	// the write and the expectation, found==1, the team_name mismatch reads as
	// Conflict, and recovery is wedged permanently rather than transiently.
	collidingA := withRepo
	collidingA.WorkItemID = "acme/api#3"
	collidingA.TeamName = stringPointer("Team One")
	collidingB := collidingA
	collidingB.TeamName = stringPointer("Team Uno")
	colliding := []githubWorkItemTeamAttributionRow{collidingA, collidingB}
	if githubTeamAttributionSortingKey(collidingA) != githubTeamAttributionSortingKey(collidingB) {
		t.Fatal("collision precondition: the two rows must share a sorting key, " +
			"otherwise this case proves nothing about RMT collapse")
	}
	collidingEffect := githubDerivedIntegrationEffect(t, githubTeamAttributionsDestination, colliding)
	collidingIdentity := githubDerivedIntegrationIdentity(githubTeamAttributionsDestination, len(colliding))
	if err := sink.WriteGitHubWorkItemEffect(ctx, collidingIdentity, collidingEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, collidingIdentity, collidingEffect); err != nil || inspection != EffectExact {
		t.Fatalf("colliding sorting keys: inspection = %v, err = %v, want EffectExact "+
			"(a permanent Conflict here is the wedged-recovery defect)", inspection, err)
	}
	// Replay must stay Exact, which is what proves the dedup is deterministic
	// rather than accidentally agreeing once.
	if err := sink.WriteGitHubWorkItemEffect(ctx, collidingIdentity, collidingEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, collidingIdentity, collidingEffect); err != nil || inspection != EffectExact {
		t.Fatalf("colliding sorting keys on replay: inspection = %v, err = %v, want EffectExact", inspection, err)
	}
	var stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_team_attributions FINAL
WHERE org_id = ? AND work_item_id = ?`, githubDerivedIntegrationOrg, "acme/api#3").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 1 {
		t.Fatalf("colliding sorting keys: stored row count = %d, want 1 -- the two "+
			"rows must genuinely collapse or this case is not exercising the defect", stored)
	}

	foreign := githubDerivedIntegrationIdentity(githubTeamAttributionsDestination, len(rows))
	foreign.OrgID = "org-other"
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, foreign, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign tenant: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}
}
