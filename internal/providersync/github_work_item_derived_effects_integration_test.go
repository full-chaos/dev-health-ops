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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This file authors NO DDL. The three tables come from the real migration
// chain (src/dev_health_ops/migrations/clickhouse), applied by chschema through
// the project's own canonical entrypoint.
//
// The previous hand-typed CREATE TABLE constants were a second, unversioned
// copy of the schema, and the SHOW CREATE TABLE test below could only ever
// confirm what those constants declared. They had already drifted: the
// work_item_team_attributions copy carried the PRE-053 enums, missing the
// `issue_project` and `manual_fallback` source values and the `manual` and
// `none` confidence values that the production resolver genuinely emits. The
// unassigned fixture in this file had been written as confidence "low" -- a
// value the resolver never produces -- which is exactly what kept the stale
// enum from failing anything.
//
// The engines remain the whole point of this file, and now they are the
// migrations' engines rather than our restatement of them:
//   - estimate coverage is ReplacingMergeTree PARTITION BY toYYYYMM(day)
//   - state durations is PLAIN MergeTree, also partitioned, where FINAL does
//     nothing at all
//   - team attributions is ReplacingMergeTree with NO PARTITION BY

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
	// Migrate BEFORE opening the Go connection, so a failure to apply the real
	// chain surfaces as a migration failure rather than as a confusing "table
	// does not exist" from the first query.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// githubWorkItemDerivedEmittableSources and ...Confidences are every value the
// production resolver can put in these two enum columns
// (github_work_items_derivation_context.go:327,404,462,491,510,561 and
// confidenceForPrimary). "low" is deliberately ABSENT: the resolver never emits
// it, and the fixture in this file that used to claim it was written to fit a
// stale hand-typed enum rather than to describe anything production produces.
var (
	githubWorkItemDerivedEmittableSources = []string{
		"native_team", "issue_project", "project_ownership", "repo_ownership",
		"assignee_membership", "linked_issue", "author_membership",
		"manual_fallback", "unassigned",
	}
	githubWorkItemDerivedEmittableConfidences = []string{
		"high", "medium", "manual", "none",
	}
)

// TestGitHubWorkItemTeamAttributionEnumsAcceptEveryResolverValue closes the
// drift class directly, rather than trusting that sourcing the schema from the
// migrations happens to keep it closed.
//
// The hand-typed DDL this file used to create carried the PRE-053 enums, so
// `issue_project`, `manual_fallback`, `manual` and `none` -- all genuinely
// reachable resolver outputs -- would have been REJECTED with
// UNKNOWN_ELEMENT_OF_ENUM by the real column. Nothing failed, because no
// fixture ever used one.
//
// This asserts against the enum the MIGRATION CHAIN produced, and the value
// list comes from the producer, so it fails if either side moves: a new
// resolver source without a migration, or a migration that narrows an enum.
func TestGitHubWorkItemTeamAttributionEnumsAcceptEveryResolverValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)

	for _, tt := range []struct {
		column string
		values []string
	}{
		{"source", githubWorkItemDerivedEmittableSources},
		{"confidence", githubWorkItemDerivedEmittableConfidences},
	} {
		t.Run(tt.column, func(t *testing.T) {
			var columnType string
			if err := conn.QueryRow(ctx,
				"SELECT type FROM system.columns WHERE table = 'work_item_team_attributions' "+
					"AND name = ? AND database = currentDatabase()", tt.column,
			).Scan(&columnType); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(columnType, "Enum") {
				t.Fatalf("%s is %s, not an Enum -- this guard assumes an enum and would "+
					"otherwise pass vacuously", tt.column, columnType)
			}
			for _, value := range tt.values {
				// Match the quoted enum element, not a bare substring: 'none'
				// must not be satisfied by a hypothetical 'none_given'.
				if !strings.Contains(columnType, "'"+value+"'") {
					t.Errorf("the resolver can emit %s=%q but the migrated column cannot "+
						"store it, so a real row would be rejected with "+
						"UNKNOWN_ELEMENT_OF_ENUM\ncolumn type: %s",
						tt.column, value, columnType)
				}
			}
		})
	}
}

// githubDerivedSortingKeyColumns extracts the sorting key's top-level entries
// from a SHOW CREATE TABLE body.
//
// A substring test is NOT good enough here: `strings.Contains(orderBy, "day")`
// is satisfied by a column named `birthday`, by `toYYYYMM(day)` appearing in
// the PARTITION BY clause the slice may still include, and by a column named
// `day_bucket` -- none of which put `day` itself in the key. The whole point of
// the assertion is that the partition expression is derivable from the sorting
// key, so it has to compare real key entries.
func githubDerivedSortingKeyColumns(t *testing.T, ddl string) []string {
	t.Helper()
	marker := "\nORDER BY "
	start := strings.Index(ddl, marker)
	if start < 0 {
		t.Fatalf("no ORDER BY clause in DDL:\n%s", ddl)
	}
	clause := ddl[start+len(marker):]
	if end := strings.IndexAny(clause, "\n"); end >= 0 {
		clause = clause[:end]
	}
	clause = strings.TrimSpace(clause)
	// A single-column key has no parentheses; a tuple key does.
	if strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") {
		clause = clause[1 : len(clause)-1]
	}
	var columns []string
	depth, current := 0, strings.Builder{}
	flush := func() {
		if value := strings.TrimSpace(current.String()); value != "" {
			columns = append(columns, value)
		}
		current.Reset()
	}
	for _, symbol := range clause {
		switch {
		case symbol == '(':
			depth++
			current.WriteRune(symbol)
		case symbol == ')':
			depth--
			current.WriteRune(symbol)
		case symbol == ',' && depth == 0:
			// Only a top-level comma separates key entries; a comma inside
			// ifNull(team_id, '') does not.
			flush()
		default:
			current.WriteRune(symbol)
		}
	}
	flush()
	return columns
}

// TestGitHubDerivedTablesPartitionExpressionsAreDerivableFromTheSortingKey is
// the precondition every partition-fenced readback in this package rests on.
//
// It is only meaningful because the schema now comes from the real migration
// chain. While the tables were created from hand-typed constants in this file,
// reading them back via SHOW CREATE TABLE proved nothing beyond "ClickHouse
// stored what we just typed" -- the assertion and the input had the same
// author.
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
			// `day` must be IN the sorting key, so toYYYYMM(day) is derivable
			// from it and one key cannot span two partitions.
			columns := githubDerivedSortingKeyColumns(t, ddl)
			present := false
			for _, column := range columns {
				if column == "day" {
					present = true
					break
				}
			}
			if !present {
				t.Fatalf("%s: `day` is NOT a column of the sorting key, so toYYYYMM(day) "+
					"is not derivable from it and fencing the readback on day is UNSAFE "+
					"under the default do_not_merge_across_partitions_select_final=0"+
					"\nsorting key columns: %q", tt.table, columns)
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
	//
	// Confidence is "none", which is what the production resolver actually
	// emits for this candidate (derivation_context.go:462). It was "low"
	// before -- a value the resolver never produces -- because "none" only
	// entered the enum in migration 053 and would have been REJECTED by the
	// hand-typed DDL this file used to create. The fixture was shaped to fit
	// the stale copy of the schema instead of the schema failing.
	unassigned := githubWorkItemTeamAttributionRow{
		WorkItemID: "acme/api#2", Provider: "github", Source: "unassigned",
		IsPrimary: 1, Confidence: "none", Evidence: "no_candidate",
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

// Each readback fences the FULL sorting key, and until now every readback test
// wrote a single row per natural key -- so dropping any one fence still
// selected exactly that row, `found` stayed 1, and the verdict stayed Exact.
// Four mutations that neutralise a fence therefore SURVIVED while reading as
// covered.
//
// The missing ingredient is a SIBLING row: one that differs from the expected
// row only in the fenced column, so it is a distinct sorting key that the
// fence is the only thing excluding. With the fence intact both rows read back
// Exact; with it dropped the query matches both, found becomes 2, and the
// comparator answers Conflict.
//
// These are written as separate top-level tests per table because the three
// tables do not share an engine and each fence fails for its own reason.
func TestGitHubEstimateCoverageReadbackFencesTeamID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	sink := GitHubEstimateCoverageClickHouseEffects{Conn: conn, Lease: githubDerivedIntegrationLease()}

	stamp := time.Date(2026, 8, 5, 0, 30, 0, 123456789, time.UTC)
	ratio := 0.5
	row := func(teamID, teamName string) githubEstimateCoverageMetricsDailyRow {
		id, name := teamID, teamName
		return githubEstimateCoverageMetricsDailyRow{
			Day: "2026-08-04", Provider: "github", WorkScopeID: "acme/api",
			TeamID: &id, TeamName: &name,
			EstimatedCount: 1, UnestimatedCount: 1, BacklogSize: 2, Ratio: &ratio,
			ComputedAt: stamp, OrgID: githubDerivedIntegrationOrg,
		}
	}
	// Same day, provider and work_scope_id; ONLY team_id differs, which is the
	// last component of this table's sorting key.
	rows := []githubEstimateCoverageMetricsDailyRow{row("t1", "Team One"), row("t2", "Team Two")}
	effect := githubDerivedIntegrationEffect(t, githubEstimateCoverageDestination, rows)
	identity := githubDerivedIntegrationIdentity(githubEstimateCoverageDestination, len(rows))

	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil ||
		inspection != EffectExact {
		t.Fatalf("two teams on one (day, provider, work_scope_id): inspection = %v, err = %v, "+
			"want EffectExact -- a readback that does not fence ifNull(team_id,'') sees both "+
			"rows and answers found=2", inspection, err)
	}
}

func TestGitHubWorkItemTeamAttributionsReadbackFencesTeamID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{Conn: conn, Lease: githubDerivedIntegrationLease()}

	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	stamp := time.Date(2026, 8, 5, 0, 30, 0, 123456789, time.UTC)
	row := func(teamID, teamName string) githubWorkItemTeamAttributionRow {
		id, name := teamID, teamName
		return githubWorkItemTeamAttributionRow{
			WorkItemID: "acme/api#1", Provider: "github", Source: "assignee_membership",
			IsPrimary: 1, Confidence: "high", Evidence: "assignee_membership=" + teamID,
			ComputedAt: stamp, RepoID: &repoID, TeamID: &id, TeamName: &name,
			OrgID: githubDerivedIntegrationOrg,
		}
	}
	// One work item legitimately carries several attribution candidates that
	// differ only in team_id -- two members of different teams assigned to it.
	rows := []githubWorkItemTeamAttributionRow{row("t1", "Team One"), row("t2", "Team Two")}
	effect := githubDerivedIntegrationEffect(t, githubTeamAttributionsDestination, rows)
	identity := githubDerivedIntegrationIdentity(githubTeamAttributionsDestination, len(rows))

	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil ||
		inspection != EffectExact {
		t.Fatalf("two teams on one (repo_id, work_item_id, source): inspection = %v, err = %v, "+
			"want EffectExact -- a readback that does not fence ifNull(team_id,'') compares "+
			"against a sibling candidate's row", inspection, err)
	}
}

func TestGitHubWorkItemStateDurationsReadbackFencesDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	sink := GitHubWorkItemStateDurationsClickHouseEffects{Conn: conn, Lease: githubDerivedIntegrationLease()}

	stamp := time.Date(2026, 8, 5, 0, 30, 0, 0, time.UTC)
	row := func(day githubWorkItemDerivedDay, hours float64) githubWorkItemStateDurationDailyRow {
		return githubWorkItemStateDurationDailyRow{
			Day: day, Provider: "github", WorkScopeID: "acme/api",
			TeamID: "t1", TeamName: "Team One", Status: "in_progress",
			DurationHours: hours, ItemsTouched: 1, ComputedAt: stamp,
			AvgWIP: hours / 24.0, OrgID: githubDerivedIntegrationOrg,
		}
	}
	// `day` is in this table's GROUP BY, so without the day fence the query
	// returns one group PER DAY for the same natural key. Two days inside ONE
	// month, so this is about the grouping and not about partition pruning.
	rows := []githubWorkItemStateDurationDailyRow{
		row("2026-08-04", 3), row("2026-08-05", 5),
	}
	effect := githubDerivedIntegrationEffect(t, githubStateDurationsDestination, rows)
	identity := githubDerivedIntegrationIdentity(githubStateDurationsDestination, len(rows))

	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil ||
		inspection != EffectExact {
		t.Fatalf("two days on one natural key: inspection = %v, err = %v, want EffectExact -- "+
			"a readback that does not fence `day` groups both days and answers found=2",
			inspection, err)
	}
}
