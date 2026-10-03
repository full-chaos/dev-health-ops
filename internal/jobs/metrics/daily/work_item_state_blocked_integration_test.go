//go:build integration

package daily

import (
	"context"
	"reflect"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// workItemStateDependenciesDDL is work_item_dependencies as the migration
// chain leaves it (011 + 024 + 027 + 065 + 071): no time zone on last_synced,
// org_id first in the key, the semantics version defaulting to the legacy one.
const workItemStateDependenciesDDL = `CREATE TABLE work_item_dependencies (
    source_work_item_id String, target_work_item_id String, relationship_type String,
    relationship_type_raw String, last_synced DateTime64(3), org_id String,
    source_id Nullable(UUID), relationship_semantics_version String DEFAULT 'legacy.v1'
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, source_work_item_id, target_work_item_id, relationship_type)`

// TestWorkItemStateComputeFamilyWritesBlockedRowsFromOpenBlockers is the
// live-ClickHouse proof of CHAOS-8493 through the production entry point
// (WorkItemStateExecutor.ComputeFamily): the state the rule exists to reach
// is a `blocked` row in work_item_state_durations_daily for an item whose
// status NAME never said "blocked".
//
// Organization A, target day 2026-08-24, four items in repository A. Each is
// created at 00:00 and goes todo -> in_progress at 06:00, so with no blocker
// each contributes todo 6h + in_progress 18h.
//
//	#1  blocked by #2, a stored item of ANOTHER repository, completed at
//	    18:00: blocked 00:00-18:00 (18h), in_progress 18:00-24:00 (6h).
//	#3  its text says "blocked by OPS-9"; one stored linear item carries that
//	    key and is open: blocked all day (24h).
//	#4  a blocking relation that NEITHER end reported at its latest sync (the
//	    link was removed at the provider): not blocked.
//	#5  a relation under the legacy semantics version: not blocked.
//
// Organization B holds the same relation as #1 between its own items of the
// same ids and is not computed; organization A's rows must not change for it,
// and organization B gets no row.
func TestWorkItemStateComputeFamilyWritesBlockedRowsFromOpenBlockers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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

	for _, statement := range []string{
		`CREATE TABLE work_items (
    repo_id UUID, work_item_id String, provider String, status String,
    project_key String, project_id String, native_team_key String, project_name String,
    created_at DateTime64(3, 'UTC'), completed_at Nullable(DateTime64(3, 'UTC')),
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
    source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6),
    is_primary UInt8, confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3), evidence String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at) ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source)`,
		`CREATE TABLE work_item_state_durations_daily (
    day Date, provider String, work_scope_id String, team_id String, team_name String,
    status String, duration_hours Float64, items_touched UInt32, computed_at DateTime,
    avg_wip Float64, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (provider, work_scope_id, team_id, status, day)`,
		workItemStateDependenciesDDL,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA  = "00000000-0000-4000-8000-0000000000a0"
		orgB  = "00000000-0000-4000-8000-0000000000b0"
		repoA = "00000000-0000-4000-8000-0000000000a1"
		repoX = "00000000-0000-4000-8000-0000000000a2" // another repository of org A: the blocker's
		repoB = "00000000-0000-4000-8000-0000000000b1"
		// Every row of one sync carries one last_synced.
		synced = "toDateTime64('2026-08-25 00:00:00', 3, 'UTC')"
		older  = "toDateTime64('2026-08-23 00:00:00', 3, 'UTC')"
		day0   = "toDateTime64('2026-08-24 00:00:00', 3, 'UTC')"
	)
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	item := func(org, repo, id, provider, status, projectID, created, completed string) string {
		return `(toUUID('` + repo + `'), '` + id + `', '` + provider + `', '` + status + `', '', '` + projectID + `', '', '', ` + created + `, ` + completed + `, '` + org + `', ` + synced + `)`
	}
	if err := conn.Exec(ctx, `
INSERT INTO work_items (repo_id, work_item_id, provider, status, project_key, project_id, native_team_key, project_name, created_at, completed_at, org_id, last_synced) VALUES
`+item(orgA, repoA, "gh:a/repo#1", "github", "in_progress", "a/repo", day0, "NULL")+`,
`+item(orgA, repoX, "gh:a/other#2", "github", "done", "a/other", "toDateTime64('2026-08-20 00:00:00', 3, 'UTC')", "toDateTime64('2026-08-24 18:00:00', 3, 'UTC')")+`,
`+item(orgA, repoA, "gh:a/repo#3", "github", "in_progress", "a/repo", day0, "NULL")+`,
`+item(orgA, repoX, "linear:OPS-9", "linear", "todo", "", "toDateTime64('2026-08-01 00:00:00', 3, 'UTC')", "NULL")+`,
`+item(orgA, repoA, "gh:a/repo#4", "github", "in_progress", "a/repo", day0, "NULL")+`,
`+item(orgA, repoA, "gh:a/repo#5", "github", "in_progress", "a/repo", day0, "NULL")+`,
`+item(orgB, repoB, "gh:a/repo#1", "github", "in_progress", "a/repo", day0, "NULL")+`,
`+item(orgB, repoB, "gh:a/other#2", "github", "todo", "a/other", "toDateTime64('2026-08-20 00:00:00', 3, 'UTC')", "NULL")); err != nil {
		t.Fatal(err)
	}
	transition := func(org, repo, id string) string {
		return `(toUUID('` + repo + `'), '` + id + `', toDateTime64('2026-08-24 06:00:00', 3, 'UTC'), 'github', 'todo', 'in_progress', 'Todo', 'In Progress', '', '` + org + `', ` + synced + `)`
	}
	if err := conn.Exec(ctx, `
INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced) VALUES
`+transition(orgA, repoA, "gh:a/repo#1")+`,
`+transition(orgA, repoA, "gh:a/repo#3")+`,
`+transition(orgA, repoA, "gh:a/repo#4")+`,
`+transition(orgA, repoA, "gh:a/repo#5")+`,
`+transition(orgB, repoB, "gh:a/repo#1")); err != nil {
		t.Fatal(err)
	}
	// last_synced has no time zone in this table; the session is UTC.
	relation := func(org, source, target, relationship, raw, version, lastSynced string) string {
		return `('` + source + `', '` + target + `', '` + relationship + `', '` + raw + `', ` + lastSynced + `, '` + org + `', '` + version + `')`
	}
	if err := conn.Exec(ctx, `
INSERT INTO work_item_dependencies (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id, relationship_semantics_version) VALUES
`+relation(orgA, "gh:a/other#2", "gh:a/repo#1", "blocks", "blocked by a/other#2", "canonical-blocks.v2", synced)+`,
`+relation(orgA, "gh:a/repo#3", "extkey:OPS-9", "blocked_by", "external_issue_key", "canonical-blocks.v2", synced)+`,
`+relation(orgA, "linear:OPS-9", "gh:a/repo#4", "blocks", "linear_relation:blocks", "canonical-blocks.v2", older)+`,
`+relation(orgA, "linear:OPS-9", "gh:a/repo#5", "blocks", "blocks", "legacy.v1", synced)+`,
`+relation(orgA, "gh:a/repo#1", "gh:a/repo#3", "relates_to", "relates", "canonical-blocks.v2", synced)+`,
`+relation(orgB, "gh:a/other#2", "gh:a/repo#1", "blocks", "blocked by a/other#2", "canonical-blocks.v2", synced)); err != nil {
		t.Fatal(err)
	}

	executor, err := NewWorkItemStateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	written, err := executor.ComputeFamily(ctx, Run{OrganizationID: orgA, TargetDay: targetDay}, Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repoA)},
	})
	if err != nil {
		t.Fatalf("org A partition: %v", err)
	}
	if written == 0 {
		t.Fatal("org A partition wrote no row")
	}

	type statusTotal struct {
		Hours float64
		Items uint64
	}
	readTotals := func(org string) map[string]statusTotal {
		t.Helper()
		rows, err := conn.Query(ctx, `
SELECT status, sum(duration_hours), sum(items_touched)
FROM work_item_state_durations_daily
WHERE org_id = ? AND day = toDate('2026-08-24')
GROUP BY status`, org)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		totals := map[string]statusTotal{}
		for rows.Next() {
			var (
				status string
				total  statusTotal
			)
			if err := rows.Scan(&status, &total.Hours, &total.Items); err != nil {
				t.Fatal(err)
			}
			totals[status] = total
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return totals
	}

	// #1: blocked 18h + in_progress 6h. #3: blocked 24h. #4 and #5: todo 6h +
	// in_progress 18h each. Four items, 96 hours.
	want := map[string]statusTotal{
		"blocked":     {Hours: 42, Items: 2},
		"in_progress": {Hours: 42, Items: 3},
		"todo":        {Hours: 12, Items: 2},
	}
	if got := readTotals(orgA); !reflect.DeepEqual(got, want) {
		t.Fatalf("org A totals by status = %+v, want %+v", got, want)
	}
	if got := readTotals(orgB); len(got) != 0 {
		t.Fatalf("org B was not computed and has rows: %+v", got)
	}

	// The other tenant, computed on its own: its blocker is open all day, so
	// its one item is blocked for 24 hours -- from ITS relation, not org A's.
	if _, err := executor.ComputeFamily(ctx, Run{OrganizationID: orgB, TargetDay: targetDay}, Partition{
		ID: "00000000-0000-4000-8000-0000000000c3", RunID: "00000000-0000-4000-8000-0000000000c2",
		RepoIDs: []RepositoryID{RepositoryID(repoB)},
	}); err != nil {
		t.Fatalf("org B partition: %v", err)
	}
	if got, wantB := readTotals(orgB), (map[string]statusTotal{"blocked": {Hours: 24, Items: 1}}); !reflect.DeepEqual(got, wantB) {
		t.Fatalf("org B totals by status = %+v, want %+v", got, wantB)
	}
	if got := readTotals(orgA); !reflect.DeepEqual(got, want) {
		t.Fatalf("org A totals changed after org B ran: %+v, want %+v", got, want)
	}
}
