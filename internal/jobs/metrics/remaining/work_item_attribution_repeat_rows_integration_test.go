//go:build integration

package remaining

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// CHAOS-9548: an ownership sync that repeats an open row no longer writes it
// again, so the row keeps the updated_at of its last real change. detectScope
// compares that updated_at with the watermark of the last attribution run.
// Two orgs hold the same ownership; the first has every row written again at
// two syncs after the last run (what a writer did before), the second has each
// row once.
//
//   - A real change after the last run (a row closed, a row opened) flags the
//     same scopes in both orgs.
//   - A repeat flags a scope only in the org whose rows were written again:
//     that scope was rederived to the same rows, so the answer that ends is
//     "rederive every owned repo at every sync".
func TestDetectScopeFlagsARealOwnershipChangeAndNotARepeatedRow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	lastRun := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	changed := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	repoKept, repoClosed, repoOpened := uuid.New(), uuid.New(), uuid.New()
	projectKey := "PLAT"

	seed := func(org string, repeats int) {
		t.Helper()
		for sync := 0; sync <= repeats; sync++ {
			updatedAt := first
			if sync > 0 {
				updatedAt = lastRun.Add(time.Duration(sync) * time.Hour)
			}
			seedWorkItemAttributionRepoOwnershipWithValidity(t, ctx, conn, org, repoKept, "team-a", updatedAt, first, nil)
			seedWorkItemAttributionRepoOwnershipWithValidity(t, ctx, conn, org, repoClosed, "team-b", updatedAt, first, nil)
			seedWorkItemAttributionProjectOwnershipAt(t, ctx, conn, org, "jira", "P-1", &projectKey, "team-a", first, updatedAt)
		}
		if err := writer.WriteAttributionRun(ctx, WorkItemAttributionRunRecord{OrgID: org, RunID: "run-" + org, CompletedAt: lastRun}); err != nil {
			t.Fatalf("seed run marker: %v", err)
		}
	}
	const repeated, once = "org-rewritten-at-every-sync-" + "9548", "org-written-once-9548"
	seed(repeated, 2)
	seed(once, 0)

	scopes := func(org string) ([]string, []string) {
		t.Helper()
		decision, err := executor.detectScope(ctx, org, asOf)
		if err != nil {
			t.Fatalf("detectScope(%s): %v", org, err)
		}
		if decision.orgWide {
			t.Fatalf("detectScope(%s) went org-wide: %+v", org, decision)
		}
		repos := append([]string(nil), decision.repoIDs...)
		projects := append([]string(nil), decision.projectKeys...)
		sort.Strings(repos)
		sort.Strings(projects)
		return repos, projects
	}

	onceRepos, onceProjects := scopes(once)
	if len(onceRepos) != 0 || len(onceProjects) != 0 {
		t.Fatalf("rows written once and not changed after the last run flagged repos %v projects %v, want nothing", onceRepos, onceProjects)
	}
	repeatedRepos, repeatedProjects := scopes(repeated)
	if len(repeatedRepos) != 2 || !containsString(repeatedProjects, projectKey) {
		t.Fatalf("control: rows written again after the last run flagged repos %v projects %v, want both repos and the project key", repeatedRepos, repeatedProjects)
	}

	for _, org := range []string{repeated, once} {
		closedAt := changed
		seedWorkItemAttributionRepoOwnershipWithValidity(t, ctx, conn, org, repoClosed, "team-b", changed, first, &closedAt)
		seedWorkItemAttributionRepoOwnershipWithValidity(t, ctx, conn, org, repoOpened, "team-b", changed, changed, nil)
	}
	want := []string{repoClosed.String(), repoOpened.String()}
	sort.Strings(want)
	gotOnce, _ := scopes(once)
	if !reflect.DeepEqual(gotOnce, want) {
		t.Fatalf("after a real change the repos flagged = %v, want the closed and the opened repo %v", gotOnce, want)
	}
	gotRepeated, _ := scopes(repeated)
	for _, id := range want {
		if !containsString(gotRepeated, id) {
			t.Fatalf("a real change was not flagged when the rows were written again: %v lacks %s", gotRepeated, id)
		}
	}
}

func seedWorkItemAttributionProjectOwnershipAt(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, provider, projectID string, projectKey *string, teamID string, validFrom, updatedAt time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_project_ownership (
		org_id, provider, team_id, project_id, project_key, source,
		is_primary, specificity, priority, valid_from, valid_to, updated_at
	)`)
	if err != nil {
		t.Fatalf("prepare team_project_ownership batch: %v", err)
	}
	if err := batch.Append(
		orgID, provider, teamID, projectID, projectKey, "native",
		uint8(1), uint16(100), int32(0), validFrom, nil, updatedAt,
	); err != nil {
		t.Fatalf("append team_project_ownership row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send team_project_ownership batch: %v", err)
	}
}
