//go:build integration

package daily

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

const (
	scopeReadOrgA = "00000000-0000-4000-8000-0000000000a0"
	scopeReadOrgB = "00000000-0000-4000-8000-0000000000b0"
)

var (
	scopeReadDay   = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	scopeReadRepoA = uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	scopeReadRepoB = uuid.MustParse("00000000-0000-4000-8000-0000000000a2")
	scopeReadRepoC = uuid.MustParse("00000000-0000-4000-8000-0000000000b1")
	scopeReadRepoD = uuid.MustParse("00000000-0000-4000-8000-0000000000b2")
)

// scopeReadItem is one stored work item row that was completed on the day. Its
// provider is github when none is given. Its work scope comes from the four
// scope columns by the rule of workItemStateWorkItem.workScopeID.
type scopeReadItem struct {
	org, id, projectID string
	provider           string
	projectKey         string
	projectName        string
	nativeTeamKey      string
	repo               uuid.UUID
	storyPoints        float64
	lastSynced         time.Time
}

func seedScopeReadItems(t *testing.T, ctx context.Context, conn driver.Conn, items ...scopeReadItem) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, project_key, project_name, native_team_key,
    created_at, completed_at, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items: %v", err)
	}
	completedAt := scopeReadDay.Add(10 * time.Hour)
	for _, item := range items {
		lastSynced := item.lastSynced
		if lastSynced.IsZero() {
			lastSynced = scopeReadDay.Add(12 * time.Hour)
		}
		storyPoints := item.storyPoints
		provider := item.provider
		if provider == "" {
			provider = "github"
		}
		if err := batch.Append(
			item.repo, item.id, provider, "story", "done", item.projectID, item.projectKey, item.projectName, item.nativeTeamKey,
			scopeReadDay.Add(-24*time.Hour), &completedAt, &storyPoints, item.org, lastSynced,
		); err != nil {
			t.Fatalf("append work item %s: %v", item.id, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items: %v", err)
	}
}

func seedScopeReadAttribution(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, id, team string) {
	t.Helper()
	if err := conn.Exec(ctx, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, ?, 'github', ?, ?, 'native_team', 1, 'high', 'test', ?)`,
		org, repo, id, team, team, scopeReadDay.Add(12*time.Hour)); err != nil {
		t.Fatalf("seed attribution of %s: %v", id, err)
	}
}

func seedScopeReadTransition(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, id string, at time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `
INSERT INTO work_item_transitions
    (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
VALUES (?, ?, ?, 'github', 'todo', 'in_progress', 'Todo', 'In Progress', '', ?, ?)`,
		repo, id, at, org, scopeReadDay.Add(12*time.Hour)); err != nil {
		t.Fatalf("seed transition of %s: %v", id, err)
	}
}

func scopeReadPartition(repos ...uuid.UUID) Partition {
	partition := Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
	}
	for _, repo := range repos {
		partition.RepoIDs = append(partition.RepoIDs, RepositoryID(repo.String()))
	}
	return partition
}

func readWorkItemScope(t *testing.T, ctx context.Context, conn driver.Conn, org string, repos ...uuid.UUID) workItemScopeRead {
	t.Helper()
	run := Run{OrganizationID: org, TargetDay: scopeReadDay}
	partition := scopeReadPartition(repos...)
	scope, err := newWorkItemPartitionScope(run, partition, "work_item")
	if err != nil {
		t.Fatal(err)
	}
	read, err := loadWorkItemScopeRead(ctx, conn, "work_item", run, partition, scope, true)
	if err != nil {
		t.Fatalf("read the work scopes of %v: %v", repos, err)
	}
	return read
}

func scopeReadItemIDs(read workItemScopeRead) string {
	ids := make([]string, 0, len(read.Items))
	for _, item := range read.Items {
		ids = append(ids, item.WorkItemID)
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// scopeReadDailyTables are the four tables whose key has a work scope and no
// repository.
var scopeReadDailyTables = []string{
	"work_item_metrics_daily", "work_item_user_metrics_daily",
	"work_item_state_durations_daily", "estimate_coverage_metrics_daily",
}

// scopeReadStoredRows is every stored row version of an organization in the
// four tables: the count and the newest version of each table.
func scopeReadStoredRows(t *testing.T, ctx context.Context, conn driver.Conn, org string) string {
	t.Helper()
	var stored []string
	for _, table := range scopeReadDailyTables {
		var (
			count  uint64
			newest string
		)
		if err := conn.QueryRow(ctx,
			`SELECT count(), toString(max(computed_at)) FROM `+table+` WHERE org_id = ?`, org,
		).Scan(&count, &newest); err != nil {
			t.Fatalf("read %s: %v", table, err)
		}
		stored = append(stored, fmt.Sprintf("%s=%d@%s", table, count, newest))
	}
	return strings.Join(stored, " ")
}

func runWorkItemScopeFamilies(t *testing.T, ctx context.Context, conn driver.Conn, org string, repos ...uuid.UUID) {
	t.Helper()
	run := Run{OrganizationID: org, TargetDay: scopeReadDay}
	partition := scopeReadPartition(repos...)
	metrics, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	estimate, err := NewWorkItemEstimateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewWorkItemStateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	for name, family := range map[string]NativeFamilyExecutor{
		"work_item": metrics, "work_item_estimate": estimate, "work_item_state": state,
	} {
		if _, err := family.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("%s of %s: %v", name, org, err)
		}
	}
}

// scopeReadCompleted is the newest row version of a work scope in
// work_item_metrics_daily, summed over its teams.
func scopeReadCompleted(t *testing.T, ctx context.Context, conn driver.Conn, org, scope string) (items uint64, storyPoints float64) {
	t.Helper()
	if err := conn.QueryRow(ctx, `
SELECT toUInt64(sum(items_completed)), toFloat64(sum(story_points_completed))
FROM work_item_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND work_scope_id = ?`, org, scopeReadDay, scope,
	).Scan(&items, &storyPoints); err != nil {
		t.Fatalf("read work_item_metrics_daily: %v", err)
	}
	return items, storyPoints
}

// One read of a partition returns every item of the partition's work scopes,
// from every repository of the organization and from the nil repository, and
// no item of another scope or of another organization. A second organization
// stores the same work scope id and the same work item ids: the read of
// organization A counts none of them, and the three families of organization
// A write no row of organization B.
func TestWorkItemScopeReadCountsItemsOfEveryRepositoryAndNoItemOfAnotherOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	newerThanA := scopeReadDay.Add(18 * time.Hour)
	seedScopeReadItems(t, ctx, conn,
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoA, id: "it-1", projectID: "shared", storyPoints: 1},
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoA, id: "it-2", projectID: "own-a", storyPoints: 1},
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoB, id: "it-3", projectID: "shared", storyPoints: 1},
		// it-4 passes the filter of the query (its project name is a wanted
		// scope id) and is an item of another scope: the scope rule drops it.
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoB, id: "it-4", projectID: "own-b", projectName: "shared", storyPoints: 1},
		scopeReadItem{org: scopeReadOrgA, repo: uuid.Nil, id: "it-5", projectID: "shared", storyPoints: 1},
		// The provider is a part of a work scope: this gitlab item has the
		// scope id of the github scope and is not an item of it.
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoB, id: "it-7", provider: "gitlab", projectID: "shared", storyPoints: 1},
		// Organization B: the same scope id and the same item ids, newer.
		scopeReadItem{org: scopeReadOrgB, repo: scopeReadRepoC, id: "it-1", projectID: "shared", storyPoints: 100, lastSynced: newerThanA},
		scopeReadItem{org: scopeReadOrgB, repo: scopeReadRepoC, id: "it-2", projectID: "shared", storyPoints: 100, lastSynced: newerThanA},
		scopeReadItem{org: scopeReadOrgB, repo: scopeReadRepoC, id: "it-3", projectID: "shared", storyPoints: 100, lastSynced: newerThanA},
		scopeReadItem{org: scopeReadOrgB, repo: scopeReadRepoC, id: "it-9", projectID: "shared", storyPoints: 100, lastSynced: newerThanA},
		// A row of organization B under a repository id of organization A:
		// its scope is not a scope of A's partition.
		scopeReadItem{org: scopeReadOrgB, repo: scopeReadRepoA, id: "it-b-under-a", projectID: "only-b", storyPoints: 100, lastSynced: newerThanA},
	)
	seedScopeReadAttribution(t, ctx, conn, scopeReadOrgA, scopeReadRepoA, "it-1", "team-a")
	seedScopeReadAttribution(t, ctx, conn, scopeReadOrgA, scopeReadRepoB, "it-3", "team-b")
	seedScopeReadAttribution(t, ctx, conn, scopeReadOrgB, scopeReadRepoC, "it-1", "team-x")
	seedScopeReadAttribution(t, ctx, conn, scopeReadOrgB, scopeReadRepoC, "it-3", "team-x")
	seedScopeReadTransition(t, ctx, conn, scopeReadOrgA, scopeReadRepoA, "it-1", scopeReadDay.Add(6*time.Hour))
	seedScopeReadTransition(t, ctx, conn, scopeReadOrgB, scopeReadRepoC, "it-1", scopeReadDay.Add(7*time.Hour))
	seedScopeReadTransition(t, ctx, conn, scopeReadOrgB, scopeReadRepoC, "it-9", scopeReadDay.Add(8*time.Hour))

	for _, test := range []struct {
		name  string
		org   string
		repos []uuid.UUID
		stats workItemScopeReadStats
		items string
	}{
		{"repository A: its own scope and the shared scope", scopeReadOrgA, []uuid.UUID{scopeReadRepoA},
			workItemScopeReadStats{Scopes: 2, ItemsInPartition: 2, ItemsOutsidePartition: 2, FilterValues: 2}, "it-1,it-2,it-3,it-5"},
		{"repositories A and B: only the nil repository is outside", scopeReadOrgA, []uuid.UUID{scopeReadRepoA, scopeReadRepoB},
			workItemScopeReadStats{Scopes: 4, ItemsInPartition: 5, ItemsOutsidePartition: 1, FilterValues: 3}, "it-1,it-2,it-3,it-4,it-5,it-7"},
		{"the nil repository: the shared scope from both repositories", scopeReadOrgA, []uuid.UUID{uuid.Nil},
			workItemScopeReadStats{Scopes: 1, ItemsInPartition: 1, ItemsOutsidePartition: 2, FilterValues: 1}, "it-1,it-3,it-5"},
		{"a scope of one repository reads nothing outside it", scopeReadOrgB, []uuid.UUID{scopeReadRepoC},
			workItemScopeReadStats{Scopes: 1, ItemsInPartition: 4, ItemsOutsidePartition: 0, FilterValues: 1}, "it-1,it-2,it-3,it-9"},
	} {
		t.Run(test.name, func(t *testing.T) {
			read := readWorkItemScope(t, ctx, conn, test.org, test.repos...)
			if read.Stats != test.stats {
				t.Errorf("stats = %+v, want %+v", read.Stats, test.stats)
			}
			if got := scopeReadItemIDs(read); got != test.items {
				t.Errorf("items = %s, want %s", got, test.items)
			}
		})
	}

	read := readWorkItemScope(t, ctx, conn, scopeReadOrgA, scopeReadRepoA)
	for _, item := range read.Items {
		if item.StoryPoints == nil || *item.StoryPoints != 1 {
			t.Errorf("item %s is not the row of organization A (story points %v)", item.WorkItemID, item.StoryPoints)
		}
	}
	if len(read.Transitions) != 1 || !read.Transitions[0].OccurredAt.Equal(scopeReadDay.Add(6*time.Hour)) {
		t.Errorf("transitions = %+v, want the one transition of organization A", read.Transitions)
	}
	if len(read.Attributions) != 2 || read.Attributions["it-1"].TeamID != "team-a" || read.Attributions["it-3"].TeamID != "team-b" {
		t.Errorf("attributions = %+v, want it-1 = team-a and it-3 = team-b", read.Attributions)
	}

	// Organization B is computed first. A run of organization A, of one
	// repository, then writes the shared scope from its three items and
	// leaves every stored row of organization B as it was.
	runWorkItemScopeFamilies(t, ctx, conn, scopeReadOrgB, scopeReadRepoC)
	before := scopeReadStoredRows(t, ctx, conn, scopeReadOrgB)
	for _, table := range scopeReadDailyTables {
		if strings.Contains(before, table+"=0@") {
			t.Fatalf("organization B has no row in %s before the run of A (%s): the check below would prove nothing", table, before)
		}
	}
	time.Sleep(1100 * time.Millisecond) // computed_at of two tables has a resolution of one second
	runWorkItemScopeFamilies(t, ctx, conn, scopeReadOrgA, scopeReadRepoA)
	if after := scopeReadStoredRows(t, ctx, conn, scopeReadOrgB); after != before {
		t.Errorf("the run of organization A changed the rows of organization B:\nbefore %s\nafter  %s", before, after)
	}
	if items, storyPoints := scopeReadCompleted(t, ctx, conn, scopeReadOrgA, "shared"); items != 3 || storyPoints != 3 {
		t.Errorf("organization A, scope shared: items_completed = %d, story points = %v; want 3 and 3", items, storyPoints)
	}
	if items, storyPoints := scopeReadCompleted(t, ctx, conn, scopeReadOrgB, "shared"); items != 4 || storyPoints != 400 {
		t.Errorf("organization B, scope shared: items_completed = %d, story points = %v; want 4 and 400", items, storyPoints)
	}
}

// A work scope id is the value of one of four columns of its item, or empty
// when all four are empty. For each of the five forms, an item of repository A
// and an item of repository B are in one scope, and the read of repository A
// returns both.
func TestWorkItemScopeReadFindsAScopeByEachOfItsColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	forms := []scopeReadItem{
		{id: "project-id", provider: "github", projectID: "by-project-id"},
		{id: "project-name", provider: "gitlab", projectName: "by-project-name"},
		{id: "native-team-key", provider: "linear", nativeTeamKey: "by-native-team-key"},
		// A jira item takes its project key before its project id.
		{id: "project-key", provider: "jira", projectKey: "BYKEY", projectID: "a-jira-project-id"},
		{id: "empty", provider: "custom"},
	}
	var items []scopeReadItem
	for _, form := range forms {
		inA, inB := form, form
		inA.org, inA.repo, inA.id = scopeReadOrgA, scopeReadRepoA, form.id+"-a"
		inB.org, inB.repo, inB.id = scopeReadOrgA, scopeReadRepoB, form.id+"-b"
		if form.provider == "jira" {
			inB.projectID = "another-jira-project-id"
		}
		items = append(items, inA, inB)
	}
	seedScopeReadItems(t, ctx, conn, items...)

	read := readWorkItemScope(t, ctx, conn, scopeReadOrgA, scopeReadRepoA)
	want := workItemScopeReadStats{Scopes: 5, ItemsInPartition: 5, ItemsOutsidePartition: 5, FilterValues: 4}
	if read.Stats != want {
		t.Errorf("stats = %+v, want %+v", read.Stats, want)
	}
	if got, want := scopeReadItemIDs(read),
		"empty-a,empty-b,native-team-key-a,native-team-key-b,project-id-a,project-id-b,project-key-a,project-key-b,project-name-a,project-name-b"; got != want {
		t.Errorf("items = %s\nwant    %s", got, want)
	}
}

// One work item id stored under two repository ids counts once: the row with
// the newest last_synced is the item, with the attribution of that row's
// repository. The result does not depend on which repository the run lists.
func TestWorkItemScopeReadCountsAWorkItemStoredUnderTwoRepositoriesOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	seedScopeReadItems(t, ctx, conn,
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoA, id: "twice", projectID: "shared", storyPoints: 2,
			lastSynced: scopeReadDay.Add(11 * time.Hour)},
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoB, id: "twice", projectID: "shared", storyPoints: 5,
			lastSynced: scopeReadDay.Add(13 * time.Hour)},
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoA, id: "once", projectID: "shared", storyPoints: 1},
	)
	seedScopeReadAttribution(t, ctx, conn, scopeReadOrgA, scopeReadRepoA, "twice", "team-of-the-older-row")
	seedScopeReadAttribution(t, ctx, conn, scopeReadOrgA, scopeReadRepoB, "twice", "team-of-the-newer-row")

	for _, listed := range []uuid.UUID{scopeReadRepoA, scopeReadRepoB} {
		read := readWorkItemScope(t, ctx, conn, scopeReadOrgA, listed)
		if got := scopeReadItemIDs(read); got != "once,twice" || read.Stats.DuplicateItems != 1 {
			t.Fatalf("listed %s: items = %s, duplicates = %d; want once,twice and 1", listed, got, read.Stats.DuplicateItems)
		}
		for _, item := range read.Items {
			if item.WorkItemID == "twice" && (item.StoryPoints == nil || *item.StoryPoints != 5) {
				t.Errorf("listed %s: the item is not its newest row (story points %v, want 5)", listed, item.StoryPoints)
			}
		}
		if team := read.Attributions["twice"].TeamID; team != "team-of-the-newer-row" {
			t.Errorf("listed %s: attribution = %q, want the attribution stored with the newest row", listed, team)
		}
	}

	runWorkItemScopeFamilies(t, ctx, conn, scopeReadOrgA, scopeReadRepoA)
	if items, storyPoints := scopeReadCompleted(t, ctx, conn, scopeReadOrgA, "shared"); items != 2 || storyPoints != 6 {
		t.Errorf("items_completed = %d, story points = %v; want 2 items and 5 + 1 story points", items, storyPoints)
	}
}

// Above a bound the read has no scope filter and returns the same rows, on a
// real server, and it says so in a log line with the count and the bound.
func TestWorkItemScopeReadAboveAFilterBoundReadsTheSameItems(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	// Organization A, the value bound: repository A has one item in each of
	// maxWorkItemScopeFilterValues scopes, repository B one item in one more
	// scope, and the nil repository one item of the first scope.
	items := make([]scopeReadItem, 0, maxWorkItemScopeFilterValues+2)
	for index := 0; index < maxWorkItemScopeFilterValues; index++ {
		scope := fmt.Sprintf("v-%04d", index)
		items = append(items, scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoA, id: "in-" + scope, projectID: scope})
	}
	items = append(items,
		scopeReadItem{org: scopeReadOrgA, repo: scopeReadRepoB, id: "one-more-scope", projectID: "v-one-more"},
		scopeReadItem{org: scopeReadOrgA, repo: uuid.Nil, id: "outside", projectID: "v-0000"},
	)
	// Organization B, the byte bound: 64 scope ids of 1000 bytes are one
	// array literal below querybound.MaxArrayBytes, 66 are above it.
	longScope := func(index int) string { return fmt.Sprintf("b-%02d-", index) + strings.Repeat("y", 994) }
	for index := 0; index < 66; index++ {
		repo := scopeReadRepoC
		if index >= 64 {
			repo = scopeReadRepoD
		}
		items = append(items, scopeReadItem{org: scopeReadOrgB, repo: repo, id: fmt.Sprintf("long-%02d", index), projectID: longScope(index)})
	}
	seedScopeReadItems(t, ctx, conn, items...)
	seedScopeReadTransition(t, ctx, conn, scopeReadOrgA, uuid.Nil, "outside", scopeReadDay.Add(6*time.Hour))

	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	unfilteredLines := func() []string {
		var lines []string
		for _, line := range strings.Split(logged.String(), "\n") {
			if strings.Contains(line, WorkItemScopeReadUnfilteredLogMessage) {
				lines = append(lines, line)
			}
		}
		return lines
	}

	atBound := readWorkItemScope(t, ctx, conn, scopeReadOrgA, scopeReadRepoA)
	wantAtBound := workItemScopeReadStats{
		Scopes: maxWorkItemScopeFilterValues, ItemsInPartition: maxWorkItemScopeFilterValues,
		ItemsOutsidePartition: 1, FilterValues: maxWorkItemScopeFilterValues,
	}
	if atBound.Stats != wantAtBound || len(atBound.Transitions) != 1 {
		t.Fatalf("at the value bound: stats = %+v, transitions = %d; want %+v and 1", atBound.Stats, len(atBound.Transitions), wantAtBound)
	}
	if lines := unfilteredLines(); len(lines) != 0 {
		t.Fatalf("a filtered read logged %q", lines)
	}

	above := readWorkItemScope(t, ctx, conn, scopeReadOrgA, scopeReadRepoA, scopeReadRepoB)
	wantAbove := workItemScopeReadStats{
		Scopes: maxWorkItemScopeFilterValues + 1, ItemsInPartition: maxWorkItemScopeFilterValues + 1,
		ItemsOutsidePartition: 1, Unfiltered: true,
	}
	if above.Stats != wantAbove || len(above.Transitions) != 1 {
		t.Fatalf("above the value bound: stats = %+v, transitions = %d; want %+v and 1", above.Stats, len(above.Transitions), wantAbove)
	}
	if got, want := scopeReadItemIDs(above), scopeReadItemIDs(workItemScopeRead{
		Items: append(append([]workItemMetricsRow{}, atBound.Items...), workItemMetricsRowWithID("one-more-scope")),
	}); got != want {
		t.Fatalf("the unfiltered read does not return the items of the filtered read and the one item more")
	}
	lines := unfilteredLines()
	if len(lines) != 1 || !strings.Contains(lines[0], "level=WARN") ||
		!strings.Contains(lines[0], fmt.Sprintf("scopes=%d", maxWorkItemScopeFilterValues+1)) ||
		!strings.Contains(lines[0], "above_bound="+workItemScopeFilterAboveValues) {
		t.Fatalf("log lines of the read above the value bound = %q; want one warning with the scope count and the bound", lines)
	}

	logged.Reset()
	belowBytes := readWorkItemScope(t, ctx, conn, scopeReadOrgB, scopeReadRepoC)
	if want := (workItemScopeReadStats{Scopes: 64, ItemsInPartition: 64, FilterValues: 64}); belowBytes.Stats != want {
		t.Fatalf("below the byte bound: stats = %+v, want %+v", belowBytes.Stats, want)
	}
	aboveBytes := readWorkItemScope(t, ctx, conn, scopeReadOrgB, scopeReadRepoC, scopeReadRepoD)
	if want := (workItemScopeReadStats{Scopes: 66, ItemsInPartition: 66, Unfiltered: true}); aboveBytes.Stats != want {
		t.Fatalf("above the byte bound: stats = %+v, want %+v", aboveBytes.Stats, want)
	}
	lines = unfilteredLines()
	if len(lines) != 1 || !strings.Contains(lines[0], "scopes=66") ||
		!strings.Contains(lines[0], "above_bound="+workItemScopeFilterAboveBytes) {
		t.Fatalf("log lines of the read above the byte bound = %q; want one warning with the scope count and the bound", lines)
	}
}

func workItemMetricsRowWithID(id string) workItemMetricsRow {
	var row workItemMetricsRow
	row.WorkItemID = id
	return row
}
