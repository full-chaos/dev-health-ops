//go:build integration

package providersync

// CHAOS-8810 on a real ClickHouse at the migration head (this file authors no
// DDL: newWorkItemEffectsConn applies the production migration chain).
//
// The owner's design: a work-items sync writes raw rows, and the daily job
// computes every derived table from STORED rows. This file is the differential
// proof that the daily families compute, from stored rows, what the sync-time
// deriver computes from the same full item set -- for a stored-row fixture of
// each of the four providers.
//
// What each side is:
//
//   - THE SYNC-TIME DERIVER: GitHubWorkItemDeriver.deriveForProvider, built by
//     its production constructor with the two real config artifacts and run
//     under each provider's name. It is the day loop of the GitHub deriver;
//     the Linear, GitLab and Jira derivers have their own copy of that loop
//     around the SAME three per-provider builders
//     (buildWorkItemMetricTripletForProvider,
//     buildWorkItemDerivedSurfacesForProvider, the engine's
//     deriveRowsForProvider), which is what this file runs for them. Its rows
//     are stored through the derived effect adapters every provider's derived
//     sink wraps.
//   - THE DAILY FAMILIES: the six work-item executors of
//     internal/jobs/metrics/daily, each built by its production constructor
//     and run in the order families.json gives, over one partition.
//
// What this file is NOT: a run of each provider's fetch and normalize route.
// The raw rows of all four cases are written through the shared raw work-item
// adapters under each provider's identity. The end-to-end route harness
// (provider answer -> route -> sinks -> families) exists for GitHub only:
// github_work_items_window_equivalence_integration_test.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// dailyFamilyMatrixDay is the day both writers compute.
var dailyFamilyMatrixDay = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

// dailyFamilyMatrixCase is the stored-row shape of one provider.
type dailyFamilyMatrixCase struct {
	provider string
	orgID    string
	// repoID is the repository of a repository-scoped provider's items. nil:
	// the provider's items have no repository and are stored under the nil
	// repository id, which is the partition the daily job computes them in.
	repoID *uuid.UUID
	// itemID builds the provider's work item id from a number.
	itemID func(number int) string
	// itemType is the provider's raw type of each fixture item, by label.
	itemType map[string]string
	// projectKey / nativeTeamKey are the scope fields the provider fills.
	projectKey    *string
	projectID     *string
	nativeTeamKey *string
}

func dailyFamilyMatrixCases() []dailyFamilyMatrixCase {
	githubRepo := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	gitlabRepo := uuid.MustParse("5b2f6f0e-8f4b-4d0a-9d0f-3f1c2b7a6e11")
	return []dailyFamilyMatrixCase{
		{
			provider: "github", orgID: "81111111-1111-4111-8111-111111111111", repoID: &githubRepo,
			itemID:    func(number int) string { return fmt.Sprintf("gh:acme/api#%d", number) },
			itemType:  map[string]string{"bug": "issue", "feature": "issue", "security": "issue", "chore": "issue", "documentation": "issue", "incident": "issue"},
			projectID: stringPointer("acme/api"),
		},
		{
			provider: "gitlab", orgID: "82222222-2222-4222-8222-222222222222", repoID: &gitlabRepo,
			itemID:    func(number int) string { return fmt.Sprintf("gitlab:acme/api#%d", number) },
			itemType:  map[string]string{"bug": "incident", "feature": "issue", "security": "issue", "chore": "task", "documentation": "issue", "incident": "issue"},
			projectID: stringPointer("123"),
		},
		{
			provider: "jira", orgID: "83333333-3333-4333-8333-333333333333",
			itemID:     func(number int) string { return fmt.Sprintf("jira:OPS-%d", number) },
			itemType:   map[string]string{"bug": "Bug", "feature": "Story", "security": "Task", "chore": "Task", "documentation": "Sub-task", "incident": "Epic"},
			projectKey: stringPointer("OPS"), projectID: stringPointer("10001"),
		},
		{
			provider: "linear", orgID: "84444444-4444-4444-8444-444444444444",
			itemID:        func(number int) string { return fmt.Sprintf("linear:OPS-%d", number) },
			itemType:      map[string]string{"bug": "bug", "feature": "feature", "security": "task", "chore": "task", "documentation": "task", "incident": "task"},
			nativeTeamKey: stringPointer("OPS"), projectID: stringPointer("project-platform"),
		},
	}
}

// dailyFamilyMatrixRows is the FULL item set of one provider case: seven items
// whose creation, start, completion and status changes put something in every
// derived table of the day.
//
//	1  bug       created day-10, in_progress at day-3, done at day 10:00, 3 points
//	2  feature   created day 08:00, in_progress at day 09:00, open
//	3  security  created day-5, in_progress at day-1, done at day 15:00
//	4  chore     created day-20, done at day-2 (not active on the day)
//	5  documentation  created day-2, open, no status change
//	6  feature   created day-1, in_progress at day 03:00, done at day 21:00, 5 points
//	7  incident  created day-30, done at day-10: not active on the day. Where
//	             the status mapping gives it a type of its own (github, gitlab,
//	             jira), that key still has a row in issue_type_metrics_daily,
//	             all zeros.
func dailyFamilyMatrixRows(testCase dailyFamilyMatrixCase, normalizedAt time.Time) githubWorkItemRows {
	day := dailyFamilyMatrixDay
	at := func(days int, hours float64) time.Time {
		return day.AddDate(0, 0, days).Add(time.Duration(hours * float64(time.Hour)))
	}
	timePtr := func(value time.Time) *time.Time { return &value }
	points := func(value float64) *float64 { return &value }
	type spec struct {
		number      int
		label       string
		status      string
		created     time.Time
		started     *time.Time
		completed   *time.Time
		storyPoints *float64
		assignee    string
	}
	specs := []spec{
		{1, "bug", "done", at(-10, 0), timePtr(at(-3, 9)), timePtr(at(0, 10)), points(3), "ada"},
		{2, "feature", "in_progress", at(0, 8), timePtr(at(0, 9)), nil, nil, "grace"},
		{3, "security", "done", at(-5, 0), timePtr(at(-1, 12)), timePtr(at(0, 15)), nil, "ada"},
		{4, "chore", "done", at(-20, 0), timePtr(at(-4, 0)), timePtr(at(-2, 0)), nil, "linus"},
		{5, "documentation", "todo", at(-2, 0), nil, nil, nil, ""},
		{6, "feature", "done", at(-1, 0), timePtr(at(0, 3)), timePtr(at(0, 21)), points(5), "grace"},
		{7, "incident", "done", at(-30, 0), timePtr(at(-25, 0)), timePtr(at(-10, 0)), nil, "linus"},
	}
	rows := githubWorkItemRows{}
	for _, item := range specs {
		assignees := []string{}
		if item.assignee != "" {
			assignees = []string{item.assignee}
		}
		row := githubWorkItemRow{
			WorkItemID: testCase.itemID(item.number), Provider: testCase.provider,
			Title: "Item " + item.label, Type: testCase.itemType[item.label],
			Status: item.status, StatusRaw: stringPointer(item.status),
			RepoID: testCase.repoID, NativeTeamKey: testCase.nativeTeamKey,
			ProjectKey: testCase.projectKey, ProjectID: testCase.projectID,
			Assignees: assignees, Reporter: stringPointer("reporter"),
			CreatedAt: item.created, UpdatedAt: normalizedAt.Add(-time.Hour),
			StartedAt: item.started, CompletedAt: item.completed, ClosedAt: item.completed,
			Labels: []string{item.label}, StoryPoints: item.storyPoints,
			OrgID: testCase.orgID, LastSynced: normalizedAt,
		}
		rows.WorkItems = append(rows.WorkItems, row)
		transition := func(occurredAt time.Time, from, to string) {
			rows.StatusTransitions = append(rows.StatusTransitions, githubWorkItemTransitionRow{
				WorkItemID: row.WorkItemID, Provider: testCase.provider, OccurredAt: occurredAt,
				FromStatusRaw: stringPointer(from), ToStatusRaw: stringPointer(to),
				FromStatus: from, ToStatus: to, Actor: stringPointer("actor"),
				OrgID: testCase.orgID, LastSynced: normalizedAt,
			})
		}
		if item.started != nil {
			transition(*item.started, "todo", "in_progress")
		}
		if item.completed != nil {
			transition(*item.completed, "in_progress", "done")
		}
	}
	return rows
}

func dailyFamilyMatrixClaim(t *testing.T, testCase dailyFamilyMatrixCase) Claim {
	t.Helper()
	claim := nativeTestClaim(testCase.provider, "work-items")
	claim.OrgID = testCase.orgID
	since, before := dailyFamilyMatrixDay, dailyFamilyMatrixDay.AddDate(0, 0, 1)
	claim.SinceAt, claim.BeforeAt = &since, &before
	if err := claim.Validate(); err != nil {
		t.Fatal(err)
	}
	return claim
}

// dailyFamilyMatrixTables are the derived tables both writers write and this
// file compares, each with the columns that are NOT compared and why. Every
// other column of every table is compared: the read is `SELECT *`.
var dailyFamilyMatrixTables = []struct {
	table    string
	excluded map[string]string
}{
	{"issue_type_metrics_daily", map[string]string{"computed_at": "each writer stamps the time of its own run"}},
	// investment_classifications_daily and investment_metrics_daily are no longer written by the daily family
	// (CHAOS-9264): the daily family writes work_item_completion_daily, which the sync deriver does not write yet.
	{"work_item_state_durations_daily", map[string]string{"computed_at": "each writer stamps the time of its own run"}},
	{"work_item_metrics_daily", map[string]string{"computed_at": "each writer stamps the time of its own run"}},
	{"work_item_user_metrics_daily", map[string]string{"computed_at": "each writer stamps the time of its own run"}},
	{"work_item_cycle_times", map[string]string{"computed_at": "each writer stamps the time of its own run"}},
	{"estimate_coverage_metrics_daily", map[string]string{"computed_at": "each writer stamps the time of its own run"}},
}

// readDailyFamilyMatrixRows returns the rows of one table and organization that
// ONE writer wrote, selected by that writer's computed_at: the sync-time
// deriver stamps the unit's normalizedAt, the daily families stamp the wall
// clock of the test. Selecting by the stamp (and not truncating between the
// writers) keeps both writers' rows in the store, as in production, and still
// shows a key that only one of them wrote.
func readDailyFamilyMatrixRows(
	ctx context.Context, t *testing.T, conn driver.Conn, table, orgID string,
	excluded map[string]string, stampCondition string, stampArgument time.Time,
) []string {
	t.Helper()
	rows, err := conn.Query(ctx,
		"SELECT formatRowNoNewline('JSONEachRow', *) FROM "+table+
			" WHERE org_id = ? AND computed_at "+stampCondition+" ?", orgID, stampArgument)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer rows.Close()
	result := []string{}
	matched := map[string]bool{}
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			t.Fatal(err)
		}
		var row map[string]json.RawMessage
		if err := json.Unmarshal([]byte(encoded), &row); err != nil {
			t.Fatalf("%s row %s: %v", table, encoded, err)
		}
		for column := range excluded {
			if _, exists := row[column]; exists {
				matched[column] = true
				delete(row, column)
			}
		}
		canonical, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, string(canonical))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(result) > 0 {
		for column := range excluded {
			if !matched[column] {
				t.Fatalf("%s: the excluded column %q matched no column", table, column)
			}
		}
	}
	sort.Strings(result)
	return result
}

// writeDailyFamilyMatrixEffect stores rows through one of the shared work-item
// effect adapters, under the provider identity of the case.
func writeDailyFamilyMatrixEffect(
	ctx context.Context, t *testing.T, claim Claim, destination string, rows []json.RawMessage,
	adapter GitHubWorkItemEffectAdapter,
) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	effect, err := BuildEffectBatch(destination, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: "work-items",
		Generation: claim.GenerationKey(), Destination: destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatalf("%s: write %s: %v", claim.Provider, destination, err)
	}
}

func marshalDailyFamilyMatrixRows[Row any](t *testing.T, rows []Row) []json.RawMessage {
	t.Helper()
	encoded := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, raw)
	}
	return encoded
}

// runDailyWorkItemFamilies runs the six work-item daily families for one
// organization and day over the given repositories, each built by its
// production constructor, in the order the partition handler derives from
// families.json. It returns the rows each family reported.
func runDailyWorkItemFamilies(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, day time.Time,
	repoIDs []daily.RepositoryID,
) map[string]int {
	t.Helper()
	statusMapping, err := LoadStatusMapping(resolveStatusMappingConfig(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	families := map[string]daily.NativeFamilyExecutor{}
	build := func(name string, executor daily.NativeFamilyExecutor, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("daily family %s: %v", name, err)
		}
		families[name] = executor
	}
	attribution, err := daily.NewWorkItemAttributionExecutor(conn)
	build("work_item_attribution", attribution, err)
	workItem, err := daily.NewWorkItemExecutor(conn)
	build("work_item", workItem, err)
	estimate, err := daily.NewWorkItemEstimateExecutor(conn)
	build("work_item_estimate", estimate, err)
	state, err := daily.NewWorkItemStateExecutor(conn)
	build("work_item_state", state, err)
	issueType, err := daily.NewWorkItemIssueTypeExecutor(conn, statusMapping)
	build(daily.WorkItemIssueTypeFamilyName, issueType, err)
	investment, err := daily.NewWorkItemInvestmentExecutor(conn)
	build(daily.WorkItemInvestmentFamilyName, investment, err)

	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	registry, err := daily.LoadRegistry()
	if err != nil {
		t.Fatal(err)
	}
	order, err := daily.FamilyRunOrder(registry, names)
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != len(families) {
		t.Fatalf("run order %v does not hold the %d families", order, len(families))
	}
	written := map[string]int{}
	for _, name := range order {
		count, err := families[name].ComputeFamily(ctx,
			daily.Run{ID: "00000000-0000-4000-8000-0000000000d0", OrganizationID: orgID, TargetDay: day},
			daily.Partition{
				ID: "00000000-0000-4000-8000-0000000000d1", RunID: "00000000-0000-4000-8000-0000000000d0",
				RepoIDs: repoIDs,
			})
		if err != nil {
			t.Fatalf("daily family %s: %v", name, err)
		}
		written[name] = count
	}
	return written
}
