//go:build integration

package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The team of a work item is reached through ownership only, in the attribution
// rows and in every daily table that copies the primary row. Through the real
// discoverer and the real partition handler with the six work-item families:
//
//   - O: its project is owned by team-own: team-own;
//   - D: its project is owned by team-donor: team-donor;
//   - L: an unowned project, a relates_to edge to the issue D: unassigned, with a
//     linked_issue row of team-donor kept as is_primary = 0;
//   - M: an unowned project, its assignee matched by a member rule of team-rule:
//     unassigned, with a manual_fallback row of team-rule kept as is_primary = 0.
//
// The test logs every row it reads, so a run before the rule shows the teams
// a computed attribution gave (L team-donor, M team-rule).
func TestAWorkItemTeamIsReachedThroughOwnershipOnlyInEveryDailyTable(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	began := day.AddDate(0, 0, -30)
	created := day.AddDate(0, 0, -2)
	startedAt := day.AddDate(0, 0, -1).Add(12 * time.Hour)
	completedAt := day.Add(10 * time.Hour)
	const provider = "jira"

	items := []struct {
		id, project string
		assignees   []string
	}{
		{"jira:PO-1", "PO", []string{"user-amy"}},
		{"jira:PD-1", "PD", []string{"user-amy"}},
		{"jira:PL-1", "PL", []string{"user-amy"}},
		{"jira:PM-1", "PM", []string{"user-bob"}},
	}
	for _, item := range items {
		assignees := item.assignees
		if err := conn.Exec(ctx, `INSERT INTO work_items (
			repo_id, work_item_id, provider, title, type, status, project_id, project_key, assignees,
			created_at, started_at, completed_at, org_id, last_synced)
			VALUES (?, ?, ?, ?, 'story', 'done', ?, ?, ?, ?, ?, ?, ?, ?)`,
			uuid.Nil, item.id, provider, "Item "+item.id, item.project, item.project, assignees,
			created, startedAt, completedAt, orgID, created); err != nil {
			t.Fatalf("seed work item %s: %v", item.id, err)
		}
		for _, transition := range []struct {
			at       time.Time
			from, to string
		}{{startedAt, "todo", "in_progress"}, {completedAt, "in_progress", "done"}} {
			if err := conn.Exec(ctx, `INSERT INTO work_item_transitions (
				repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw,
				actor, last_synced, org_id) VALUES (?, ?, ?, '', ?, ?, ?, ?, 'a', ?, ?)`,
				uuid.Nil, item.id, transition.at, transition.from, transition.to, transition.from, transition.to,
				transition.at, orgID); err != nil {
				t.Fatalf("seed transition of %s: %v", item.id, err)
			}
		}
	}
	seedItemDayOwnershipOf(t, ctx, conn, "project", orgID, provider, "PO", "team-own", began, nil, began)
	seedItemDayOwnershipOf(t, ctx, conn, "project", orgID, provider, "PD", "team-donor", began, nil, began)
	seedWorkItemAttributionLinkedDependency(t, ctx, conn, orgID, "jira:PL-1", "jira:PD-1", "relates_to", created)
	if err := conn.Exec(ctx, `INSERT INTO manual_attribution_fallbacks (
		org_id, provider, scope_type, scope_id, team_id, team_name, reason, priority, valid_from, valid_to, updated_at)
		VALUES (?, ?, 'member', 'user-bob', 'team-rule', 'Rule Team', 'cell', 1, ?, NULL, ?)`,
		orgID, provider, began, began); err != nil {
		t.Fatalf("seed member rule: %v", err)
	}

	discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	discovered, err := discoverer.RepositoryIDs(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	runAt := day.AddDate(0, 0, 1).Add(2 * time.Hour)
	attribution, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	attribution.nowUTC = func() time.Time { return runAt }
	state, err := NewWorkItemStateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	state.nowUTC = func() time.Time { return runAt }
	workItem, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	workItem.nowUTC = func() time.Time { return runAt }
	estimate, err := NewWorkItemEstimateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	issueType, err := NewWorkItemIssueTypeExecutor(conn, workItemEngineTestNormalizer{})
	if err != nil {
		t.Fatal(err)
	}
	issueType.nowUTC = func() time.Time { return runAt }
	investment, err := NewWorkItemInvestmentExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	investment.nowUTC = func() time.Time { return runAt }

	runID, partitionID := uuid.NewString(), uuid.NewString()
	store := &fakeStore{
		partitionClaim: &PartitionClaim{
			Partition: Partition{ID: partitionID, RunID: runID, RepoIDs: discovered},
			Token:     uuid.NewString(), LeaseDuration: time.Minute,
		},
		run: Run{ID: runID, OrganizationID: orgID, Status: "running", TargetDay: day},
	}
	handler, err := NewPartitionHandler(store, fakePublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.SetNativeFamilies(map[string]NativeFamilyExecutor{
		"work_item_attribution": attribution, "work_item_state": state, "work_item": workItem,
		"work_item_estimate": estimate, WorkItemIssueTypeFamilyName: issueType, WorkItemInvestmentFamilyName: investment,
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(ctx, partitionExecutionFor(partitionID, runID, orgID)); err != nil {
		t.Fatalf("daily partition: %v", err)
	}
	if store.partitionCompletions != 1 {
		t.Fatalf("partition completions = %d, want 1", store.partitionCompletions)
	}

	attributionRows := discovererMatrixLines(t, ctx, conn, `
SELECT concat(work_item_id, '|', source, '|', ifNull(team_id, ''), '|', toString(is_primary))
FROM work_item_team_attributions FINAL WHERE org_id = ?`, orgID)
	scoped := map[string][]string{}
	for table, query := range map[string]string{
		"work_item_metrics_daily": `SELECT concat(work_scope_id, '|', team_id, '|completed=', toString(argMax(items_completed, computed_at)))
FROM work_item_metrics_daily WHERE org_id = ? AND day = ? AND row_view IN ('', 'org') GROUP BY work_scope_id, team_id`,
		"work_item_cycle_times": `SELECT concat(work_item_id, '|', ifNull(team_id, ''))
FROM work_item_cycle_times FINAL WHERE org_id = ? AND day = ?`,
		"work_item_user_metrics_daily": `SELECT concat(work_scope_id, '|', team_id, '|completed=', toString(argMax(items_completed, computed_at)))
FROM work_item_user_metrics_daily WHERE org_id = ? AND day = ? AND row_view IN ('', 'org') GROUP BY work_scope_id, team_id`,
		"work_item_state_durations_daily": `SELECT DISTINCT concat(work_scope_id, '|', team_id)
FROM work_item_state_durations_daily FINAL WHERE org_id = ? AND day = ? AND row_view IN ('', 'org')`,
		"issue_type_metrics_daily": `SELECT concat(team_id, '|completed=', toString(argMax(completed_count, computed_at)))
FROM issue_type_metrics_daily WHERE org_id = ? AND day = ? GROUP BY repo_id, provider, team_id, issue_type_norm`,
		"work_item_completion_daily": `SELECT concat(team_id, '|completed=', toString(argMax(work_items_completed, computed_at)))
FROM work_item_completion_daily WHERE org_id = ? AND day = ? GROUP BY repo_id, team_id`,
	} {
		scoped[table] = discovererMatrixLines(t, ctx, conn, query, orgID, day)
	}
	dump := "\nwork_item_team_attributions:\n  " + strings.Join(attributionRows, "\n  ")
	for _, table := range []string{"work_item_metrics_daily", "work_item_cycle_times", "work_item_user_metrics_daily",
		"work_item_state_durations_daily", "issue_type_metrics_daily", "work_item_completion_daily"} {
		dump += "\n" + table + ":\n  " + strings.Join(scoped[table], "\n  ")
	}
	t.Log(dump)

	holds := func(lines []string, want string) bool {
		for _, line := range lines {
			if line == want {
				return true
			}
		}
		return false
	}
	for _, want := range []string{
		"jira:PO-1|project_ownership|team-own|1",
		"jira:PD-1|project_ownership|team-donor|1",
		"jira:PL-1|unassigned||1",
		"jira:PL-1|linked_issue|team-donor|0",
		"jira:PM-1|unassigned||1",
		"jira:PM-1|manual_fallback|team-rule|0",
	} {
		if !holds(attributionRows, want) {
			t.Errorf("work_item_team_attributions: no row %s%s", want, dump)
		}
	}
	scopeTeams := map[string]string{"PO": "team-own", "PD": "team-donor", "PL": "unassigned", "PM": "unassigned"}
	for _, table := range []string{"work_item_metrics_daily", "work_item_user_metrics_daily", "work_item_state_durations_daily"} {
		for scope, team := range scopeTeams {
			found := false
			for _, line := range scoped[table] {
				fields := strings.Split(line, "|")
				if !strings.Contains(fields[0], scope) {
					continue
				}
				found = true
				if fields[1] != team {
					t.Errorf("%s: scope %s has team %q, want %q%s", table, scope, fields[1], team, dump)
				}
			}
			if !found {
				t.Errorf("%s: no row of scope %s%s", table, scope, dump)
			}
		}
	}
	for item, team := range map[string]string{"jira:PO-1": "team-own", "jira:PD-1": "team-donor", "jira:PL-1": "unassigned", "jira:PM-1": "unassigned"} {
		if want := item + "|" + team; !holds(scoped["work_item_cycle_times"], want) {
			t.Errorf("work_item_cycle_times: no row %s%s", want, dump)
		}
	}
	// work_item_completion_daily stores no team as '' (workitemengine/completion.go).
	unassignedKey := map[string]string{"issue_type_metrics_daily": "unassigned", "work_item_completion_daily": ""}
	for _, table := range []string{"issue_type_metrics_daily", "work_item_completion_daily"} {
		for _, want := range []string{"team-own|completed=1", "team-donor|completed=1", unassignedKey[table] + "|completed=2"} {
			if !holds(scoped[table], want) {
				t.Errorf("%s: no row %s%s", table, want, dump)
			}
		}
		for _, line := range scoped[table] {
			if strings.HasPrefix(line, "team-rule|") {
				t.Errorf("%s: a row of team-rule %s: a member rule gave a team%s", table, line, dump)
			}
		}
	}
}
