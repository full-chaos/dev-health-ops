//go:build integration

package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// An issue with no team of its own takes the owners of the repositories of its
// linked pull requests, read from work_graph_issue_pr at the issue's best link
// tier only, through the real discoverer and the real partition handler. R1 is
// owned by team-a, R2 by team-b, R3 by no team.
//
//   - PI: one native link to R1: team-a;
//   - PM: native links to R1 (2 pull requests) and R2 (1): team-a is the main
//     owner (most links), team-b a co-owner;
//   - PT: one native link each to R1 and R2: a tie, the lowest team id team-a
//     is the main owner, team-b a co-owner;
//   - PB: a native link to R3 and a heuristic link to R1: the best tier names
//     no owner, so unassigned;
//   - PF: a project fallback rule of team-rule and a native link to R2: team-b
//     (an owner wins over a fallback rule);
//   - PO: its project owned by team-own and a native link to R1: team-own, the
//     repository owner kept as a provenance row;
//   - PU: no link: unassigned.
func TestAnIssueWithNoTeamTakesItsLinkedPullRequestRepoOwner(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	began := day.AddDate(0, 0, -30)
	created := day.AddDate(0, 0, -2)
	startedAt := day.AddDate(0, 0, -1).Add(12 * time.Hour)
	completedAt := day.Add(10 * time.Hour)
	const provider = "jira"
	r1, r2, r3 := uuid.New(), uuid.New(), uuid.New()

	projects := []string{"PI", "PM", "PT", "PB", "PF", "PO", "PU"}
	for _, project := range projects {
		id := "jira:" + project + "-1"
		if err := conn.Exec(ctx, `INSERT INTO work_items (
			repo_id, work_item_id, provider, title, type, status, project_id, project_key, assignees,
			created_at, started_at, completed_at, org_id, last_synced)
			VALUES (?, ?, ?, ?, 'story', 'done', ?, ?, ?, ?, ?, ?, ?, ?)`,
			uuid.Nil, id, provider, "Item "+id, project, project, []string{},
			created, startedAt, completedAt, orgID, created); err != nil {
			t.Fatalf("seed work item %s: %v", id, err)
		}
		for _, transition := range []struct {
			at       time.Time
			from, to string
		}{{startedAt, "todo", "in_progress"}, {completedAt, "in_progress", "done"}} {
			if err := conn.Exec(ctx, `INSERT INTO work_item_transitions (
				repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw,
				actor, last_synced, org_id) VALUES (?, ?, ?, '', ?, ?, ?, ?, 'a', ?, ?)`,
				uuid.Nil, id, transition.at, transition.from, transition.to, transition.from, transition.to,
				transition.at, orgID); err != nil {
				t.Fatalf("seed transition of %s: %v", id, err)
			}
		}
	}
	seedItemDayOwnershipOf(t, ctx, conn, "repository", orgID, "github", r1.String(), "team-a", began, nil, began)
	seedItemDayOwnershipOf(t, ctx, conn, "repository", orgID, "github", r2.String(), "team-b", began, nil, began)
	seedItemDayOwnershipOf(t, ctx, conn, "project", orgID, provider, "PO", "team-own", began, nil, began)
	if err := conn.Exec(ctx, `INSERT INTO manual_attribution_fallbacks (
		org_id, provider, scope_type, scope_id, team_id, team_name, reason, priority, valid_from, valid_to, updated_at)
		VALUES (?, ?, 'project', 'PF', 'team-rule', 'Rule Team', 'cell', 1, ?, NULL, ?)`,
		orgID, provider, began, began); err != nil {
		t.Fatalf("seed project rule: %v", err)
	}
	for _, link := range []struct {
		repo       uuid.UUID
		item, tier string
		pr         uint32
	}{
		{r1, "jira:PI-1", "native", 1},
		{r1, "jira:PM-1", "native", 2}, {r1, "jira:PM-1", "native", 3}, {r2, "jira:PM-1", "native", 4},
		{r1, "jira:PT-1", "native", 5}, {r2, "jira:PT-1", "native", 6},
		{r3, "jira:PB-1", "native", 7}, {r1, "jira:PB-1", "heuristic", 8},
		{r2, "jira:PF-1", "native", 9},
		{r1, "jira:PO-1", "native", 10},
	} {
		if err := conn.Exec(ctx, `INSERT INTO work_graph_issue_pr (
			repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
			VALUES (?, ?, ?, 1.0, ?, 'cell', ?, ?)`, link.repo, link.item, link.pr, link.tier, created, orgID); err != nil {
			t.Fatalf("seed work_graph_issue_pr: %v", err)
		}
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
	}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(ctx, partitionExecutionFor(partitionID, runID, orgID)); err != nil {
		t.Fatalf("daily partition: %v", err)
	}

	attributionRows := discovererMatrixLines(t, ctx, conn, `
SELECT concat(work_item_id, '|', source, '|', ifNull(team_id, ''), '|', toString(is_primary))
FROM work_item_team_attributions FINAL WHERE org_id = ?`, orgID)
	evidence := discovererMatrixLines(t, ctx, conn, `
SELECT concat(work_item_id, '|', ifNull(team_id, ''), '|', evidence)
FROM work_item_team_attributions FINAL WHERE org_id = ? AND source = 'repo_ownership'`, orgID)
	cycle := discovererMatrixLines(t, ctx, conn, `SELECT concat(work_item_id, '|', ifNull(team_id, ''))
FROM work_item_cycle_times FINAL WHERE org_id = ? AND day = ?`, orgID, day)
	metrics := discovererMatrixLines(t, ctx, conn, `SELECT DISTINCT concat(work_scope_id, '|', team_id)
FROM work_item_metrics_daily WHERE org_id = ? AND day = ? AND row_view IN ('', 'org')`, orgID, day)
	dump := "\nwork_item_team_attributions:\n  " + strings.Join(attributionRows, "\n  ") +
		"\nrepo_ownership evidence:\n  " + strings.Join(evidence, "\n  ") +
		"\nwork_item_cycle_times:\n  " + strings.Join(cycle, "\n  ") +
		"\nwork_item_metrics_daily:\n  " + strings.Join(metrics, "\n  ")
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
		"jira:PI-1|repo_ownership|team-a|1",
		"jira:PM-1|repo_ownership|team-a|1", "jira:PM-1|repo_ownership|team-b|2",
		"jira:PT-1|repo_ownership|team-a|1", "jira:PT-1|repo_ownership|team-b|2",
		"jira:PB-1|unassigned||1",
		"jira:PF-1|repo_ownership|team-b|1", "jira:PF-1|manual_fallback|team-rule|0",
		"jira:PO-1|project_ownership|team-own|1", "jira:PO-1|repo_ownership|team-a|0",
		"jira:PU-1|unassigned||1",
	} {
		if !holds(attributionRows, want) {
			t.Errorf("work_item_team_attributions: no row %s%s", want, dump)
		}
	}
	for _, line := range attributionRows {
		if strings.HasPrefix(line, "jira:PB-1|repo_ownership|") {
			t.Errorf("a lower-tier link gave a row: %s%s", line, dump)
		}
	}
	for _, want := range []string{
		"jira:PI-1|team-a|repo_ownership=" + r1.String() + ";work_graph_issue_pr=1;tier=native",
		"jira:PM-1|team-a|repo_ownership=" + r1.String() + ";work_graph_issue_pr=2;tier=native",
		"jira:PM-1|team-b|repo_ownership=" + r2.String() + ";work_graph_issue_pr=4;tier=native",
	} {
		if !holds(evidence, want) {
			t.Errorf("repo_ownership evidence: no row %s%s", want, dump)
		}
	}
	for item, team := range map[string]string{
		"jira:PI-1": "team-a", "jira:PM-1": "team-a", "jira:PT-1": "team-a", "jira:PB-1": "unassigned",
		"jira:PF-1": "team-b", "jira:PO-1": "team-own", "jira:PU-1": "unassigned",
	} {
		if want := item + "|" + team; !holds(cycle, want) {
			t.Errorf("work_item_cycle_times: no row %s%s", want, dump)
		}
	}
	for scope, team := range map[string]string{"PI": "team-a", "PM": "team-a", "PB": "unassigned", "PF": "team-b"} {
		found := false
		for _, line := range metrics {
			fields := strings.Split(line, "|")
			if strings.Contains(fields[0], scope) {
				found = true
				if fields[1] != team {
					t.Errorf("work_item_metrics_daily: scope %s team %q, want %q%s", scope, fields[1], team, dump)
				}
			}
		}
		if !found {
			t.Errorf("work_item_metrics_daily: no row of scope %s%s", scope, dump)
		}
	}
}
