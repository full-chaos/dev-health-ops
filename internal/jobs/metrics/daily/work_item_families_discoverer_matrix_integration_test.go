//go:build integration

package daily

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The state the daily job exists to reach, for every provider, through the
// path production takes: the ClickHouse discoverer gives the partition
// members, the partition handler runs the six work-item families in the
// families.json order, on a ClickHouse built from the migration chain.
//
// github and gitlab store a work item under its repository. jira and linear
// store it under the nil repository id, which no repos row has: the
// discoverer must return that id, or no family reads those items. Each
// provider first gets the rows an hourly sync unit leaves behind (computed
// from ONE fetched item), and the daily job must make the full recompute the
// newest row of every key again.

func discovererMatrixLines(
	t *testing.T, ctx context.Context, conn driver.Conn, query string, arguments ...any,
) []string {
	t.Helper()
	rows, err := conn.Query(ctx, query, arguments...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(lines)
	return lines
}

func TestDailyJobThroughTheDiscovererRestoresTheDerivedRowsOfEveryProvider(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	timePtr := func(value time.Time) *time.Time { return &value }
	points := 3.0

	repositories := map[string]uuid.UUID{
		"github": uuid.New(), "gitlab": uuid.New(), "jira": uuid.Nil, "linear": uuid.Nil,
	}
	providers := []string{"github", "gitlab", "jira", "linear"}
	for _, provider := range []string{"github", "gitlab"} {
		if err := conn.Exec(ctx,
			`INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
			repositories[provider], orgID, "acme/"+provider, provider, day, day); err != nil {
			t.Fatal(err)
		}
	}
	seedItem := func(repoID uuid.UUID, workItemID, provider string, createdAt time.Time, startedAt, completedAt *time.Time, storyPoints *float64) {
		t.Helper()
		status := "in_progress"
		if completedAt != nil {
			status = "done"
		}
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
			repo_id, work_item_id, provider, title, type, status, project_key, created_at, started_at, completed_at,
			labels, story_points, org_id, last_synced)`)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(repoID, workItemID, provider, "Item "+workItemID, "issue", status, "PK",
			createdAt, startedAt, completedAt, []string{"quality"}, storyPoints, orgID, createdAt); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}
	// A transition as the sync sink stores it: nil repository id, no provider.
	seedTransition := func(workItemID string, at time.Time, from, to string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_item_transitions (
			repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw,
			actor, last_synced, org_id) VALUES (?, ?, ?, '', ?, ?, ?, ?, 'a', ?, ?)`,
			uuid.Nil, workItemID, at, from, to, from, to, at, orgID); err != nil {
			t.Fatal(err)
		}
	}
	unitAt := day.AddDate(0, 0, 1).Add(time.Hour)
	for _, provider := range providers {
		repoID := repositories[provider]
		// Two items completed on the day (one with 3 story points), one item
		// created on the day and open.
		seedItem(repoID, provider+":1", provider, day.AddDate(0, 0, -2),
			timePtr(day.AddDate(0, 0, -1).Add(12*time.Hour)), timePtr(day.Add(10*time.Hour)), &points)
		seedItem(repoID, provider+":2", provider, day.AddDate(0, 0, -2), nil, timePtr(day.Add(11*time.Hour)), nil)
		seedItem(repoID, provider+":3", provider, day.Add(8*time.Hour), nil, nil, nil)
		seedTransition(provider+":1", day.AddDate(0, 0, -1).Add(12*time.Hour), "todo", "in_progress")
		seedTransition(provider+":1", day.Add(10*time.Hour), "in_progress", "done")

		// What an hourly unit that fetched ONE completed item appends.
		var stored any
		if repoID != uuid.Nil {
			stored = repoID
		}
		if err := conn.Exec(ctx, `INSERT INTO issue_type_metrics_daily (
			repo_id, day, provider, team_id, issue_type_norm, created_count, completed_count, active_count,
			cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at, org_id)
			VALUES (?, ?, ?, 'unassigned', 'quality', 0, 1, 1, 0, 0, 0, ?, ?)`,
			stored, day, provider, unitAt, orgID); err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, `INSERT INTO investment_metrics_daily (
			repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed,
			prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
			VALUES (?, ?, '', 'quality', 'general', 3, 1, 0, 0, 0, ?, ?)`,
			stored, day, unitAt, orgID); err != nil {
			t.Fatal(err)
		}
	}

	// --- the real discoverer ------------------------------------------------
	discoverer, err := NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	discovered, err := discoverer.RepositoryIDs(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	wantDiscovered := []string{
		uuid.Nil.String(), repositories["github"].String(), repositories["gitlab"].String(),
	}
	sort.Strings(wantDiscovered)
	if got := strings.Join(repositoryIDStrings(discovered), ","); got != strings.Join(wantDiscovered, ",") {
		t.Fatalf("discovered = %s, want the two repositories and the nil repository id (%s)", got, strings.Join(wantDiscovered, ","))
	}

	// --- the real partition handler, the six work-item families --------------
	runAt := unitAt.Add(time.Hour)
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
	investment, err := NewWorkItemInvestmentExecutor(conn, workItemEngineTestClassifier{})
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

	// --- the newest row of every key is the full recompute -------------------
	issueTypeRows := discovererMatrixLines(t, ctx, conn, `
SELECT concat(ifNull(toString(repo_id), 'NULL'), '|', provider, '|', team_id, '|', issue_type_norm,
  '|created=', toString(argMax(created_count, computed_at)),
  '|completed=', toString(argMax(completed_count, computed_at)),
  '|active=', toString(argMax(active_count, computed_at)))
FROM issue_type_metrics_daily WHERE org_id = ? AND day = ?
GROUP BY repo_id, provider, team_id, issue_type_norm`, orgID, day)
	investmentRows := discovererMatrixLines(t, ctx, conn, `
SELECT concat(ifNull(toString(repo_id), 'NULL'), '|', ifNull(team_id, ''), '|', investment_area, '|', project_stream,
  '|completed=', toString(argMax(work_items_completed, computed_at)),
  '|units=', toString(argMax(delivery_units, computed_at)))
FROM investment_metrics_daily WHERE org_id = ? AND day = ?
GROUP BY repo_id, team_id, investment_area, project_stream`, orgID, day)
	classificationRows := discovererMatrixLines(t, ctx, conn, `
SELECT concat(ifNull(toString(repo_id), 'NULL'), '|', provider, '|', artifact_id)
FROM investment_classifications_daily WHERE org_id = ? AND day = ?
GROUP BY repo_id, provider, artifact_id`, orgID, day)
	stateRows := discovererMatrixLines(t, ctx, conn, `
SELECT concat(provider, '|', status, '|hours=', toString(round(duration_hours, 2)), '|items=', toString(items_touched))
FROM work_item_state_durations_daily FINAL WHERE org_id = ? AND day = ?`, orgID, day)
	holds := func(lines []string, want string) bool {
		for _, line := range lines {
			if line == want {
				return true
			}
		}
		return false
	}
	dump := func() string {
		return "\nissue_type_metrics_daily:\n  " + strings.Join(issueTypeRows, "\n  ") +
			"\ninvestment_metrics_daily:\n  " + strings.Join(investmentRows, "\n  ") +
			"\ninvestment_classifications_daily:\n  " + strings.Join(classificationRows, "\n  ") +
			"\nwork_item_state_durations_daily:\n  " + strings.Join(stateRows, "\n  ")
	}

	for _, provider := range providers {
		t.Run(provider, func(t *testing.T) {
			repoKey := "NULL"
			if repositories[provider] != uuid.Nil {
				repoKey = repositories[provider].String()
			}
			if want := repoKey + "|" + provider + "|unassigned|quality|created=1|completed=2|active=3"; !holds(issueTypeRows, want) {
				t.Errorf("issue_type_metrics_daily: the newest row is not the full recompute, want %s%s", want, dump())
			}
			// The investment key has no provider: jira and linear share the key
			// of the nil repository id, so it holds the items of both.
			wantInvestment := repoKey + "||quality|general|completed=2|units=4"
			if repoKey == "NULL" {
				wantInvestment = "NULL||quality|general|completed=4|units=8"
			}
			if !holds(investmentRows, wantInvestment) {
				t.Errorf("investment_metrics_daily: the newest row is not the full recompute, want %s%s", wantInvestment, dump())
			}
			for _, item := range []string{"1", "2", "3"} {
				if want := repoKey + "|" + provider + "|" + provider + ":" + item; !holds(classificationRows, want) {
					t.Errorf("investment_classifications_daily: no row %s%s", want, dump())
				}
			}
			if want := provider + "|in_progress|hours=10|items=1"; !holds(stateRows, want) {
				t.Errorf("work_item_state_durations_daily: no row %s%s", want, dump())
			}
		})
	}
}
