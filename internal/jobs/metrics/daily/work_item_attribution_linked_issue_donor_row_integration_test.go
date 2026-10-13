//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// TestALinkedItemCopiesTheTeamOfItsDonorsOwnRow (CHAOS-9460): a pull request
// that inherits through a linked issue takes the team of the issue's own
// primary attribution row, never a resolution of the issue on the pull
// request's day. The donor issue D's stored row names team-donor
// (repo_ownership, written when D was attributed); on the pull request's day
// D's repository is owned by team-other, which is what a resolution of D on
// that day would give. Every donor provider. The donor's row is its newest
// primary row. A donor with no stored row gives nothing: the pull request is
// not given a team by linked_issue.
func TestALinkedItemCopiesTheTeamOfItsDonorsOwnRow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	targetDay := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seededAt := targetDay.Add(time.Hour)

	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		for _, stored := range []bool{true, false} {
			name := provider + "/donor_row"
			if !stored {
				name = provider + "/no_donor_row"
			}
			t.Run(name, func(t *testing.T) {
				orgID := "org-donor-row-" + uuid.NewString()
				repoPR, repoD := uuid.New(), uuid.New()
				donor := provider + ":D-1"
				seedWorkItemAttributionLinkedItem(t, ctx, conn, orgID, "PR", repoPR, seededAt)
				seedWorkItemAttributionDonorItem(t, ctx, conn, orgID, donor, provider, repoD, seededAt)
				seedWorkItemAttributionLinkedRepoOwnership(t, ctx, conn, orgID, repoD, "team-other", targetDay)
				seedWorkItemAttributionLinkedDependency(t, ctx, conn, orgID, "PR", donor, "relates_to", seededAt)
				if stored {
					// An older primary row of another team: the donor's row is
					// its newest primary row.
					seedWorkItemAttributionStoredRow(t, ctx, conn, orgID, repoD, donor, provider, "team-old", "repo_ownership", targetDay.Add(-48*time.Hour))
					seedWorkItemAttributionStoredRow(t, ctx, conn, orgID, repoD, donor, provider, "team-donor", "repo_ownership", targetDay.Add(-24*time.Hour))
				}

				executor, err := NewWorkItemAttributionExecutor(conn)
				if err != nil {
					t.Fatalf("new executor: %v", err)
				}
				run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: targetDay}
				partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoPR.String())}}
				if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
					t.Fatalf("ComputeFamily: %v", err)
				}

				var linked []workItemAttributionLinkedRow
				for _, row := range queryWorkItemAttributionLinkedRows(t, ctx, conn, orgID, "PR") {
					if row.source == "linked_issue" {
						linked = append(linked, row)
					}
				}
				if !stored {
					if len(linked) != 0 {
						t.Fatalf("the pull request has linked_issue rows %+v from a donor with no row of its own, want none", linked)
					}
					return
				}
				if len(linked) != 1 || linked[0].teamID == nil || *linked[0].teamID != "team-donor" || linked[0].isPrimary != 1 {
					t.Fatalf("linked_issue rows %+v, want one primary row of team-donor (the donor's own row), "+
						"not team-other (a resolution of the donor on the pull request's day)", linked)
				}
				assertLinkedIssueEvidence(t, ctx, conn, orgID, "PR",
					"linked_issue="+donor+";dependency_type=relates_to;donor_provider="+provider+";donor_source=repo_ownership")
			})
		}
	}
}

func seedWorkItemAttributionDonorItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID, workItemID, provider string, repoID uuid.UUID, createdAt time.Time,
) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, created_at, org_id, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
		repoID, workItemID, provider, createdAt, orgID, createdAt); err != nil {
		t.Fatalf("seed donor work item: %v", err)
	}
}

func seedWorkItemAttributionStoredRow(
	t *testing.T, ctx context.Context, conn driver.Conn,
	orgID string, repoID uuid.UUID, workItemID, provider, teamID, source string, computedAt time.Time,
) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO work_item_team_attributions
		(org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at, writer, run_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'medium', ?, ?, 'test', 'earlier-run')`,
		orgID, repoID, workItemID, provider, teamID, teamID, source, source+"="+repoID.String(), computedAt); err != nil {
		t.Fatalf("seed the donor's stored attribution row: %v", err)
	}
}

// TestALinkedItemIsRevisitedOnceItsDonorHasARow (CHAOS-9460, lead ruling A):
// run 1, the donor D has no attribution row, so the pull request gets no
// linked_issue row and falls through. D's own partition then writes D's row
// (team-donor by project_ownership on day 1). Run 2, on day 2, the pull
// request inherits D's row: team-donor by linked_issue, although on day 2 D's
// project is owned by team-other (what a resolution of D on day 2 would give).
// Every donor provider.
func TestALinkedItemIsRevisitedOnceItsDonorHasARow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	seededAt := day1.Add(-time.Hour)

	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			orgID := "org-donor-revisit-" + uuid.NewString()
			repoPR, repoD := uuid.New(), uuid.New()
			donor := provider + ":D-1"
			seedWorkItemAttributionLinkedItem(t, ctx, conn, orgID, "PR", repoPR, seededAt)
			if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, project_id, created_at, org_id, last_synced) VALUES (?, ?, ?, 'proj-d', ?, ?, ?)`,
				repoD, donor, provider, seededAt, orgID, seededAt); err != nil {
				t.Fatalf("seed donor: %v", err)
			}
			for _, owner := range []struct {
				team string
				from time.Time
				to   any
			}{{"team-donor", day1.AddDate(0, 0, -3), day2}, {"team-other", day2, nil}} {
				if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
					(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, last_synced)
					VALUES (?, ?, ?, 'proj-d', NULL, 'native', 1, 100, 0, ?, ?, now64(3), now64(3))`,
					orgID, provider, owner.team, owner.from, owner.to); err != nil {
					t.Fatalf("seed project ownership %s: %v", owner.team, err)
				}
			}
			seedWorkItemAttributionLinkedDependency(t, ctx, conn, orgID, "PR", donor, "relates_to", seededAt)
			executor, err := NewWorkItemAttributionExecutor(conn)
			if err != nil {
				t.Fatalf("new executor: %v", err)
			}
			compute := func(day time.Time, repo uuid.UUID) {
				t.Helper()
				run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
				partition := Partition{ID: "p-" + repo.String(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
				if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
					t.Fatalf("ComputeFamily %s %v: %v", day.Format(time.DateOnly), repo, err)
				}
			}
			linked := func() []workItemAttributionLinkedRow {
				var rows []workItemAttributionLinkedRow
				for _, row := range queryWorkItemAttributionLinkedRows(t, ctx, conn, orgID, "PR") {
					if row.source == "linked_issue" {
						rows = append(rows, row)
					}
				}
				return rows
			}

			compute(day1, repoPR)
			if rows := linked(); len(rows) != 0 {
				t.Fatalf("run 1: the pull request has linked_issue rows %+v while its donor has no row, want none", rows)
			}
			compute(day1, repoD)
			if donorRows := queryWorkItemAttributionLinkedRows(t, ctx, conn, orgID, donor); len(donorRows) != 1 || donorRows[0].source != "project_ownership" || donorRows[0].teamID == nil || *donorRows[0].teamID != "team-donor" {
				t.Fatalf("the donor's own row = %+v, want team-donor by project_ownership", donorRows)
			}
			compute(day2, repoPR)
			rows := linked()
			if len(rows) != 1 || rows[0].teamID == nil || *rows[0].teamID != "team-donor" || rows[0].isPrimary != 1 {
				t.Fatalf("run 2: linked_issue rows %+v, want one primary row of team-donor (the donor's row), "+
					"not team-other (a resolution of the donor on day 2)", rows)
			}
			assertLinkedIssueEvidence(t, ctx, conn, orgID, "PR",
				"linked_issue="+donor+";dependency_type=relates_to;donor_provider="+provider+";donor_source=project_ownership")
		})
	}
}

// assertLinkedIssueEvidence holds the evidence of the item's linked_issue row:
// the donor's provenance (team-attribution.md section 0, row 5).
func assertLinkedIssueEvidence(t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID, want string) {
	t.Helper()
	var evidence string
	if err := conn.QueryRow(ctx, `SELECT evidence FROM work_item_team_attributions FINAL
		WHERE org_id = ? AND work_item_id = ? AND source = 'linked_issue'`, orgID, workItemID).Scan(&evidence); err != nil {
		t.Fatalf("read the linked_issue evidence: %v", err)
	}
	if evidence != want {
		t.Errorf("linked_issue evidence = %q, want %q", evidence, want)
	}
}

// TestALinkedItemCopiesADonorThatAnotherPartitionOfTheRunWrites (CHAOS-9460,
// round 2 finding): the pull request and its donor are in two partitions of
// one run, and the pull request's partition runs first, before the donor has
// any row. The donor is in the run's scope and in the day's window, so the
// run writes its row for the day: the pull request copies that row (computed
// with the day's facts), not nothing. Every donor provider.
func TestALinkedItemCopiesADonorThatAnotherPartitionOfTheRunWrites(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seededAt := day.Add(-time.Hour)

	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			orgID := "org-donor-same-run-" + uuid.NewString()
			repoPR, repoD := uuid.New(), uuid.New()
			donor := provider + ":D-1"
			seedWorkItemAttributionLinkedItem(t, ctx, conn, orgID, "PR", repoPR, seededAt)
			if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, project_id, created_at, org_id, last_synced) VALUES (?, ?, ?, 'proj-d', ?, ?, ?)`,
				repoD, donor, provider, seededAt, orgID, seededAt); err != nil {
				t.Fatalf("seed donor: %v", err)
			}
			if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
				(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, last_synced)
				VALUES (?, ?, 'team-donor', 'proj-d', NULL, 'native', 1, 100, 0, ?, NULL, now64(3), now64(3))`, orgID, provider, day.AddDate(0, 0, -3)); err != nil {
				t.Fatalf("seed project ownership: %v", err)
			}
			seedWorkItemAttributionLinkedDependency(t, ctx, conn, orgID, "PR", donor, "relates_to", seededAt)
			executor, err := NewWorkItemAttributionExecutor(conn)
			if err != nil {
				t.Fatalf("new executor: %v", err)
			}
			run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day,
				DiscoveredRepoIDs: []RepositoryID{RepositoryID(repoPR.String()), RepositoryID(repoD.String())}}
			for _, repo := range []uuid.UUID{repoPR, repoD} {
				partition := Partition{ID: "p-" + repo.String(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
				if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
					t.Fatalf("ComputeFamily %v: %v", repo, err)
				}
			}
			var linked []workItemAttributionLinkedRow
			for _, row := range queryWorkItemAttributionLinkedRows(t, ctx, conn, orgID, "PR") {
				if row.source == "linked_issue" {
					linked = append(linked, row)
				}
			}
			if len(linked) != 1 || linked[0].teamID == nil || *linked[0].teamID != "team-donor" || linked[0].isPrimary != 1 {
				t.Fatalf("linked_issue rows %+v, want one primary row of team-donor: the donor's row this run writes for the day", linked)
			}
			assertLinkedIssueEvidence(t, ctx, conn, orgID, "PR",
				"linked_issue="+donor+";dependency_type=relates_to;donor_provider="+provider+";donor_source=project_ownership")
		})
	}
}

// TestADonorOutsideTheDaysWindowGivesItsStoredRow: a donor in the run's scope
// that is in no partition of the day -- completed before the day, or created
// after it -- is not written by the run: the pull request copies its stored
// row (team-old), never a resolution of it with the day's facts (team-donor).
func TestADonorOutsideTheDaysWindowGivesItsStoredRow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seededAt := day.AddDate(0, 0, -20)
	for _, tc := range []struct {
		name               string
		created, completed time.Time
		status             string
	}{
		{"completed before the day", seededAt, day.AddDate(0, 0, -10), "done"},
		{"created after the day", day.AddDate(0, 0, 2), time.Time{}, "todo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orgID := "org-donor-window-" + uuid.NewString()
			repoPR, repoD := uuid.New(), uuid.New()
			seedWorkItemAttributionLinkedItem(t, ctx, conn, orgID, "PR", repoPR, seededAt)
			var completed any
			if !tc.completed.IsZero() {
				completed = tc.completed
			}
			if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, project_id, status, created_at, completed_at, org_id, last_synced) VALUES (?, 'jira:D-1', 'jira', 'proj-d', ?, ?, ?, ?, ?)`,
				repoD, tc.status, tc.created, completed, orgID, seededAt); err != nil {
				t.Fatalf("seed donor: %v", err)
			}
			if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
				(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, last_synced)
				VALUES (?, 'jira', 'team-donor', 'proj-d', NULL, 'native', 1, 100, 0, ?, NULL, now64(3), now64(3))`, orgID, seededAt); err != nil {
				t.Fatalf("seed project ownership: %v", err)
			}
			seedWorkItemAttributionStoredRow(t, ctx, conn, orgID, repoD, "jira:D-1", "jira", "team-old", "project_ownership", day.AddDate(0, 0, -10))
			seedWorkItemAttributionLinkedDependency(t, ctx, conn, orgID, "PR", "jira:D-1", "relates_to", seededAt)
			executor, err := NewWorkItemAttributionExecutor(conn)
			if err != nil {
				t.Fatalf("new executor: %v", err)
			}
			run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day,
				DiscoveredRepoIDs: []RepositoryID{RepositoryID(repoPR.String()), RepositoryID(repoD.String())}}
			if _, err := executor.ComputeFamily(ctx, run, Partition{ID: "p1", RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repoPR.String())}}); err != nil {
				t.Fatalf("ComputeFamily: %v", err)
			}
			var linked []workItemAttributionLinkedRow
			for _, row := range queryWorkItemAttributionLinkedRows(t, ctx, conn, orgID, "PR") {
				if row.source == "linked_issue" {
					linked = append(linked, row)
				}
			}
			if len(linked) != 1 || linked[0].teamID == nil || *linked[0].teamID != "team-old" {
				t.Fatalf("linked_issue rows %+v, want team-old (the donor's stored row), not team-donor (a resolution on the day)", linked)
			}
		})
	}
}
