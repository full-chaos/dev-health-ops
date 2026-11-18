//go:build integration

package remaining

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// An issue's linked_issue row is provenance only (is_primary = 0) beside its
// unassigned primary row. When its donor's row changes, the backstop still
// rederives the issue from the linked_issue row of its newest snapshot: a donor
// of another team gives a new provenance row, a donor with no team leaves the
// issue with no linked_issue row, a donor recomputed with the same team
// triggers nothing. Jira and Linear issues.
//
// The backstop never writes an item that already has a row of the run's UTC
// date (alreadyCoveredToday), so each step runs on its own date, and one
// subtest pins that rule: a second donor change on the same date waits, and the
// issue follows it on the next date.
func TestAnIssuesLinkedRowFollowsItsDonorsChangedRow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day := func(daysAgo int, hour int) time.Time {
		return today.AddDate(0, 0, -daysAgo).Add(time.Duration(hour) * time.Hour)
	}
	setup := today.AddDate(0, 0, -10)

	for _, provider := range []string{"jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			orgID := "org-donor-provenance-" + provider + "-" + uuid.NewString()
			issueID, donorID := provider+":CORE-2", provider+":CORE-1"
			for _, team := range []string{"team-a", "team-b"} {
				if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], [], 1, ?, ?, ?, ?, '')`,
					team, uuid.New(), "Team "+team, setup, setup, orgID, provider); err != nil {
					t.Fatalf("insert team %s: %v", team, err)
				}
			}
			seedWorkItemAttributionProjectOwnership(t, ctx, conn, orgID, provider, "proj-a", nil, "team-a", setup)
			seedWorkItemAttributionProjectOwnership(t, ctx, conn, orgID, provider, "proj-b", nil, "team-b", setup)
			seedItem := func(id, project string, syncedAt time.Time) {
				t.Helper()
				if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, status, created_at, org_id, last_synced)
					VALUES (?, ?, ?, 'story', ?, 'in_progress', ?, ?, ?)`, uuid.Nil, id, provider, project, setup, orgID, syncedAt); err != nil {
					t.Fatalf("seed %s: %v", id, err)
				}
			}
			seedItem(donorID, "proj-a", setup)
			seedItem(issueID, "proj-none", setup)
			seedWorkItemAttributionDependency(t, ctx, conn, orgID, issueID, donorID, "relates_to", setup)

			runBackstopAt := func(at time.Time) WorkItemAttributionOutcome {
				t.Helper()
				executor := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return at }}
				outcome, err := executor.ComputeOrg(ctx, orgID)
				if err != nil {
					t.Fatalf("backstop at %s: %v", at, err)
				}
				return outcome
			}
			wantIssueRows := func(want string) {
				t.Helper()
				var got []string
				for _, row := range latestAttributions(t, ctx, conn, orgID, issueID) {
					got = append(got, fmt.Sprintf("%s/%s/%d/%s", row.teamID, row.source, row.isPrimary, linkedIssueEvidenceDonor(row.evidence)))
				}
				if fmt.Sprint(got) != want {
					t.Fatalf("issue rows %v, want %s", got, want)
				}
			}
			// moveDonor puts the donor in a project and writes its own row, as
			// the daily family does.
			moveDonor := func(project string, at time.Time) {
				t.Helper()
				seedItem(donorID, project, at)
				subjects, err := LoadWorkItemDonorSubjects(ctx, conn, orgID, []string{donorID}, nil)
				if err != nil || len(subjects) != 1 {
					t.Fatalf("load donor subject: %v %v", subjects, err)
				}
				facts, err := LoadWorkItemDerivationFacts(ctx, conn, orgID, at)
				if err != nil {
					t.Fatalf("load facts: %v", err)
				}
				rows := BuildWorkItemAttributionRows(orgID, at, map[string]struct{}{donorID: {}}, subjects, teamattribution.NewGitHubWorkItemDerivationContext(facts))
				if _, err := writer.WriteAttributions(ctx, WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: uuid.NewString()}, rows); err != nil {
					t.Fatalf("write donor: %v", err)
				}
			}
			wantRederived := func(step string, outcome WorkItemAttributionOutcome) {
				t.Helper()
				if outcome.DonorChangedDependents != 1 || outcome.RowsWritten == 0 {
					t.Fatalf("%s: outcome %+v, want the issue rederived", step, outcome)
				}
			}
			linkedTo := func(team string) string {
				return fmt.Sprintf("[/unassigned/1/ %s/linked_issue/0/%s]", team, donorID)
			}

			runBackstopAt(day(6, 12))
			wantIssueRows(linkedTo("team-a"))

			moveDonor("proj-a", day(5, 6))
			if outcome := runBackstopAt(day(5, 12)); !outcome.SkippedNoop || outcome.DonorChangedDependents != 0 {
				t.Fatalf("same-team donor row: outcome %+v, want a no-op", outcome)
			}

			moveDonor("proj-b", day(4, 6))
			wantRederived("donor of another team", runBackstopAt(day(4, 12)))
			wantIssueRows(linkedTo("team-b"))

			t.Run("a second donor change on the same UTC date waits for the next date", func(t *testing.T) {
				moveDonor("proj-a", day(4, 14))
				if outcome := runBackstopAt(day(4, 18)); outcome.DonorChangedDependents != 1 || outcome.RowsWritten != 0 {
					t.Fatalf("same date: outcome %+v, want the issue found and its write deferred (0 rows)", outcome)
				}
				wantIssueRows(linkedTo("team-b"))
				wantRederived("next date", runBackstopAt(day(3, 12)))
				wantIssueRows(linkedTo("team-a"))
			})

			moveDonor("proj-unowned", day(2, 6))
			wantRederived("donor with no team", runBackstopAt(day(2, 12)))
			wantIssueRows("[/unassigned/1/]")

			if outcome := runBackstopAt(day(2, 18)); !outcome.SkippedNoop || outcome.DonorChangedDependents != 0 {
				t.Fatalf("rerun: outcome %+v, want a no-op", outcome)
			}
		})
	}
}
