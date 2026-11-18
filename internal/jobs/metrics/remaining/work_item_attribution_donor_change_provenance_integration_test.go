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
func TestAnIssuesLinkedRowFollowsItsDonorsChangedRow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	now := time.Now().UTC()
	setup := now.Add(-72 * time.Hour)

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

			// Each backstop run is dated after the donor row it follows, so the
			// donor row is newer than the issue's row of the run before.
			runBackstopAt := func(offset time.Duration) WorkItemAttributionOutcome {
				t.Helper()
				executor := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return now.Add(offset) }}
				outcome, err := executor.ComputeOrg(ctx, orgID)
				if err != nil {
					t.Fatalf("backstop at %s: %v", offset, err)
				}
				return outcome
			}
			runBackstopAt(-24 * time.Hour)
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
			wantIssueRows(fmt.Sprintf("[/unassigned/1/ team-a/linked_issue/0/%s]", donorID))

			writeDonor := func(at time.Time) {
				t.Helper()
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
			writeDonor(now.Add(-12 * time.Hour))
			if outcome := runBackstopAt(-11 * time.Hour); !outcome.SkippedNoop || outcome.DonorChangedDependents != 0 {
				t.Fatalf("same-team donor row: outcome %+v, want a no-op", outcome)
			}

			seedItem(donorID, "proj-b", time.Now().UTC())
			writeDonor(now.Add(-6 * time.Hour))
			if outcome := runBackstopAt(-5 * time.Hour); outcome.DonorChangedDependents != 1 || outcome.RowsWritten == 0 {
				t.Fatalf("donor of another team: outcome %+v, want the issue rederived", outcome)
			}
			wantIssueRows(fmt.Sprintf("[/unassigned/1/ team-b/linked_issue/0/%s]", donorID))

			seedItem(donorID, "proj-unowned", time.Now().UTC())
			writeDonor(now.Add(-3 * time.Hour))
			if outcome := runBackstopAt(-2 * time.Hour); outcome.DonorChangedDependents != 1 || outcome.RowsWritten == 0 {
				t.Fatalf("donor with no team: outcome %+v, want the issue rederived", outcome)
			}
			wantIssueRows("[/unassigned/1/]")

			if outcome := runBackstopAt(-1 * time.Hour); !outcome.SkippedNoop || outcome.DonorChangedDependents != 0 {
				t.Fatalf("rerun: outcome %+v, want a no-op", outcome)
			}
		})
	}
}
