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

// TestADonorsChangedRowRederivesTheItemsThatCopyIt (CHAOS-9478): a pull
// request copies its linked issue's stored row (CHAOS-9460). When the issue
// moves to a project of another team, the daily family writes the issue's new
// row, and no ownership changed, so no watermark scope covers the pull request.
// The backstop rederives the pull request from the donor's changed row, dated
// at its run; the next run finds nothing; a donor recomputed with the same team
// triggers nothing. One donor per provider; a Jira or Linear donor is linked
// through its issue key (extkey). A run with an ownership scope of its own (an
// unrelated repository's owner changed) rederives the pull request beside it.
func TestADonorsChangedRowRederivesTheItemsThatCopyIt(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	now := time.Now().UTC()
	setup := now.Add(-72 * time.Hour)

	for _, provider := range coOwnerProviders {
		for _, scoped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/scoped=%v", provider, scoped), func(t *testing.T) {
				orgID := "org-donor-change-" + provider + "-" + uuid.NewString()
				donorID := map[string]string{"github": "gh:acme/api#issue-1", "gitlab": "gl:acme/api#issue-1", "jira": "jira:CORE-1", "linear": "linear:CORE-1"}[provider]
				prProvider, prType := "github", "pr"
				if provider == "gitlab" {
					prProvider, prType = "gitlab", "merge_request"
				}
				prID := prProvider + "-pr:acme/web#7"
				target := donorID
				if provider == "jira" || provider == "linear" {
					target = "extkey:CORE-1"
				}
				for _, team := range []string{"team-a", "team-b"} {
					if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], [], 1, ?, ?, ?, ?, '')`,
						team, uuid.New(), "Team "+team, setup, setup, orgID, provider); err != nil {
						t.Fatalf("insert team %s: %v", team, err)
					}
				}
				seedWorkItemAttributionProjectOwnership(t, ctx, conn, orgID, provider, "proj-a", nil, "team-a", setup)
				seedWorkItemAttributionProjectOwnership(t, ctx, conn, orgID, provider, "proj-b", nil, "team-b", setup)
				seedItem := func(id, itemProvider, itemType, project string, repo uuid.UUID, syncedAt time.Time) {
					t.Helper()
					if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, status, created_at, org_id, last_synced)
					VALUES (?, ?, ?, ?, ?, 'in_progress', ?, ?, ?)`, repo, id, itemProvider, itemType, project, setup, orgID, syncedAt); err != nil {
						t.Fatalf("seed %s: %v", id, err)
					}
				}
				seedItem(donorID, provider, "issue", "proj-a", uuid.Nil, setup)
				seedItem(prID, prProvider, prType, "", uuid.New(), setup)
				seedWorkItemAttributionDependency(t, ctx, conn, orgID, prID, target, "relates_to", setup)

				// Yesterday's backstop: the first run of the organization is
				// org-wide and gives the pull request the donor's team-a.
				backstop, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
				if err != nil {
					t.Fatalf("new executor: %v", err)
				}
				yesterday := &WorkItemAttributionExecutor{conn: conn, writer: writer, nowUTC: func() time.Time { return now.Add(-24 * time.Hour) }}
				if _, err := yesterday.ComputeOrg(ctx, orgID); err != nil {
					t.Fatalf("first backstop: %v", err)
				}
				wantLinked := func(team string) {
					t.Helper()
					var got []string
					for _, row := range latestAttributions(t, ctx, conn, orgID, prID) {
						if row.isPrimary == 1 {
							got = append(got, fmt.Sprintf("%s/%s/%s", row.teamID, row.source, linkedIssueEvidenceDonor(row.evidence)))
						}
					}
					if want := fmt.Sprintf("[%s/linked_issue/%s]", team, donorID); fmt.Sprint(got) != want {
						t.Fatalf("pull request primary %v, want %s", got, want)
					}
				}
				wantLinked("team-a")

				// The daily family writes the donor's own row: once with the
				// same team, then after the donor moved to proj-b.
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
				runBackstop := func() WorkItemAttributionOutcome {
					t.Helper()
					outcome, err := backstop.ComputeOrg(ctx, orgID)
					if err != nil {
						t.Fatalf("backstop: %v", err)
					}
					return outcome
				}

				writeDonor(now.Add(-12 * time.Hour))
				if outcome := runBackstop(); !outcome.SkippedNoop || outcome.DonorChangedDependents != 0 {
					t.Fatalf("same-team donor row: outcome %+v, want a no-op", outcome)
				}
				wantLinked("team-a")

				seedItem(donorID, provider, "issue", "proj-b", uuid.Nil, time.Now().UTC())
				writeDonor(now.Add(-6 * time.Hour))
				unrelated := uuid.New()
				if scoped {
					seedWorkItemAttributionItem(t, ctx, conn, orgID, "gh-pr:acme/other#1", unrelated, setup)
					seedWorkItemAttributionRepoOwnership(t, ctx, conn, orgID, unrelated, "team-other", time.Now().UTC())
					waitForWorkItemAttributionScope(t, ctx, backstop, orgID, time.Now().UTC(), func(scope workItemAttributionScopeDecision) bool {
						return containsString(scope.repoIDs, unrelated.String())
					})
				}
				outcome := runBackstop()
				if scoped {
					if outcome.DonorChangedDependents != 1 || outcome.OrgWide || fmt.Sprint(outcome.RepoIDs) != fmt.Sprintf("[%s]", unrelated) {
						t.Fatalf("changed donor row beside a repository scope: outcome %+v", outcome)
					}
				} else if outcome.DonorChangedDependents != 1 || outcome.RowsWritten == 0 || outcome.OrgWide || len(outcome.RepoIDs)+len(outcome.ProjectKeys) != 0 {
					t.Fatalf("changed donor row: outcome %+v, want the pull request rederived by an item-only run", outcome)
				}
				wantLinked("team-b")

				if outcome := runBackstop(); !outcome.SkippedNoop || outcome.DonorChangedDependents != 0 {
					t.Fatalf("rerun: outcome %+v, want a no-op", outcome)
				}
			})
		}
	}
}
