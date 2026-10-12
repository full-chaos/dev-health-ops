//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// TestInferredOwnershipStartsAtItsEarliestEvidence is the provider matrix of
// the start of an inferred ownership row: valid_from is the earliest evidence
// time of the winning team's claim, never the time of the derive run.
//
//   - github: a team's project holds pull requests of the repository
//     (work_items type "pr"); the evidence time is the earliest one's
//     created_at.
//   - gitlab: the same with merge requests (type "merge_request").
//   - jira and linear: a team's project holds an issue linked to pull requests
//     of the repository (work_graph_issue_pr); the evidence time is the
//     earliest linked pull request's created_at (git_pull_requests).
//
// For each: the first run with no timed evidence writes the row at the run
// (the state every org is in today); once the evidence is synced, the next run
// writes the row at the evidence time and supersedes the run-time row, so one
// row is open; a third run writes nothing; and the ownership read of a day
// between the evidence and the first run names the team.
func TestInferredOwnershipStartsAtItsEarliestEvidence(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	// Whole seconds: a positional ? bind of a time on this driver keeps whole seconds only.
	evidence := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	later := evidence.Add(30 * 24 * time.Hour)

	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			org := "org-evidence-start-" + provider
			team := provider + "-team-core"
			repo := uuid.NewSHA1(uuid.NameSpaceURL, []byte("repo:"+provider))
			repoProvider := provider
			if provider == "jira" || provider == "linear" {
				repoProvider = "github"
			}
			if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, now64(3))`,
				repo, "acme/"+provider, org, repoProvider); err != nil {
				t.Fatal(err)
			}
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-core", team, true, evidence.Add(-time.Hour))

			// Evidence with NO time first: the run writes the row at the run.
			switch provider {
			case "github", "gitlab":
				itemType := map[string]string{"github": "pr", "gitlab": "merge_request"}[provider]
				insertTimedWorkItem(t, ctx, conn, org, provider+":acme#1", provider, itemType, repo, "proj-core", time.Time{})
			case "jira", "linear":
				insertTimedWorkItem(t, ctx, conn, org, provider+":CORE-1", provider, "issue", uuid.Nil, "proj-core", time.Time{})
				seedWorkGraphIssuePR(t, ctx, conn, org, repo, provider+":CORE-1", 7, time.Now().UTC())
			}
			service := TeamRepoOwnershipDerivationService{Conn: conn}
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("first derive: ready %v err %v", ready, err)
			}
			firstRun := openOwnershipStarts(t, ctx, conn, org, team)
			if len(firstRun) != 1 || firstRun[0].Before(time.Now().Add(-time.Hour)) {
				t.Fatalf("first run (no timed evidence): open rows start at %v, want one at the run", firstRun)
			}

			// The evidence gets its time (two pieces: the earliest wins).
			switch provider {
			case "github", "gitlab":
				itemType := map[string]string{"github": "pr", "gitlab": "merge_request"}[provider]
				insertTimedWorkItem(t, ctx, conn, org, provider+":acme#1", provider, itemType, repo, "proj-core", evidence)
				insertTimedWorkItem(t, ctx, conn, org, provider+":acme#2", provider, itemType, repo, "proj-core", later)
			case "jira", "linear":
				insertPullRequest(t, ctx, conn, org, repo, 7, evidence)
				insertTimedWorkItem(t, ctx, conn, org, provider+":CORE-2", provider, "issue", uuid.Nil, "proj-core", time.Time{})
				seedWorkGraphIssuePR(t, ctx, conn, org, repo, provider+":CORE-2", 8, time.Now().UTC())
				insertPullRequest(t, ctx, conn, org, repo, 8, later)
			}
			_, _, _, _, stats, err := service.DeriveWithStats(ctx, org)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Superseded != 1 {
				t.Errorf("second run superseded %d row(s), want the run-time row (1)", stats.Superseded)
			}
			second := openOwnershipStarts(t, ctx, conn, org, team)
			if len(second) != 1 || !second[0].Equal(evidence) {
				t.Fatalf("after the evidence is synced: open rows start at %v, want exactly one at %v", second, evidence)
			}

			written, _, _, _, stats, err := service.DeriveWithStats(ctx, org)
			if err != nil {
				t.Fatal(err)
			}
			if written != 0 || stats.Superseded != 0 {
				t.Errorf("third run wrote %d and superseded %d, want 0 and 0", written, stats.Superseded)
			}
			if third := openOwnershipStarts(t, ctx, conn, org, team); len(third) != 1 || !third[0].Equal(evidence) {
				t.Errorf("after a third run: open rows start at %v, want one at %v", third, evidence)
			}

			owned, err := teamownership.OwnedRepoIDs(ctx, conn, org, team, evidence.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(owned) != 1 || owned[0] != repo {
				t.Errorf("owned repositories of %s the day after the evidence = %v, want [%s]", team, owned, repo)
			}
			before, err := teamownership.OwnedRepoIDs(ctx, conn, org, team, evidence.Add(-time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if len(before) != 0 {
				t.Errorf("owned repositories of %s just before the evidence = %v, want none", team, before)
			}
		})
	}
}

func insertTimedWorkItem(
	t *testing.T, ctx context.Context, conn driver.Conn,
	org, workItemID, provider, itemType string, repoID uuid.UUID, projectID string, createdAt time.Time,
) {
	t.Helper()
	at := createdAt
	if at.IsZero() {
		at = time.Unix(0, 0).UTC()
	}
	if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, created_at, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, now64(3))`, repoID, workItemID, provider, itemType, projectID, at, org); err != nil {
		t.Fatal(err)
	}
}

func insertPullRequest(t *testing.T, ctx context.Context, conn driver.Conn, org string, repoID uuid.UUID, number uint32, createdAt time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO git_pull_requests (repo_id, number, created_at, last_synced, org_id) VALUES (?, ?, ?, now64(3), ?)`,
		repoID, number, createdAt, org); err != nil {
		t.Fatal(err)
	}
}

// openOwnershipStarts is the valid_from of each open inferred row of a team,
// the rows a read of any instant from their start on can see.
func openOwnershipStarts(t *testing.T, ctx context.Context, conn driver.Conn, org, team string) []time.Time {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT valid_from FROM team_repo_ownership FINAL
WHERE org_id = ? AND team_id = ? AND source = 'inferred' AND valid_to IS NULL ORDER BY valid_from`, org, team)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var starts []time.Time
	for rows.Next() {
		var start time.Time
		if err := rows.Scan(&start); err != nil {
			t.Fatal(err)
		}
		starts = append(starts, start.UTC())
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return starts
}
