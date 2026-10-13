//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestTheBackstopGivesALinkedItemItsDonorsRowOfTheDonorsOwnDay (CHAOS-9460):
// the backstop resolves every item with the facts of its own day. Donor D
// (completed on day 1) is owned by team-donor on day 1; on day 2, when the
// pull request P is completed, D's repository is owned by team-other. P
// inherits through its relates_to edge to D, so it takes the team of D's own
// row, of D's own day: team-donor, never a resolution of D on P's day.
func TestTheBackstopGivesALinkedItemItsDonorsRowOfTheDonorsOwnDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	executor, err := NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-donor-day-" + uuid.NewString()
	repoD := uuid.New()
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day1, day2 := today.AddDate(0, 0, -10), today.AddDate(0, 0, -5)
	handover := day1.AddDate(0, 0, 2)

	for _, item := range []struct {
		id       string
		repo     uuid.UUID
		complete time.Time
	}{{"D", repoD, day1.Add(12 * time.Hour)}, {"P", uuid.Nil, day2.Add(12 * time.Hour)}} {
		if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, project_id, status, created_at, completed_at, org_id, last_synced)
			VALUES (?, ?, 'github', '', 'done', ?, ?, ?, ?)`, item.repo, item.id, day1.AddDate(0, 0, -1), item.complete, orgID, now); err != nil {
			t.Fatalf("seed %s: %v", item.id, err)
		}
	}
	for _, owner := range []struct {
		team     string
		from     time.Time
		to       any
		priority int32
	}{{"team-donor", day1.AddDate(0, 0, -3), handover, 0}, {"team-other", handover, nil, 0}} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, 'acme/donor-repo', 'exact', 'native', 1, 100, ?, ?, ?, ?)`,
			orgID, owner.team, repoD, owner.priority, owner.from, owner.to, now); err != nil {
			t.Fatalf("seed ownership %s: %v", owner.team, err)
		}
	}
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "P", "D", "relates_to", now)
	waitForWorkItemAttributionRepoFactVisible(t, ctx, conn, orgID)

	if _, err := executor.ComputeOrg(ctx, orgID); err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}

	rows, err := conn.Query(ctx, `SELECT work_item_id, toString(source), ifNull(team_id, '') FROM work_item_team_attributions FINAL
		WHERE org_id = ? AND is_primary = 1`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	teams := map[string]string{}
	for rows.Next() {
		var id, source, team string
		if err := rows.Scan(&id, &source, &team); err != nil {
			t.Fatal(err)
		}
		teams[id+"/"+source] = team
	}
	rows.Close()
	if teams["D/repo_ownership"] != "team-donor" {
		t.Fatalf("D's own row = %v, want team-donor by repo_ownership on D's day", teams)
	}
	if teams["P/linked_issue"] != "team-donor" {
		t.Fatalf("P's rows = %v, want linked_issue team-donor (D's own row), not team-other "+
			"(a resolution of D on P's day)", teams)
	}
	var evidence string
	if err := conn.QueryRow(ctx, `SELECT evidence FROM work_item_team_attributions FINAL
		WHERE org_id = ? AND work_item_id = 'P' AND source = 'linked_issue'`, orgID).Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	if want := "linked_issue=D;dependency_type=relates_to;donor_provider=github;donor_source=repo_ownership"; evidence != want {
		t.Errorf("P's linked_issue evidence = %q, want %q", evidence, want)
	}
}
