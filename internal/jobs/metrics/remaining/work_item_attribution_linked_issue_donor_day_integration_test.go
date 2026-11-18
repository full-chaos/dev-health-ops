//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// TestTheBackstopGivesALinkedItemItsDonorsRowOfTheLinkedItemsDay (CHAOS-9460,
// lead ruling (i)): the backstop resolves every item with the facts of its own
// day, and a linked item copies its donor's row as of the linked item's day,
// as the daily partition of that day does. Donor D (an issue, completed on day 1)
// is in a project owned by team-donor on day 1; on day 2, when the pull request P
// is completed, the project is owned by team-other. D's own row (its day) is
// team-donor; P copies D's row of P's day, team-other, with D's provenance. An
// issue takes no team from its own repository, so the donor row is the project's.
func TestTheBackstopGivesALinkedItemItsDonorsRowOfTheLinkedItemsDay(t *testing.T) {
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
	const projectD = "project-donor"
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day1, day2 := today.AddDate(0, 0, -10), today.AddDate(0, 0, -5)
	handover := day1.AddDate(0, 0, 2)

	for _, item := range []struct {
		id, kind, project string
		complete          time.Time
	}{{"D", "issue", projectD, day1.Add(12 * time.Hour)}, {"P", "pr", "", day2.Add(12 * time.Hour)}} {
		if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, status, created_at, completed_at, org_id, last_synced)
			VALUES (?, ?, 'github', ?, ?, 'done', ?, ?, ?, ?)`, uuid.Nil, item.id, item.kind, item.project, day1.AddDate(0, 0, -1), item.complete, orgID, now); err != nil {
			t.Fatalf("seed %s: %v", item.id, err)
		}
	}
	for _, owner := range []struct {
		team     string
		from     time.Time
		to       any
		priority int32
	}{{"team-donor", day1.AddDate(0, 0, -3), handover, 0}, {"team-other", handover, nil, 0}} {
		if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
			(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, 'DON', 'native', 1, 100, ?, ?, ?, ?)`,
			orgID, owner.team, projectD, owner.priority, owner.from, owner.to, now); err != nil {
			t.Fatalf("seed ownership %s: %v", owner.team, err)
		}
	}
	seedWorkItemAttributionDependency(t, ctx, conn, orgID, "P", "D", "relates_to", now)
	waitForWorkItemAttributionProjectFactVisible(t, ctx, conn, orgID)

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
	if teams["D/project_ownership"] != "team-donor" {
		t.Fatalf("D's own row = %v, want team-donor by project_ownership on D's day", teams)
	}
	if teams["P/linked_issue"] != "team-other" {
		t.Fatalf("P's rows = %v, want linked_issue team-other (D's row of P's day), not team-donor "+
			"(D's row of its own day)", teams)
	}
	var evidence string
	if err := conn.QueryRow(ctx, `SELECT evidence FROM work_item_team_attributions FINAL
		WHERE org_id = ? AND work_item_id = 'P' AND source = 'linked_issue'`, orgID).Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	if want := "linked_issue=D;dependency_type=relates_to;donor_provider=github;donor_source=project_ownership"; evidence != want {
		t.Errorf("P's linked_issue evidence = %q, want %q", evidence, want)
	}
}

func waitForWorkItemAttributionProjectFactVisible(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for attempt := 1; ; attempt++ {
		projects, err := teamattribution.ClickHouseFactSource{Conn: conn}.LoadProjects(ctx, orgID, time.Now().UTC())
		if err != nil {
			t.Fatalf("poll LoadProjects: %v", err)
		}
		if len(projects) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("LoadProjects never returned a row within the poll budget (%d attempts)", attempt)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
