//go:build integration

package icfinalize

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamactive"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestEachReadOfTheFamilyReadsAnInactiveTeamAsNoTeam holds the rule at each of
// the three reads alone, on real ClickHouse: the read-back of the day's user
// rows and the read of the day's work item rows give the fallback team (id
// and name) for a stored inactive id; the rolling read gives no team, so the
// landscape can take the mapped team. A stored active id is not touched.
//
// The work item read is held here and nowhere else: the merge takes the team
// of a person from the user row or from the member map, never from the work
// item row, so no output of the family shows that read's team today.
func TestEachReadOfTheFamilyReadsAnInactiveTeamAsNoTeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const org = "00000000-0000-4000-8000-00000009083b"
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	computedAt := day.Add(30 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000908b1")
	for person, team := range map[string]string{"old@example.com": "ENG", "new@example.com": "jira:ENG"} {
		if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, 1, 5, 1, ?, ?)`, repo, day, person, person, team, "Team "+team, computedAt, org); err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily
    (day, provider, work_scope_id, user_identity, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at, org_id)
    VALUES (?, 'jira', 'ENGPROJ', ?, ?, ?, 1, 1, 1, ?, ?)`, day, person, team, "Team "+team, computedAt, org); err != nil {
			t.Fatal(err)
		}
	}
	inactive := teamactive.Inactive{"ENG": {}}
	executor := NewExecutor(conn)

	gitRows, err := executor.loadGitMetrics(ctx, org, day, inactive)
	if err != nil {
		t.Fatal(err)
	}
	git := map[string]string{}
	for _, row := range gitRows {
		git[row.AuthorEmail] = row.TeamID + " / " + row.TeamName
	}
	if git["old@example.com"] != "unassigned / Unassigned" || git["new@example.com"] != "jira:ENG / Team jira:ENG" || len(git) != 2 {
		t.Errorf("the read-back of the user rows = %v, want the fallback team for old@ and the stored team for new@", git)
	}

	workRows, err := executor.loadWorkItemMetrics(ctx, org, day, inactive)
	if err != nil {
		t.Fatal(err)
	}
	work := map[string]string{}
	for _, row := range workRows {
		work[row.UserIdentity] = row.TeamID + " / " + row.TeamName
	}
	if work["old@example.com"] != "unassigned / Unassigned" || work["new@example.com"] != "jira:ENG / Team jira:ENG" || len(work) != 2 {
		t.Errorf("the read of the work item rows = %v, want the fallback team for old@ and the stored team for new@", work)
	}

	stats, err := LoadRollingStats(ctx, conn, org, day, inactive)
	if err != nil {
		t.Fatal(err)
	}
	rolling := map[string]string{}
	for _, stat := range stats {
		rolling[stat.IdentityID] = stat.TeamID
	}
	if team, found := rolling["old@example.com"]; !found || team != "" || rolling["new@example.com"] != "jira:ENG" || len(rolling) != 2 {
		t.Errorf("the rolling read = %v, want no team for old@ and the stored team for new@", rolling)
	}

	// With no inactive team the three reads give the stored ids.
	gitRows, err = executor.loadGitMetrics(ctx, org, day, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range gitRows {
		if row.AuthorEmail == "old@example.com" && row.TeamID != "ENG" {
			t.Errorf("with no inactive team the stored id is read as %q, want ENG", row.TeamID)
		}
	}
}
