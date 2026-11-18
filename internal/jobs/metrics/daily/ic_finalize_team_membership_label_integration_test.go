//go:build integration

package daily

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestUserMetricsTeamIDIsTheMembershipLabelOfTheProductionFinalize pins the source of user_metrics_daily.team_id through
// the ic_finalize family as the production wiring builds it (NewICFinalizeExecutor) on real ClickHouse: the column is a
// membership label, NOT team attribution (docs/reference/metrics/definitions.md). Every repository of the day is owned by
// another team, so an ownership source would show.
//
//   - member: a membership in github:member-team -> github:member-team, not the owner.
//   - second: a primary membership in an inactive team and one in an active team -> the active team (the rank applies
//     after the inactive teams are left out).
//   - inactive-only: a membership in an inactive team only -> unassigned.
//   - loner: no membership -> unassigned, not the owner.
func TestUserMetricsTeamIDIsTheMembershipLabelOfTheProductionFinalize(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-000000004396"
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	t0 := day.Add(-72 * time.Hour)
	written := day.Add(30 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-000000006446")
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}

	for _, team := range []struct {
		id     string
		active uint8
	}{{"github:member-team", 1}, {"github:repo-owner", 1}, {"github:inactive-primary", 0}, {"github:second", 1}} {
		exec("insert team "+team.id, `INSERT INTO teams
    (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'github', ?)`,
			team.id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+team.id)), "Team "+team.id, []string{}, t0, t0, org, team.active)
	}
	for _, membership := range []struct {
		team, person string
		primary      uint8
	}{
		{"github:member-team", "member@example.com", 1},
		{"github:inactive-primary", "second@example.com", 1},
		{"github:second", "second@example.com", 0},
		{"github:inactive-primary", "inactive-only@example.com", 1},
	} {
		exec("insert membership of "+membership.person, `INSERT INTO team_memberships
    (org_id, provider, team_id, member_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, identity_facets)
    VALUES (?, 'github', ?, ?, ?, 'native', ?, 100, 10, ?, ?, ?, ?)`,
			org, membership.team, membership.person, membership.person, membership.primary, t0, (*time.Time)(nil), t0,
			[]string{membership.person})
	}
	exec("insert repo", `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, 'acme/api', ?, 'github', ?)`, repo, org, t0)
	exec("insert ownership", `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, 'github', 'github:repo-owner', ?, 'acme/api', 'exact', 'native', 1, 10, ?, ?)`, org, repo, t0, t0)
	people := []string{"member@example.com", "second@example.com", "inactive-only@example.com", "loner@example.com"}
	for _, person := range people {
		// The repository/user family writes every git row as unassigned.
		exec("insert user metrics of "+person, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, ?, ?, 'unassigned', 'Unassigned', 3, 40, 10, 1, 1, 50, 1, ?, ?)`,
			repo, day, person, person, written, org)
	}

	if _, err := NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, `SELECT author_email, toString(team_id) FROM (
    SELECT * FROM user_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ? AND day = ?`, org, day)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var person, team string
		if err := rows.Scan(&person, &team); err != nil {
			t.Fatal(err)
		}
		got[person] = team
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"member@example.com":        "github:member-team",
		"second@example.com":        "github:second",
		"inactive-only@example.com": "unassigned",
		"loner@example.com":         "unassigned",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("user_metrics_daily.team_id after the finalize\n got %v\nwant %v (the membership label; the owner github:repo-owner never)", got, want)
	}
}
