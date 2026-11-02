//go:build integration

package daily

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// landscapePoint is one (team, person) point of ic_landscape_rolling_30d, read
// from the churn_throughput map (the three maps carry the same values).
type landscapePoint struct {
	Churn, Delivery, WIP, Cycle float64
}

// landscapeWorkItemWorld is one organization of one provider, computed through
// the production families: the work_item family for each of three days, then
// ic_finalize for each of them.
type landscapeWorkItemWorld struct {
	provider, org     string
	alpha, beta, gama string
	// points by "identity in team" on the last day.
	points map[string]landscapePoint
	// the delivery units of user_metrics_daily over the 30-day window.
	storedDelivery float64
}

const (
	landscapeAlice = "alice@example.com"
	landscapeBob   = "bob@example.com"
	landscapeCarol = "carol@example.com"
	landscapeDave  = "dave@example.com"
	// The person of an item with no assignee (workitemmetrics: `assignee or "unassigned"`).
	landscapeNoAssignee = "unassigned"
)

// TestTheLandscapeTakesAWorkItemsTeamFromTheLadder holds the team of a work
// item's counts in the landscape to the item's attribution (the ladder,
// work_item_team_attributions is_primary = 1, and the co-owner rows
// is_primary = 2 for a team-grouped view), for every provider. A person's git
// rows keep the owner of their repository (D5985). Membership gives no team.
//
// The world of each provider (day D, the work_item family and ic_finalize run
// for D-2, D-1 and D):
//
//   - teams alpha, beta and gamma; alpha owns the repository alice commits to;
//     alice is a MEMBER of gamma, which owns nothing and is named on one item
//     only by a provenance row (assignee_membership, is_primary = 0);
//   - alice, scope S1: i1 and i11 open (beta), i2 done on D-1 (beta), i3 done on
//     D (no attribution row), i3b open (a primary row with no team); scope S2:
//     i7 open (beta). Each of her S1 rows is labelled beta (the team with the
//     most of her items, beta on a tie);
//   - bob: i8 done on D (beta); no git row;
//   - carol: i4 done on D, primary alpha, co-owner beta; no git row;
//   - an item with no assignee, i5 open (beta);
//   - dave: a stored work_item_user_metrics_daily row on D-1 written before the
//     row held a team split (2 done, 1 open, labelled beta);
//   - alice's git row on D: 40 + 10 lines, 2 merged pull requests, cycle 7 h.
func TestTheLandscapeTakesAWorkItemsTeamFromTheLadder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	for index, provider := range []string{"jira", "gitlab", "github", "linear"} {
		world := buildLandscapeWorkItemWorld(ctx, t, conn, provider, index, day)
		t.Run(provider, func(t *testing.T) {
			checkLandscapeWorkItemWorld(t, world)
		})
	}
}

func checkLandscapeWorkItemWorld(t *testing.T, world landscapeWorkItemWorld) {
	point := func(identity, team string) (landscapePoint, bool) {
		value, ok := world.points[identity+" in "+team]
		return value, ok
	}
	pointsOf := func(identity string) []string {
		var teams []string
		for key := range world.points {
			if strings.HasPrefix(key, identity+" in ") {
				teams = append(teams, strings.TrimPrefix(key, identity+" in "))
			}
		}
		sort.Strings(teams)
		return teams
	}
	t.Logf("points: %v", world.points)

	t.Run("C1 an attributed work item reaches the team of its primary attribution", func(t *testing.T) {
		alice, ok := point(landscapeAlice, world.beta)
		if !ok || alice.Delivery != 1 || alice.WIP != 3 {
			t.Errorf("alice in beta = %+v (present %v), want delivery 1 (i2) and wip 3 (i1, i11, i7)", alice, ok)
		}
		bob, ok := point(landscapeBob, world.beta)
		if !ok || bob.Delivery != 1 {
			t.Errorf("bob in beta = %+v (present %v), want delivery 1 (i8)", bob, ok)
		}
	})

	t.Run("C2 an unattributed work item stays on unassigned only", func(t *testing.T) {
		alice, ok := point(landscapeAlice, "unassigned")
		if !ok || alice.Delivery != 1 || alice.WIP != 1 || alice.Churn != 0 {
			t.Errorf("alice in unassigned = %+v (present %v), want delivery 1 (i3), wip 1 (i3b), churn 0", alice, ok)
		}
		if got, want := pointsOf(landscapeAlice), sortedStrings(world.alpha, world.beta, "unassigned"); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("alice's teams = %v, want %v", got, want)
		}
	})

	t.Run("C3 the organization totals do not change", func(t *testing.T) {
		// Each completed item and each merged pull request once: 2 merged pull
		// requests and the done items i2, i3, i4, i8 and dave's 2.
		if world.storedDelivery != 8 {
			t.Errorf("user_metrics_daily delivery units over the window = %v, want 8", world.storedDelivery)
		}
		// Over the points, each item once: carol's co-owned item is the one
		// item a team-grouped view shows under two teams, so her points are
		// left out and her one item is taken off the stored total.
		var sum float64
		for key, value := range world.points {
			if !strings.HasPrefix(key, landscapeCarol+" in ") {
				sum += value.Delivery
			}
		}
		if sum != world.storedDelivery-1 {
			t.Errorf("delivery units over the points (carol left out) = %v, want %v", sum, world.storedDelivery-1)
		}
	})

	t.Run("C4 a co-owned work item counts whole in each owning team", func(t *testing.T) {
		for _, team := range []string{world.alpha, world.beta} {
			if carol, ok := point(landscapeCarol, team); !ok || carol.Delivery != 1 {
				t.Errorf("carol in %s = %+v (present %v), want delivery 1 (i4)", team, carol, ok)
			}
		}
		if got, want := pointsOf(landscapeCarol), sortedStrings(world.alpha, world.beta); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("carol's teams = %v, want %v", got, want)
		}
	})

	t.Run("C5 membership gives a work item no team", func(t *testing.T) {
		for key := range world.points {
			if strings.HasSuffix(key, " in "+world.gama) {
				t.Errorf("point %s: gamma owns nothing and is alice's membership and a provenance row only", key)
			}
		}
	})

	t.Run("C6 the git half stays on the owner of the repository", func(t *testing.T) {
		alice, ok := point(landscapeAlice, world.alpha)
		if !ok || alice.Churn != 50 || alice.Delivery != 2 || alice.WIP != 0 || alice.Cycle != 7 {
			t.Errorf("alice in alpha = %+v (present %v), want churn 50, delivery 2 (merged pull requests), wip 0, cycle 7", alice, ok)
		}
		for _, team := range []string{world.beta, "unassigned"} {
			if value, ok := point(landscapeAlice, team); ok && (value.Churn != 0 || value.Cycle != 0) {
				t.Errorf("alice in %s = %+v: a work-item point holds no churn and no cycle time", team, value)
			}
		}
	})

	t.Run("C7 wip is the largest daily sum over the work scopes", func(t *testing.T) {
		// i1 and i11 (S1) and i7 (S2) are open on each of the three days: 3 a
		// day, never 9.
		if alice, ok := point(landscapeAlice, world.beta); !ok || alice.WIP != 3 {
			t.Errorf("alice in beta = %+v (present %v), want wip 3", alice, ok)
		}
	})

	t.Run("C8 a person with no git row has no point of the made-up repository", func(t *testing.T) {
		if got := pointsOf(landscapeBob); strings.Join(got, ",") != world.beta {
			t.Errorf("bob's teams = %v, want [%s] only", got, world.beta)
		}
	})

	t.Run("C9 an item with no assignee is no person of a team", func(t *testing.T) {
		if got := pointsOf(landscapeNoAssignee); strings.Join(got, ",") != "unassigned" {
			t.Errorf("the no-assignee person's teams = %v, want [unassigned] only", got)
		}
		if value, ok := point(landscapeNoAssignee, "unassigned"); !ok || value.WIP != 1 {
			t.Errorf("the no-assignee person in unassigned = %+v (present %v), want wip 1 (i5)", value, ok)
		}
	})

	t.Run("C10 a row stored without a team split stays on unassigned", func(t *testing.T) {
		if got := pointsOf(landscapeDave); strings.Join(got, ",") != "unassigned" {
			t.Errorf("dave's teams = %v, want [unassigned] only", got)
		}
		if dave, ok := point(landscapeDave, "unassigned"); !ok || dave.Delivery != 2 || dave.WIP != 1 {
			t.Errorf("dave in unassigned = %+v (present %v), want delivery 2 and wip 1", dave, ok)
		}
	})
}

func sortedStrings(values ...string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func buildLandscapeWorkItemWorld(
	ctx context.Context, t *testing.T, conn driver.Conn, provider string, index int, day time.Time,
) landscapeWorkItemWorld {
	t.Helper()
	world := landscapeWorkItemWorld{
		provider: provider,
		org:      fmt.Sprintf("00000000-0000-4000-8000-0000125350%02d", index),
		alpha:    provider + ":alpha", beta: provider + ":beta", gama: provider + ":gamma",
	}
	org := world.org
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s %s: %v", provider, what, err)
		}
	}
	longBefore := day.AddDate(0, 0, -60)
	gitRepo := uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"/git"))
	itemRepo := uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"/items"))

	for _, team := range []string{world.alpha, world.beta, world.gama} {
		exec("insert team "+team, `INSERT INTO teams
    (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)`,
			team, uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"/team/"+team)), "Team "+team, []string{}, longBefore, longBefore, org, provider)
	}
	exec("insert membership of alice in gamma", `INSERT INTO team_memberships
    (org_id, provider, team_id, member_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, identity_facets)
    VALUES (?, ?, ?, 'alice', ?, 'native', 1, 100, 10, ?, NULL, ?, ?)`,
		org, provider, world.gama, landscapeAlice, longBefore, longBefore, []string{landscapeAlice})
	exec("insert the git repository", `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`,
		gitRepo, "acme/"+provider+"-git", org, longBefore)
	exec("insert the ownership of the git repository", `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, valid_to, updated_at)
    VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 10, ?, NULL, ?)`,
		org, world.alpha, gitRepo, "acme/"+provider+"-git", longBefore, longBefore)

	at := func(days int, hour int) time.Time {
		return day.AddDate(0, 0, days).Add(time.Duration(hour) * time.Hour)
	}
	type item struct {
		id, scope, assignee     string
		started, completed      *time.Time
		primary                 *string // nil: no attribution row; "": a primary row with no team
		coOwner, provenanceTeam string
	}
	ptr := func(value time.Time) *time.Time { return &value }
	team := func(value string) *string { return &value }
	noTeam := ""
	items := []item{
		{id: "i1", scope: "S1", assignee: landscapeAlice, started: ptr(at(-5, 9)), primary: team(world.beta), provenanceTeam: world.gama},
		{id: "i11", scope: "S1", assignee: landscapeAlice, started: ptr(at(-5, 9)), primary: team(world.beta)},
		{id: "i2", scope: "S1", assignee: landscapeAlice, started: ptr(at(-1, 8)), completed: ptr(at(-1, 12)), primary: team(world.beta)},
		{id: "i3", scope: "S1", assignee: landscapeAlice, started: ptr(at(0, 8)), completed: ptr(at(0, 12))},
		{id: "i3b", scope: "S1", assignee: landscapeAlice, started: ptr(at(-5, 9)), primary: &noTeam},
		{id: "i7", scope: "S2", assignee: landscapeAlice, started: ptr(at(-4, 9)), primary: team(world.beta)},
		{id: "i8", scope: "S2", assignee: landscapeBob, started: ptr(at(0, 8)), completed: ptr(at(0, 15)), primary: team(world.beta)},
		{id: "i4", scope: "S1", assignee: landscapeCarol, started: ptr(at(0, 8)), completed: ptr(at(0, 9)), primary: team(world.alpha), coOwner: world.beta},
		{id: "i5", scope: "S1", started: ptr(at(-5, 9)), primary: team(world.beta)},
	}
	attributedAt := day.AddDate(0, 0, -20)
	synced := day.AddDate(0, 0, 2)
	for _, entry := range items {
		workItemID := provider + ":" + entry.scope + "-" + entry.id
		status := "in_progress"
		if entry.completed != nil {
			status = "done"
		}
		var assignees []string
		if entry.assignee != "" {
			assignees = []string{entry.assignee}
		}
		exec("insert item "+entry.id, `INSERT INTO work_items (
    repo_id, work_item_id, provider, status, project_key, project_id, native_team_key, project_name,
    type, assignees, created_at, started_at, completed_at, closed_at, story_points, org_id, last_synced)
    VALUES (?, ?, ?, ?, ?, ?, '', '', 'task', ?, ?, ?, ?, NULL, NULL, ?, ?)`,
			itemRepo, workItemID, provider, status, entry.scope, entry.scope, assignees,
			day.AddDate(0, 0, -10), entry.started, entry.completed, org, synced)
		attribution := func(teamID *string, source string, isPrimary uint8) {
			exec("insert attribution of "+entry.id, `INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'high', '{}', ?)`,
				org, itemRepo, workItemID, provider, teamID, teamID, source, isPrimary, attributedAt)
		}
		switch {
		case entry.primary == nil:
		case *entry.primary == "":
			attribution(nil, "unassigned", 1)
		default:
			attribution(entry.primary, "issue_project", 1)
		}
		if entry.coOwner != "" {
			attribution(team(entry.coOwner), "issue_project", 2)
		}
		if entry.provenanceTeam != "" {
			attribution(team(entry.provenanceTeam), "assignee_membership", 0)
		}
	}
	// A row written before the row held a team split: labelled beta, no split.
	exec("insert dave's stored row", `INSERT INTO work_item_user_metrics_daily
    (org_id, day, provider, work_scope_id, user_identity, team_id, team_name,
     items_started, items_completed, wip_count_end_of_day, computed_at, row_view, retracted)
    VALUES (?, ?, ?, 'LEGACY', ?, ?, ?, 0, 2, 1, ?, '', 0)`,
		org, day.AddDate(0, 0, -1), provider, landscapeDave, world.beta, "Team "+world.beta, at(-1, 20))
	// What the repository/user family writes for alice on D.
	exec("insert alice's git row", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, median_pr_cycle_hours, pr_cycle_p90_hours, computed_at, org_id)
    VALUES (?, ?, ?, ?, 'unassigned', 'Unassigned', 3, 40, 10, 2, 2, 7, 9, ?, ?)`,
		gitRepo, day, landscapeAlice, landscapeAlice, at(0, 20), org)

	workItems, err := NewWorkItemExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	finalize := NewICFinalizeExecutor(conn)
	partition := Partition{ID: "partition-" + provider, RepoIDs: []RepositoryID{RepositoryID(itemRepo.String())}}
	for offset := -2; offset <= 0; offset++ {
		target := day.AddDate(0, 0, offset)
		run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: target}
		if _, err := workItems.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("%s work_item of %s: %v", provider, target.Format("2006-01-02"), err)
		}
		if _, err := finalize.ComputeFinalizeFamily(ctx, run); err != nil {
			t.Fatalf("%s ic_finalize of %s: %v", provider, target.Format("2006-01-02"), err)
		}
	}

	world.points = map[string]landscapePoint{}
	rows, err := conn.Query(ctx, `SELECT identity_id, toString(team_id),
    toFloat64(churn_loc_30d), toFloat64(delivery_units_30d), toFloat64(wip_max_30d), cycle_p50_30d_hours
FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND map_name = 'churn_throughput'`, org, day)
	if err != nil {
		t.Fatalf("%s read the points: %v", provider, err)
	}
	defer rows.Close()
	for rows.Next() {
		var identity, teamID string
		var value landscapePoint
		if err := rows.Scan(&identity, &teamID, &value.Churn, &value.Delivery, &value.WIP, &value.Cycle); err != nil {
			t.Fatal(err)
		}
		world.points[identity+" in "+teamID] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT toFloat64(sum(delivery_units)) FROM (
    SELECT * FROM user_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ? AND day >= ? AND day <= ? AND retracted = 0`, org, day.AddDate(0, 0, -29), day).Scan(&world.storedDelivery); err != nil {
		t.Fatalf("%s read the stored delivery units: %v", provider, err)
	}
	return world
}
