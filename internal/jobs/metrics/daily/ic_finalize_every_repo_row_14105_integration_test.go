//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/icfinalize"
)

// CHAOS-14105: a person with git rows in two repositories on a day. The finalize writes EVERY (repository, person) row
// of the day with the person's membership label, each row with its own git-derived columns, and the person's work items
// on the no-repository row only (acp form B'). The reference merge kept one git row per person, so the other row kept
// the label and the columns of its last write.
func TestFinalizeWritesEveryRepositoryRowOfAPerson14105(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	t0 := day.Add(-72 * time.Hour)
	stored := day.Add(30 * time.Hour)
	repoA := uuid.MustParse("00000000-0000-4000-8000-00000014105a")
	repoB := uuid.MustParse("00000000-0000-4000-8000-00000014105b")

	type live struct {
		team                              string
		locTouched, prsOpened, delivery   uint32
		itemsCompleted                    uint32
		locAdded, locDeleted, prsAuthored uint32
	}
	// seed writes one org: two repositories, teams, memberships, the person's two stored rows (git values differ per
	// repository), and optionally completed work items. It returns the org id.
	seed := func(t *testing.T, name string, teams map[string]uint8, membership string, storedTeam [2]string, workItems uint32) string {
		t.Helper()
		org := uuid.NewSHA1(uuid.NameSpaceURL, []byte("org-14105-"+name)).String()
		exec := func(what, query string, args ...any) {
			t.Helper()
			if err := conn.Exec(ctx, query, args...); err != nil {
				t.Fatalf("%s: %v", what, err)
			}
		}
		for id, active := range teams {
			exec("team "+id, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'github', ?)`, id, uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+id)), "Team "+id, []string{}, t0, t0, org, active)
		}
		if membership != "" {
			exec("membership", `INSERT INTO team_memberships
    (org_id, provider, team_id, member_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, identity_facets)
    VALUES (?, 'github', ?, 'p@example.com', 'p@example.com', 'native', 1, 100, 10, ?, ?, ?, ?)`,
				org, membership, t0, (*time.Time)(nil), t0, []string{"p@example.com"})
		}
		for i, r := range []struct {
			id   uuid.UUID
			name string
		}{{repoA, "acme/a"}, {repoB, "acme/b"}} {
			exec("repo", `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`, r.id, r.name, org, t0)
			// repo A: 40+10 loc, 1 PR; repo B: 7+3 loc, 2 PRs. The IC columns are as the repository/user family leaves them (0).
			added, deleted, prs := uint32(40), uint32(10), uint32(1)
			if i == 1 {
				added, deleted, prs = 7, 3, 2
			}
			exec("stored row", `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, computed_at, org_id)
    VALUES (?, ?, 'p@example.com', 'p@example.com', ?, ?, 3, ?, ?, ?, ?, ?, ?)`,
				r.id, day, storedTeam[i], "Team "+storedTeam[i], added, deleted, prs, prs, stored, org)
		}
		if workItems > 0 {
			exec("work items", `INSERT INTO work_item_user_metrics_daily
    (day, provider, work_scope_id, user_identity, team_id, team_name, items_completed, computed_at, org_id)
    VALUES (?, 'jira', 'ENG', 'p@example.com', 'unassigned', 'Unassigned', ?, ?, ?)`, day, workItems, stored, org)
		}
		if _, err := NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}); err != nil {
			t.Fatal(err)
		}
		return org
	}
	read := func(t *testing.T, org string) map[uuid.UUID]live {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT repo_id, toString(team_id), loc_touched, prs_opened, delivery_units,
    work_items_completed, loc_added, loc_deleted, prs_authored FROM (
    SELECT * FROM user_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ? AND day = ? AND author_email = 'p@example.com' AND retracted = 0`, org, day)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[uuid.UUID]live{}
		for rows.Next() {
			var repo uuid.UUID
			var l live
			if err := rows.Scan(&repo, &l.team, &l.locTouched, &l.prsOpened, &l.delivery, &l.itemsCompleted, &l.locAdded, &l.locDeleted, &l.prsAuthored); err != nil {
				t.Fatal(err)
			}
			got[repo] = l
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return got
	}
	labels := func(got map[uuid.UUID]live) string {
		return fmt.Sprintf("repo A %q, repo B %q", got[repoA].team, got[repoB].team)
	}

	t.Run("an inactive team label is replaced on every repository row", func(t *testing.T) {
		org := seed(t, "inactive", map[string]uint8{"github:member-team": 0}, "github:member-team",
			[2]string{"github:member-team", "github:member-team"}, 0)
		got := read(t, org)
		for _, repo := range []uuid.UUID{repoA, repoB} {
			if got[repo].team != "unassigned" {
				t.Errorf("after the finalize: %s, want both unassigned (the only membership is inactive)", labels(got))
				break
			}
		}
	})
	t.Run("a legacy repository-owner label is replaced on every repository row", func(t *testing.T) {
		org := seed(t, "owner", map[string]uint8{"github:repo-owner": 1}, "",
			[2]string{"github:repo-owner", "github:repo-owner"}, 0)
		got := read(t, org)
		for _, repo := range []uuid.UUID{repoA, repoB} {
			if got[repo].team != "unassigned" {
				t.Errorf("after the finalize: %s, want both unassigned (no membership; an owner is never the label)", labels(got))
				break
			}
		}
	})
	t.Run("an active membership labels every repository row", func(t *testing.T) {
		org := seed(t, "member", map[string]uint8{"github:member-team": 1}, "github:member-team",
			[2]string{"unassigned", "unassigned"}, 0)
		got := read(t, org)
		if got[repoA].team != "github:member-team" || got[repoB].team != "github:member-team" {
			t.Errorf("after the finalize: %s, want both github:member-team", labels(got))
		}
	})
	t.Run("each repository row keeps its own git values and its own IC columns", func(t *testing.T) {
		org := seed(t, "values", map[string]uint8{"github:member-team": 1}, "github:member-team",
			[2]string{"unassigned", "unassigned"}, 0)
		got := read(t, org)
		if a := got[repoA]; a.locAdded != 40 || a.locDeleted != 10 || a.prsAuthored != 1 || a.locTouched != 50 || a.prsOpened != 1 {
			t.Errorf("repo A = %+v, want its own git values and loc_touched 50, prs_opened 1", a)
		}
		if b := got[repoB]; b.locAdded != 7 || b.locDeleted != 3 || b.prsAuthored != 2 || b.locTouched != 10 || b.prsOpened != 2 {
			t.Errorf("repo B = %+v, want its own git values and loc_touched 10, prs_opened 2", b)
		}
	})
	t.Run("work items sit on the no-repository row only, and the person's totals stay equal", func(t *testing.T) {
		org := seed(t, "workitems", map[string]uint8{"github:member-team": 1}, "github:member-team",
			[2]string{"unassigned", "unassigned"}, 4)
		got := read(t, org)
		synthesized := icfinalize.SynthesizedRepoID(org, "p@example.com")
		var items, delivery uint32
		for repo, l := range got {
			items += l.itemsCompleted
			delivery += l.delivery
			if repo != synthesized && l.itemsCompleted != 0 {
				t.Errorf("git row %s holds %d work items, want 0 (work items belong to no repository)", repo, l.itemsCompleted)
			}
		}
		if s, ok := got[synthesized]; !ok || s.itemsCompleted != 4 || s.delivery != 4 || s.team != "github:member-team" {
			t.Errorf("no-repository row = %+v (present %v), want 4 items, delivery 4, the membership label", got[synthesized], ok)
		}
		// the control: the person's completed items once, delivery = the git rows' merged PRs (1 + 2) + the items (4)
		if len(got) != 3 || items != 4 || delivery != 7 {
			t.Errorf("rows %d, work_items_completed %d, delivery_units %d, want 3 rows, 4, 7", len(got), items, delivery)
		}
	})
}
