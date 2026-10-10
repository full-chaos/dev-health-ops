//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// CHAOS-9084 class: the team of team_metrics_daily is the owner of the
// repository (team_repo_ownership), never a member of a team. The case is the
// shape of a production organization: teams that carry NO repo_patterns, some
// with members who match no commit author, and open ownership rows.
//
// Before the fix every row of the day was "unassigned": the repo-pattern
// resolver found nothing, ownership was never read, and the member fallback
// found no author. After it, the owned repository's commits sit under the
// owner's provider-keyed id, an unowned repository is "unassigned", and a
// member of a team never moves a commit of an unowned repository to that team.

const wellbeingOwnershipOrg = "00000000-0000-4000-8000-00000000f0d3"

var wellbeingOwnershipDay = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

func wellbeingOwnershipExec(t *testing.T, ctx context.Context, conn driver.Conn, what, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func wellbeingOwnershipTeam(t *testing.T, ctx context.Context, conn driver.Conn, provider, id string, members []string, at time.Time) {
	t.Helper()
	wellbeingOwnershipExec(t, ctx, conn, "insert team "+id, `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)`,
		id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)), "Team "+id, members, []string{}, at, at, wellbeingOwnershipOrg, provider)
}

func wellbeingOwnershipRepo(t *testing.T, ctx context.Context, conn driver.Conn, provider, name string, repo uuid.UUID, authorEmail string) {
	t.Helper()
	seeded := wellbeingOwnershipDay.Add(26 * time.Hour)
	wellbeingOwnershipExec(t, ctx, conn, "insert repo "+name,
		"INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)", repo, name, wellbeingOwnershipOrg, provider, seeded)
	for hash, when := range map[string]time.Time{"c1": wellbeingOwnershipDay.Add(12 * time.Hour), "c2": wellbeingOwnershipDay.Add(13 * time.Hour)} {
		wellbeingOwnershipExec(t, ctx, conn, "insert commit "+hash, `INSERT INTO git_commits
    (repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, repo, hash, "Dev", authorEmail, when, when, uint32(1), seeded, wellbeingOwnershipOrg)
	}
}

func wellbeingOwnershipOwns(t *testing.T, ctx context.Context, conn driver.Conn, provider, teamID string, repo uuid.UUID, name string, at time.Time) {
	t.Helper()
	wellbeingOwnershipExec(t, ctx, conn, "insert ownership of "+name, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, ?, ?, ?, ?, 'exact', 'native', 1, ?, ?, ?)`,
		wellbeingOwnershipOrg, provider, teamID, repo, name, uint16(10), at, at)
}

func wellbeingOwnershipCompute(t *testing.T, ctx context.Context, conn driver.Conn, repos []uuid.UUID, clock time.Time) {
	t.Helper()
	run := Run{ID: uuid.NewString(), OrganizationID: wellbeingOwnershipOrg, TargetDay: wellbeingOwnershipDay}
	ids := make([]RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, RepositoryID(repo.String()))
	}
	executor, err := NewTeamWellbeingExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	ticks := 0
	executor.nowUTC = func() time.Time {
		ticks++
		return clock.Add(time.Duration(ticks) * time.Second)
	}
	if _, err := executor.ComputeFamily(ctx, run, Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: ids}); err != nil {
		t.Fatalf("team_wellbeing at %s: %v", clock.Format(time.RFC3339), err)
	}
}

// wellbeingOwnershipRead is the newest generation of every (team, repository)
// key of the day: "team|repo" -> commits_count.
func wellbeingOwnershipRead(t *testing.T, ctx context.Context, conn driver.Conn) map[string]uint32 {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT team_id, repo_id, argMax(commits_count, computed_at)
FROM team_metrics_daily WHERE org_id = ? AND day = ? GROUP BY team_id, repo_id`, wellbeingOwnershipOrg, wellbeingOwnershipDay)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]uint32{}
	for rows.Next() {
		var team, repo string
		var commits uint32
		if err := rows.Scan(&team, &repo, &commits); err != nil {
			t.Fatal(err)
		}
		out[team+"|"+repo] = commits
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWellbeingTeamIsTheOwnerOfTheRepositoryNeverAMemberOfATeam(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	t0 := wellbeingOwnershipDay.Add(-72 * time.Hour)

	// One owned repository per provider, plus one repository nobody owns whose
	// author is a MEMBER of a team (the member fallback would have taken it).
	type owned struct {
		provider, team string
		repo           uuid.UUID
		name           string
	}
	var ownedRepos []owned
	var repos []uuid.UUID
	for index, provider := range []string{"github", "gitlab", "jira", "linear"} {
		repo := uuid.MustParse(fmt.Sprintf("00000000-0000-4000-8000-0000000000a%d", index+1))
		item := owned{provider: provider, team: provider + ":platform", repo: repo, name: "acme/" + provider + "-api"}
		ownedRepos = append(ownedRepos, item)
		repos = append(repos, repo)
		wellbeingOwnershipTeam(t, ctx, conn, provider, item.team, []string{}, t0)
		wellbeingOwnershipRepo(t, ctx, conn, provider, item.name, repo, "owner-dev@example.com")
		wellbeingOwnershipOwns(t, ctx, conn, provider, item.team, repo, item.name, t0)
	}
	unowned := uuid.MustParse("00000000-0000-4000-8000-0000000000b9")
	repos = append(repos, unowned)
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/orphan", unowned, "member-dev@example.com")
	// Thirteen-teams shape: teams with no members, and teams whose members
	// match no author, and one whose member IS the author of the unowned repo.
	wellbeingOwnershipTeam(t, ctx, conn, "github", "github:docs", []string{"somebody-else@example.com"}, t0)
	wellbeingOwnershipTeam(t, ctx, conn, "github", "github:people", []string{"member-dev@example.com"}, t0)

	wellbeingOwnershipCompute(t, ctx, conn, repos, wellbeingOwnershipDay.Add(30*time.Hour))
	got := wellbeingOwnershipRead(t, ctx, conn)

	want := map[string]uint32{"unassigned|" + unowned.String(): 2}
	for _, item := range ownedRepos {
		want[item.team+"|"+item.repo.String()] = 2
	}
	if len(got) != len(want) {
		t.Errorf("rows of the day = %v, want %v", got, want)
	}
	for key, commits := range want {
		if got[key] != commits {
			t.Errorf("row %s = %d commits, want %d (all rows: %v)", key, got[key], commits, got)
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected row %s: a team that does not own the repository holds its commits (all rows: %v)", key, got)
		}
	}
}

// A day first computed with no owner of the repository ("unassigned" rows, the
// 906 stored rows of production) and recomputed once ownership exists: the
// stale-key rule must leave the old unassigned key as a row of zeros in the
// newest generation, and the owner must hold the commits.
func TestWellbeingRecomputeOnceOwnedSupersedesTheUnassignedRows(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	t0 := wellbeingOwnershipDay.Add(-72 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000c1")
	wellbeingOwnershipTeam(t, ctx, conn, "github", "github:platform", []string{}, t0)
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/api", repo, "dev@example.com")

	wellbeingOwnershipCompute(t, ctx, conn, []uuid.UUID{repo}, wellbeingOwnershipDay.Add(30*time.Hour))
	if got := wellbeingOwnershipRead(t, ctx, conn); got["unassigned|"+repo.String()] != 2 || len(got) != 1 {
		t.Fatalf("the first compute is not the case (no owner yet): %v", got)
	}

	// The ownership is valid since before the day (it was synced late).
	wellbeingOwnershipOwns(t, ctx, conn, "github", "github:platform", repo, "acme/api", t0)
	wellbeingOwnershipCompute(t, ctx, conn, []uuid.UUID{repo}, wellbeingOwnershipDay.Add(40*time.Hour))
	got := wellbeingOwnershipRead(t, ctx, conn)
	if got["github:platform|"+repo.String()] != 2 {
		t.Errorf("the owner does not hold the commits after the recompute: %v", got)
	}
	if got["unassigned|"+repo.String()] != 0 {
		t.Errorf("the unassigned key still holds %d commits of a repository that is owned now: %v", got["unassigned|"+repo.String()], got)
	}
}
