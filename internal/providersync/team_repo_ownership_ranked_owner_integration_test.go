//go:build integration

package providersync

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// The owner rule against the real migration chain (rulings D5432, D5988):
// every team that reaches a repo owns it with its share (its links over all
// links of the repo); the main owner (is_primary = 1) is the team with the
// most native links, then the most explicit_text links, then the most
// heuristic links (work_graph_issue_pr.provenance); every other owner is a
// co-owner (is_primary = 2). A full tie keeps the existing main owner.
// Every contest runs for each provider pair, so each of jira, github, gitlab
// and linear is the winning and the losing team's provider at least once.

// rankedOwnerOrg is one org's fixture: one repo, and per team one project of
// the team's provider that the team owns.
type rankedOwnerOrg struct {
	t      *testing.T
	ctx    context.Context
	conn   driver.Conn
	orgID  string
	repoID uuid.UUID
	repo   string
	at     time.Time
	issue  int
	pr     uint32
}

func newRankedOwnerOrg(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) *rankedOwnerOrg {
	t.Helper()
	org := &rankedOwnerOrg{t: t, ctx: ctx, conn: conn, orgID: orgID, repoID: uuid.New(), repo: "acme/" + orgID,
		at: time.Date(2026, 10, 7, 19, 0, 0, 0, time.UTC)}
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{org.repoID: org.repo})
	return org
}

// ownProject seeds the team's ownership of its provider's project.
func (org *rankedOwnerOrg) ownProject(provider, teamID string) {
	seedTeamProjectOwnership(org.t, org.ctx, org.conn, org.orgID, provider, provider+"-proj-"+teamID, teamID, true, org.at)
}

// link seeds count issues of the team's project, each linked to its own PR
// in the repo by a work_graph_issue_pr row of the given provenance.
func (org *rankedOwnerOrg) link(provider, teamID, provenance string, count int) {
	for range count {
		org.issue++
		org.pr++
		workItemID := fmt.Sprintf("%s:%s-%d", provider, teamID, org.issue)
		seedWorkItem(org.t, org.ctx, org.conn, org.orgID, workItemID, provider, uuid.Nil, provider+"-proj-"+teamID, org.at)
		seedWorkGraphIssuePRWithProvenance(org.t, org.ctx, org.conn, org.orgID, org.repoID, workItemID, org.pr, provenance, org.at)
	}
}

func (org *rankedOwnerOrg) derive() (written, retracted int, stats TeamRepoOwnershipDerivationStats) {
	org.t.Helper()
	written, retracted, inputsReady, _, stats, err := TeamRepoOwnershipDerivationService{Conn: org.conn}.DeriveWithStats(org.ctx, org.orgID)
	if err != nil || !inputsReady {
		org.t.Fatalf("Derive: inputsReady=%v err=%v", inputsReady, err)
	}
	return written, retracted, stats
}

// openOwners reads the repo's open inferred owner rows back from ClickHouse.
func (org *rankedOwnerOrg) openOwners() []string {
	org.t.Helper()
	rows, err := org.conn.Query(org.ctx, `
SELECT team_id
FROM team_repo_ownership FINAL
WHERE org_id = ? AND repo_full_name = ? AND source = 'inferred' AND valid_to IS NULL`, org.orgID, org.repo)
	if err != nil {
		org.t.Fatalf("read open owners: %v", err)
	}
	defer rows.Close()
	var owners []string
	for rows.Next() {
		var teamID string
		if err := rows.Scan(&teamID); err != nil {
			org.t.Fatalf("scan open owner: %v", err)
		}
		owners = append(owners, teamID)
	}
	if err := rows.Err(); err != nil {
		org.t.Fatalf("open owners rows.Err: %v", err)
	}
	sort.Strings(owners)
	return owners
}

// ownerRow is one open inferred row's role and share, as "primary:share".
type ownerRow string

func ownerState(primary uint8, share float64) ownerRow {
	return ownerRow(fmt.Sprintf("%d:%.6f", primary, share))
}

// openOwnerRows reads every open inferred row of the repo as team -> role and
// share; a team with two open rows fails (one open row per claim).
func (org *rankedOwnerOrg) openOwnerRows() map[string]ownerRow {
	org.t.Helper()
	rows, err := org.conn.Query(org.ctx, `
SELECT team_id, is_primary, ifNull(share, -1)
FROM team_repo_ownership FINAL
WHERE org_id = ? AND repo_full_name = ? AND source = 'inferred' AND valid_to IS NULL`, org.orgID, org.repo)
	if err != nil {
		org.t.Fatalf("read open owner rows: %v", err)
	}
	defer rows.Close()
	out := map[string]ownerRow{}
	for rows.Next() {
		var teamID string
		var primary uint8
		var share float64
		if err := rows.Scan(&teamID, &primary, &share); err != nil {
			org.t.Fatalf("scan open owner row: %v", err)
		}
		if _, twice := out[teamID]; twice {
			org.t.Fatalf("team %s has two open rows on %s", teamID, org.repo)
		}
		out[teamID] = ownerState(primary, share)
	}
	if err := rows.Err(); err != nil {
		org.t.Fatalf("open owner rows.Err: %v", err)
	}
	return out
}

func (org *rankedOwnerOrg) assertOwnerRows(want map[string]ownerRow) {
	org.t.Helper()
	if got := org.openOwnerRows(); !reflect.DeepEqual(got, want) {
		org.t.Fatalf("open inferred rows of %s = %v, want %v", org.repo, got, want)
	}
}

func (org *rankedOwnerOrg) assertOpenOwners(want ...string) {
	org.t.Helper()
	if got := org.openOwners(); !reflect.DeepEqual(got, want) {
		org.t.Fatalf("open inferred owners of %s = %v, want %v", org.repo, got, want)
	}
}

var rankedOwnerIntegrationProviderPairs = [][2]string{
	{"linear", "jira"}, {"jira", "github"}, {"github", "gitlab"}, {"gitlab", "linear"},
}

// TestRankedOwnerKeepsTheNativeMajorityTeamWhenFewExplicitTextLinksArrive is
// the shape that lost a repo's owner: a team owns the repo through many
// native links, then four teams of another provider each get two
// explicit_text links to it. The owner stays the main owner, nothing is
// retracted, its one open row takes its new share in place, and each rival is
// a co-owner with its share.
func TestRankedOwnerKeepsTheNativeMajorityTeamWhenFewExplicitTextLinksArrive(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_owner_"+pair[1]+"_rivals", func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-majority-%d", index))
			org.ownProject(pair[0], "owner")
			org.link(pair[0], "owner", "native", 12)
			org.link(pair[0], "owner", "explicit_text", 2)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			org.assertOwnerRows(map[string]ownerRow{"owner": ownerState(1, 1)})

			for _, rival := range []string{"rival-a", "rival-b", "rival-c", "rival-d"} {
				org.ownProject(pair[1], rival)
				org.link(pair[1], rival, "explicit_text", 2)
			}
			written, retracted, stats := org.derive()
			if written != 5 || retracted != 0 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 5 (the owner's new share, four co-owners) and 0", written, retracted)
			}
			if stats.Derived != 5 || stats.Unchanged != 0 {
				t.Fatalf("second Derive stats: %+v, want derived=5 unchanged=0", stats)
			}
			rival := ownerState(2, 2.0/22)
			org.assertOwnerRows(map[string]ownerRow{"owner": ownerState(1, 14.0/22),
				"rival-a": rival, "rival-b": rival, "rival-c": rival, "rival-d": rival})
			if written, retracted, _ := org.derive(); written != 0 || retracted != 0 {
				t.Fatalf("third Derive: written=%d retracted=%d, want 0 and 0", written, retracted)
			}
		})
	}
}

// TestRankedOwnerOneNativeLinkTakesTheRepoFromFiftyExplicitTextLinks: a
// count in a lower tier never outweighs a higher tier for the main owner. The
// main owner moves to the native team; the old owner stays as a co-owner with
// the larger share (the rank and the share disagree, and each row says both).
func TestRankedOwnerOneNativeLinkTakesTheRepoFromFiftyExplicitTextLinks(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_native_over_"+pair[1]+"_text", func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-tier-%d", index))
			org.ownProject(pair[1], "text-team")
			org.link(pair[1], "text-team", "explicit_text", 50)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			org.assertOpenOwners("text-team")

			org.ownProject(pair[0], "native-team")
			org.link(pair[0], "native-team", "native", 1)
			if written, retracted, _ := org.derive(); written != 2 || retracted != 0 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 2 (native-team, text-team as a co-owner) and 0", written, retracted)
			}
			org.assertOwnerRows(map[string]ownerRow{"native-team": ownerState(1, 1.0/51), "text-team": ownerState(2, 50.0/51)})
		})
	}
}

// TestRankedOwnerFullTieKeepsTheExistingOwnerAndSignalsTheTie: a second team
// reaches the repo with the owner's exact link counts at every tier. The
// existing main owner stays the main owner, the second team is a co-owner,
// both at half the links, nothing is retracted, and the run reports the tie.
func TestRankedOwnerFullTieKeepsTheExistingOwnerAndSignalsTheTie(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_owner_ties_"+pair[1], func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-tie-%d", index))
			org.ownProject(pair[0], "team-a")
			org.link(pair[0], "team-a", "native", 1)
			org.link(pair[0], "team-a", "heuristic", 2)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			org.assertOpenOwners("team-a")

			org.ownProject(pair[1], "team-b")
			org.link(pair[1], "team-b", "native", 1)
			org.link(pair[1], "team-b", "heuristic", 2)
			written, retracted, stats := org.derive()
			if written != 2 || retracted != 0 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 2 (team-a's new share, team-b) and 0 (tie keeps the owner)", written, retracted)
			}
			org.assertOwnerRows(map[string]ownerRow{"team-a": ownerState(1, 0.5), "team-b": ownerState(2, 0.5)})
			wantTies := []TeamRepoOwnershipTie{{RepoID: org.repoID.String(), TeamIDs: []string{"team-a", "team-b"}}}
			if !reflect.DeepEqual(stats.Ties, wantTies) {
				t.Fatalf("second Derive ties = %+v, want %+v", stats.Ties, wantTies)
			}
			if written, retracted, _ := org.derive(); written != 0 || retracted != 0 {
				t.Fatalf("rerun in the tie: written=%d retracted=%d, want 0 and 0", written, retracted)
			}

			org.link(pair[1], "team-b", "heuristic", 1)
			if written, retracted, _ := org.derive(); written != 2 || retracted != 0 {
				t.Fatalf("third Derive: written=%d retracted=%d, want 2 (team-b main, team-a co-owner) and 0 once the tie breaks", written, retracted)
			}
			org.assertOwnerRows(map[string]ownerRow{"team-b": ownerState(1, 4.0/7), "team-a": ownerState(2, 3.0/7)})
		})
	}
}

// TestRankedOwnerSingleTeamIsUnchangedAgainstMigratedSchema: a repo only one team reaches is
// owned by that team whatever its tier mix, and a re-run writes nothing.
func TestRankedOwnerSingleTeamIsUnchangedAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, provider := range []string{"jira", "github", "gitlab", "linear"} {
		t.Run(provider, func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, "ranked-single-"+provider)
			org.ownProject(provider, "only")
			org.link(provider, "only", "native", 2)
			org.link(provider, "only", "explicit_text", 3)
			org.link(provider, "only", "heuristic", 1)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			if written, retracted, _ := org.derive(); written != 0 || retracted != 0 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 0 and 0", written, retracted)
			}
			org.assertOwnerRows(map[string]ownerRow{"only": ownerState(1, 1)})
		})
	}
}

// TestRankedOwnerAnIssuesOwnRepoIsNotACandidateAgainstMigratedSchema: the
// loader reads work_items.type. A GitHub or GitLab issue with a repo_id in an
// owned project and no linked PR names no owner; a pull request or merge
// request of the same shape does.
func TestRankedOwnerAnIssuesOwnRepoIsNotACandidateAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, tc := range []struct {
		name, provider, itemType, workItemID string
		wantOwner                            bool
	}{
		{"github_issue", "github", "issue", "gh:acme/repo#1", false},
		{"github_pr", "github", "pr", "ghpr:acme/repo#2", true},
		{"gitlab_issue", "gitlab", "issue", "gitlab:acme/repo#3", false},
		{"gitlab_mr", "gitlab", "merge_request", "gitlab:acme/repo!4", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, "ranked-issue-"+tc.name)
			org.ownProject(tc.provider, "team-a")
			seedWorkItemOfType(t, ctx, conn, org.orgID, tc.workItemID, tc.provider, tc.itemType, org.repoID, tc.provider+"-proj-team-a", org.at)
			written, retracted, _ := org.derive()
			if tc.wantOwner {
				if written != 1 || retracted != 0 {
					t.Fatalf("Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
				}
				org.assertOpenOwners("team-a")
				return
			}
			if written != 0 || retracted != 0 {
				t.Fatalf("Derive: written=%d retracted=%d, want 0 and 0 (an issue's own repo_id is not a relation)", written, retracted)
			}
			org.assertOpenOwners()
		})
	}
}

// TestRankedOwnerFullTieRetractsTheRowOfATeamOutsideTheTie: the repo's main
// owner loses its rank to two other teams that tie with each other. No main
// owner is named; the old owner -- not in the tie -- stays only as a co-owner
// with its share, and the tied teams are co-owners.
func TestRankedOwnerFullTieRetractsTheRowOfATeamOutsideTheTie(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_old_owner_"+pair[1]+"_tie", func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-third-%d", index))
			org.ownProject(pair[0], "old-owner")
			org.link(pair[0], "old-owner", "native", 1)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			org.assertOpenOwners("old-owner")

			org.ownProject(pair[1], "team-a")
			org.link(pair[1], "team-a", "native", 2)
			org.ownProject(pair[0], "team-b")
			org.link(pair[0], "team-b", "native", 2)
			written, retracted, stats := org.derive()
			if written != 3 || retracted != 0 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 3 (old-owner as a co-owner, team-a, team-b) and 0", written, retracted)
			}
			org.assertOwnerRows(map[string]ownerRow{"old-owner": ownerState(2, 0.2), "team-a": ownerState(2, 0.4), "team-b": ownerState(2, 0.4)})
			wantTies := []TeamRepoOwnershipTie{{RepoID: org.repoID.String(), TeamIDs: []string{"team-a", "team-b"}}}
			if !reflect.DeepEqual(stats.Ties, wantTies) {
				t.Fatalf("second Derive ties = %+v, want %+v", stats.Ties, wantTies)
			}
		})
	}
}

// TestRankedOwnerTieWithNoOwnerStaysOwnerlessAndIsReportedEveryRun: a tie
// on a repo no tied team owns yet names no main owner: both teams are
// co-owners with their shares, a rerun writes nothing, and every run reports
// the tie again while it lasts.
func TestRankedOwnerTieWithNoOwnerStaysOwnerlessAndIsReportedEveryRun(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_ties_"+pair[1], func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-ownerless-%d", index))
			org.ownProject(pair[0], "team-a")
			org.link(pair[0], "team-a", "explicit_text", 1)
			org.ownProject(pair[1], "team-b")
			org.link(pair[1], "team-b", "explicit_text", 1)
			wantTies := []TeamRepoOwnershipTie{{RepoID: org.repoID.String(), TeamIDs: []string{"team-a", "team-b"}}}
			for run := 1; run <= 2; run++ {
				wantWritten := 2
				if run == 2 {
					wantWritten = 0
				}
				written, retracted, stats := org.derive()
				if written != wantWritten || retracted != 0 || !reflect.DeepEqual(stats.Ties, wantTies) {
					t.Fatalf("run %d: written=%d retracted=%d ties=%+v, want %d, 0 and %+v", run, written, retracted, stats.Ties, wantWritten, wantTies)
				}
				org.assertOwnerRows(map[string]ownerRow{"team-a": ownerState(2, 0.5), "team-b": ownerState(2, 0.5)})
			}
		})
	}
}

// writeLinks seeds count issues of the team's project and writes one
// work_graph_issue_pr row per issue through the production issue<->PR link
// writer, each at its own PR of the repo. It returns the written links so a
// caller can write the same keys again.
func (org *rankedOwnerOrg) writeLinks(provider, teamID, provenance string, count int, at time.Time) []issueprlinks.Link {
	org.t.Helper()
	writer, err := issueprlinks.NewWriter(org.conn)
	if err != nil {
		org.t.Fatalf("issue-pr link writer: %v", err)
	}
	links := make([]issueprlinks.Link, 0, count)
	for range count {
		org.issue++
		org.pr++
		workItemID := fmt.Sprintf("%s:%s-%d", provider, teamID, org.issue)
		seedWorkItem(org.t, org.ctx, org.conn, org.orgID, workItemID, provider, uuid.Nil, provider+"-proj-"+teamID, org.at)
		links = append(links, issueprlinks.Link{OrgID: org.orgID, RepoID: org.repoID, WorkItemID: workItemID, PRNumber: org.pr,
			Confidence: 1, Provenance: provenance, Evidence: "ranked-owner-test", LastSynced: at})
	}
	if err := writer.Write(org.ctx, links); err != nil {
		org.t.Fatalf("write issue-pr links: %v", err)
	}
	return links
}

// TestRankedOwnerReadsLinksWrittenByTheIssuePRLinkWriter seeds
// work_graph_issue_pr through the production writer, so the derivation reads
// the provenance that survives the table's version precedence: a native row
// outranks a newer explicit_text row of the same (repo, issue, PR) key.
func TestRankedOwnerReadsLinksWrittenByTheIssuePRLinkWriter(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_vs_"+pair[1], func(t *testing.T) {
			older, newer := time.Date(2026, 10, 7, 18, 0, 0, 0, time.UTC), time.Date(2026, 10, 7, 19, 0, 0, 0, time.UTC)

			majority := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-writer-majority-%d", index))
			majority.ownProject(pair[0], "owner")
			majority.writeLinks(pair[0], "owner", issueprlinks.ProvenanceNative, 6, older)
			majority.ownProject(pair[1], "rival")
			majority.writeLinks(pair[1], "rival", "explicit_text", 2, newer)
			if written, retracted, _ := majority.derive(); written != 2 || retracted != 0 {
				t.Fatalf("majority Derive: written=%d retracted=%d, want 2 and 0", written, retracted)
			}
			majority.assertOwnerRows(map[string]ownerRow{"owner": ownerState(1, 0.75), "rival": ownerState(2, 0.25)})

			precedence := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-writer-precedence-%d", index))
			precedence.ownProject(pair[0], "three-native")
			precedence.writeLinks(pair[0], "three-native", issueprlinks.ProvenanceNative, 3, older)
			precedence.ownProject(pair[1], "four-relinked")
			relinked := precedence.writeLinks(pair[1], "four-relinked", issueprlinks.ProvenanceNative, 4, older)
			for i := range relinked {
				relinked[i].Provenance, relinked[i].LastSynced = "explicit_text", newer
			}
			writer, err := issueprlinks.NewWriter(conn)
			if err != nil {
				t.Fatalf("issue-pr link writer: %v", err)
			}
			if err := writer.Write(ctx, relinked); err != nil {
				t.Fatalf("rewrite issue-pr links: %v", err)
			}
			if written, retracted, _ := precedence.derive(); written != 2 || retracted != 0 {
				t.Fatalf("precedence Derive: written=%d retracted=%d, want 2 and 0", written, retracted)
			}
			precedence.assertOwnerRows(map[string]ownerRow{"four-relinked": ownerState(1, 4.0/7), "three-native": ownerState(2, 3.0/7)})
		})
	}
}

// seedSameNamedGitLabRepo adds a GitLab repo with the org repo's full name and
// an open inferred row of teamID on it that no link supports.
func (org *rankedOwnerOrg) seedSameNamedGitLabRepo(teamID string) uuid.UUID {
	org.t.Helper()
	gitlabRepo := uuid.New()
	batch, err := org.conn.PrepareBatch(org.ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced)`)
	if err != nil {
		org.t.Fatalf("prepare repos batch: %v", err)
	}
	if err := batch.Append(gitlabRepo, org.repo, org.orgID, "gitlab", time.Now().UTC()); err != nil {
		org.t.Fatalf("append gitlab repo: %v", err)
	}
	if err := batch.Send(); err != nil {
		org.t.Fatalf("send gitlab repo: %v", err)
	}
	if err := org.conn.Exec(org.ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'gitlab', ?, ?, ?, 'exact', 'inferred', 0, 100, 10, ?, NULL, ?)`,
		org.orgID, teamID, gitlabRepo, org.repo, org.at, org.at); err != nil {
		org.t.Fatalf("seed the unsupported gitlab row: %v", err)
	}
	return gitlabRepo
}

func (org *rankedOwnerOrg) openGitLabRows(teamID string) uint64 {
	org.t.Helper()
	return countRows(org.t, org.ctx, org.conn, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND provider = 'gitlab' AND team_id = ? AND source = 'inferred' AND valid_to IS NULL`, org.orgID, teamID)
}

// TestRankedOwnerClaimsOfASameNamedRepoOfAnotherProviderAreNotProtected: two
// repos of one organization share a full name on different providers. Neither
// a tie on the GitHub repo nor a derived row on it may keep an unsupported
// open row of the same team on the GitLab repo open.
func TestRankedOwnerClaimsOfASameNamedRepoOfAnotherProviderAreNotProtected(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)

	t.Run("a tie on the github repo", func(t *testing.T) {
		org := newRankedOwnerOrg(t, ctx, conn, "ranked-samename-tie")
		org.ownProject("github", "team-a")
		org.link("github", "team-a", "explicit_text", 1)
		org.ownProject("jira", "team-b")
		org.link("jira", "team-b", "explicit_text", 1)
		org.seedSameNamedGitLabRepo("team-a")
		written, retracted, stats := org.derive()
		if written != 2 || retracted != 1 || len(stats.Ties) != 1 {
			t.Fatalf("Derive: written=%d retracted=%d ties=%+v, want 2 (two tied co-owners), 1 and one tie", written, retracted, stats.Ties)
		}
		if open := org.openGitLabRows("team-a"); open != 0 {
			t.Fatalf("open gitlab rows of team-a = %d, want 0", open)
		}
	})

	t.Run("a derived row on the github repo", func(t *testing.T) {
		org := newRankedOwnerOrg(t, ctx, conn, "ranked-samename-desired")
		org.ownProject("github", "team-a")
		org.link("github", "team-a", "native", 1)
		org.seedSameNamedGitLabRepo("team-a")
		written, retracted, _ := org.derive()
		if written != 1 || retracted != 1 {
			t.Fatalf("Derive: written=%d retracted=%d, want 1 and 1", written, retracted)
		}
		org.assertOpenOwners("team-a")
		if open := org.openGitLabRows("team-a"); open != 0 {
			t.Fatalf("open gitlab rows of team-a = %d, want 0", open)
		}
	})
}
